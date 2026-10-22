//! The worker↔leader heartbeat (design doc, Control plane): the request a worker POSTs on its interval,
//! the directive it gets back, and the blocking client that sends it. Every exchange is worker-initiated.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::metrics::{MetricSample, MetricValue, Metrics};

/// How many logs one heartbeat carries. A worker out of contact accumulates more than this, so the queue
/// drains over several beats rather than turning one request into an unbounded body.
///
/// With the default beat this is what the leader drains at, and a worker saying more than that per second
/// never catches up: its queue fills and what it says after is refused. The bound on raising it is the body
/// (see [`HEARTBEAT_BODY_LIMIT`]), not the leader's appetite.
pub const LOGS_PER_HEARTBEAT: usize = 512;

/// How long one exchange with the leader may take, end to end: name resolution, connection, request, and reading
/// the response body. `ureq` bounds none of these on its own, so without it a leader that accepts a connection and
/// then stops answering holds the caller for as long as the socket stays open.
///
/// Generous rather than tight, because it bounds a stall and not ordinary work: a beat carrying a full log batch
/// over a slow link is healthy, and a budget under that would refuse workers for being far away. It is the same
/// half-minute a Kafka client gets to set itself up and an Elasticsearch destination gets before it counts as
/// stalled.
pub const REQUEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// What a finished capture gets to upload in. A capture is invited up to [`crate::leader::capture::MAX_BYTES_CEILING`]
/// of raw records, which is three orders of magnitude past a heartbeat; on a link where that takes longer than
/// [`REQUEST_BUDGET`], every retry would restart it and the records would expire undelivered. It is the same five
/// minutes a capture itself may run for.
pub const CAPTURE_BUDGET: std::time::Duration = std::time::Duration::from_secs(300);

/// How long one lookup file may take to arrive. A file is up to 2 GB, so this is what that size needs at a little over
/// a megabyte a second; a deploy that needs a file waits on it, and one that does not fetches nothing.
pub const LOOKUP_FILE_BUDGET: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// What one string may contribute to a heartbeat: a log's message and fields, a destination's refusal, a source's
/// block reason. The console keeps the whole of a message; the wire does not, because what a destination or a TLS
/// stack put in a field is not ours to bound, and this rides every heartbeat.
///
/// Long enough for the worst error worth reading. An Elasticsearch mapping error that names the field and the
/// document runs to several hundred characters, and a block reason carries its fix ahead of one.
pub const MAX_TEXT_LEN: usize = 1000;

/// Fields one log carries onward. A call site with more than this to say has stopped describing an event.
pub const MAX_LOG_FIELDS: usize = 16;

/// What the leader will read from one heartbeat. Stated rather than inherited from the web framework, because
/// the bounds above are chosen against it: a full batch of the worst event there can be measures 9.1 MB, and
/// nothing bounds how many metric series a worker's own thread and reader count can add beside it the way
/// [`LOGS_PER_HEARTBEAT`] bounds logs.
///
/// Sized to make reaching it implausible rather than to fit an exact worst case: an operator running 10,000
/// reader threads at 10 metrics apiece is still an order of magnitude under this. A request over the limit is
/// refused with a status code, which [`HeartbeatClient::send`] does not retry -- so a worker that could build
/// one would rebuild it every beat and never heartbeat again. [`HEARTBEAT_BODY_WARNING`] is the advance notice
/// for a worker heading toward it.
pub const HEARTBEAT_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// The share of [`HEARTBEAT_BODY_LIMIT`] a heartbeat may reach before the worker that built it warns about it.
/// Metrics have no cap of their own to fall back on, so this is what stands in for one: an operator sees which
/// worker and how large well before a beat is ever refused.
pub const HEARTBEAT_BODY_WARNING: f64 = 0.5;

/// One log on the wire. The worker it came from is on the request, so it is not repeated per event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogDto {
    /// This event's number in the reporting process's life. With the request's `boot_id` it is what lets the
    /// leader take the same batch twice without writing it twice -- which it must, because `send` retries.
    pub seq: u64,
    pub timestamp_ms: i64,
    pub level: String,
    pub site: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    /// Whose life `seq` counts in, when that is not the process sending the request. A worker's runtime is a
    /// process of its own with a sequence of its own, and the leader's `(worker, boot, seq)` key would collide
    /// two of them under one number. Absent from a worker whose logs all come from the process that heartbeats.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<(String, FieldValueDto)>,
}

/// A detail field's value, typed so that whatever reads it does not have to parse the sentence.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FieldValueDto {
    Str(String),
    Int(i64),
    Uint(u64),
    Float(f64),
    Bool(bool),
}

impl From<&crate::logs::Event> for LogDto {
    fn from(event: &crate::logs::Event) -> LogDto {
        use crate::runtime::truncate;
        LogDto {
            seq: event.seq,
            timestamp_ms: event
                .timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or_default(),
            level: event.level.as_str().to_lowercase(),
            site: truncate(&event.site, MAX_TEXT_LEN),
            message: truncate(&event.message, MAX_TEXT_LEN),
            thread: event.thread.clone(),
            boot_id: None,
            fields: event
                .fields
                .iter()
                .take(MAX_LOG_FIELDS)
                .map(|(name, value)| (name.to_string(), FieldValueDto::bounded(value)))
                .collect(),
        }
    }
}

impl LogDto {
    /// The event this carries, for a process that reads it rather than delivers it: a runtime handed its
    /// supervisor's logs to mirror. Named for no worker, which the reading process supplies. `None` for a severity
    /// this build does not know.
    pub fn into_event(self) -> Option<crate::logs::Event> {
        Some(crate::logs::Event {
            timestamp: std::time::UNIX_EPOCH + std::time::Duration::from_millis(self.timestamp_ms.max(0) as u64),
            level: crate::logs::level_for_label(&self.level)?,
            site: self.site.into(),
            message: self.message,
            fields: self
                .fields
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
            worker: None,
            thread: self.thread,
            local_only: false,
            seq: self.seq,
        })
    }
}

impl From<FieldValueDto> for crate::logs::Value {
    fn from(value: FieldValueDto) -> crate::logs::Value {
        match value {
            FieldValueDto::Str(s) => crate::logs::Value::Str(s),
            FieldValueDto::Int(n) => crate::logs::Value::Int(n),
            FieldValueDto::Uint(n) => crate::logs::Value::Uint(n),
            FieldValueDto::Float(n) => crate::logs::Value::Float(n),
            FieldValueDto::Bool(b) => crate::logs::Value::Bool(b),
        }
    }
}

impl FieldValueDto {
    /// A value cut to what the wire carries. Only a string can be long; a number is a number.
    fn bounded(value: &crate::logs::Value) -> FieldValueDto {
        match value {
            crate::logs::Value::Str(s) => FieldValueDto::Str(crate::runtime::truncate(s, MAX_TEXT_LEN)),
            other => FieldValueDto::from(other),
        }
    }
}

impl From<&crate::logs::Value> for FieldValueDto {
    fn from(value: &crate::logs::Value) -> FieldValueDto {
        match value {
            crate::logs::Value::Str(s) => FieldValueDto::Str(s.clone()),
            crate::logs::Value::Int(n) => FieldValueDto::Int(*n),
            crate::logs::Value::Uint(n) => FieldValueDto::Uint(*n),
            crate::logs::Value::Float(n) => FieldValueDto::Float(*n),
            crate::logs::Value::Bool(b) => FieldValueDto::Bool(*b),
        }
    }
}

/// One metric sample on the wire: owned strings, no borrow of the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricSampleDto {
    pub metric: String,
    pub labels: Vec<(String, String)>,
    pub value: MetricValueDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MetricValueDto {
    Counter(u64),
    FloatCounter(f64),
    Gauge(f64),
    /// A fact with no natural number -- a reason, a destination's own words -- carried as text. A metric sample never
    /// produces this from a registry value; a caller builds one directly when a thread has a string fact to report
    /// alongside its numbers.
    Text(String),
    /// A kind from a newer build, which this one does not read, kept as it arrived.
    #[serde(untagged)]
    Unknown(serde_json::Value),
}

impl From<MetricSample> for MetricSampleDto {
    fn from(s: MetricSample) -> MetricSampleDto {
        MetricSampleDto {
            metric: s.key.name.to_string(),
            labels: s.key.labels.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            value: match s.value {
                MetricValue::Counter(v) => MetricValueDto::Counter(v),
                MetricValue::FloatCounter(v) => MetricValueDto::FloatCounter(v),
                MetricValue::Gauge(v) => MetricValueDto::Gauge(v),
            },
        }
    }
}

impl MetricSampleDto {
    /// Back into a registry sample, for the worker process that mirrors what its other process recorded. `None` for
    /// text, which no registry holds, and for a kind this build does not read.
    pub fn into_sample(self) -> Option<MetricSample> {
        let value = match self.value {
            MetricValueDto::Counter(v) => MetricValue::Counter(v),
            MetricValueDto::FloatCounter(v) => MetricValue::FloatCounter(v),
            MetricValueDto::Gauge(v) => MetricValue::Gauge(v),
            MetricValueDto::Text(_) | MetricValueDto::Unknown(_) => return None,
        };
        Some(MetricSample {
            key: crate::metrics::MetricKey {
                name: intern(self.metric),
                labels: self.labels.into_iter().map(|(k, v)| (intern(k), v)).collect(),
            },
            value,
        })
    }
}

/// A registry key's names are `&'static str`. The ones arriving from the other process are the same binary's own
/// literals, so the set is bounded and leaking each once is what it costs.
fn intern(name: String) -> &'static str {
    static NAMES: std::sync::Mutex<std::collections::BTreeSet<&'static str>> =
        std::sync::Mutex::new(std::collections::BTreeSet::new());
    let mut names = NAMES.lock().unwrap();
    if let Some(held) = names.get(name.as_str()) {
        return held;
    }
    let leaked: &'static str = Box::leak(name.into_boxed_str());
    names.insert(leaked);
    leaked
}

/// The registry's increase since the heartbeat's last commit. The cursor advances only in [`commit_for_heartbeat`],
/// once the request has landed, so a failed send leaves the window's counts owed.
pub fn peek_for_heartbeat(metrics: &Metrics) -> Vec<MetricSample> {
    metrics.peek_delta(crate::metrics::SUBSCRIBER_HEARTBEAT)
}

/// Peeked samples as per-thread wire DTOs, keyed by the `thread` label. Every sample carries one: `MetricsHandle`
/// pushes it on every write.
pub fn split_by_thread(samples: &[MetricSample]) -> HashMap<String, Vec<MetricSampleDto>> {
    let mut out: HashMap<String, Vec<MetricSampleDto>> = HashMap::new();
    for sample in samples {
        let thread = sample
            .key
            .labels
            .iter()
            .find(|(k, _)| *k == crate::metrics::THREAD_LABEL)
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        out.entry(thread)
            .or_default()
            .push(MetricSampleDto::from(sample.clone()));
    }
    out
}

/// Advance the heartbeat's cursor past `sent`, once the request landed.
pub fn commit_for_heartbeat(metrics: &Metrics, sent: &[MetricSample]) {
    metrics.commit_delta(crate::metrics::SUBSCRIBER_HEARTBEAT, sent);
}

/// A topic-partition a worker thread currently owns, reported so the leader's health view shows ownership without a
/// consumer of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRef {
    pub topic: String,
    pub partition: i32,
}

/// What a reporting thread does. Each stage is sized by something the others cannot influence (design doc, Runtime
/// stages), so a worker's thread count is three numbers rather than one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ThreadKind {
    /// Ingest: one per source, holding the shards the source's protocol gave it.
    Reader,
    /// Transform: `thread_limit` of them, draining every reader's lane.
    Transform,
    /// Egress: one per destination, owning that destination's client.
    Egress,
    /// Not a thread: the process itself. A worker with nothing deployed reports this alone, so it learns a
    /// directive; a deployed one reports its deployment-wide gauges under it.
    Worker,
}

/// What a worker's supervisor is doing, as distinct from what its deployment is doing. A heartbeat proves the
/// supervisor is healthy and says nothing about whether anything is processing, so these are the facts that tell
/// a worker running its target from one that cannot reach it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Supervising {
    /// The newest commit discovery has seen. Ahead of the running commit while a deployment is in flight, and
    /// still ahead of it when one will not start -- which is what tells a worker that refused a commit from one
    /// that is simply up to date.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_commit: Option<String>,
    /// What the deployer is doing, and when it started. A phase that does not move is the shape a stall takes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_since_ms: Option<i64>,
    /// When discovery last succeeded, which is not when it last tried: a worker whose fetches are all failing
    /// looks busy and learns nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempted_ms: Option<i64>,
    /// Which runtime process is running, counted per supervisor. A number that climbs is a runtime that keeps
    /// being replaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<u64>,
    /// Why the last attempt did not run, cut to the length every other reported string is. What a bundle is
    /// refused for is a name and a version, never a value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    /// What every receiver the running deployment reads listens on, which is where its senders send. Empty while
    /// nothing runs, and from a worker older than this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listens: Vec<Listening>,
    /// The name this worker's host goes by, which a sender reaches it under when a receiver listens on every
    /// address. `None` where the host would not say, and from a worker older than this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
}

/// One receiver's address, as its worker listens on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Listening {
    /// The source the receiver is.
    pub source: String,
    pub transport: crate::plan::topology::Transport,
    /// Every address of the host when its IP is unspecified.
    pub address: std::net::SocketAddr,
}

/// What this worker -- its supervisor and the runtime it runs -- is spending against what it is
/// allowed to run in (`resources.rs`). Worker-level, like [`Supervising`]: CPU and memory belong
/// to the whole worker, not to one of its threads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceReport {
    /// `None` on a worker's first heartbeat: a rate needs a previous reading, and reporting zero
    /// would say "idle" about a worker that has simply just started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_percent: Option<f64>,
    /// How many cores `cpu_percent` is a percentage of: a container's own quota where one was
    /// found, every core the host has otherwise. Fractional under a quota, which is what a k8s
    /// `cpu: 500m` limit is. Absent from a worker older than this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit_cores: Option<f64>,
    /// Whether `cpu_percent` is measured against a real container CPU quota rather than every
    /// core the host has.
    pub cpu_limited: bool,
    pub memory_used_bytes: u64,
    /// What `memory_used_bytes` is measured against. `None` only when neither a container memory
    /// limit nor the host's own total could be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_bytes: Option<u64>,
    /// Whether `memory_limit_bytes` is a real container memory limit rather than the host's own
    /// total.
    pub memory_limited: bool,
    /// Free and total bytes of the filesystem under this worker's state directory. Absent from a
    /// worker older than these fields, and from one whose filesystem would not answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_free_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_total_bytes: Option<u64>,
}

impl From<crate::resources::ResourceUsage> for ResourceReport {
    fn from(u: crate::resources::ResourceUsage) -> ResourceReport {
        ResourceReport {
            cpu_percent: u.cpu_percent,
            cpu_limit_cores: Some(u.cpu_limit_cores),
            cpu_limited: u.cpu_limited,
            memory_used_bytes: u.memory_used_bytes,
            memory_limit_bytes: u.memory_limit_bytes,
            memory_limited: u.memory_limited,
            disk_free_bytes: u.disk_free_bytes,
            disk_total_bytes: u.disk_total_bytes,
        }
    }
}

/// One worker thread's report, as of this heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadHeartbeat {
    pub thread_id: String,
    /// The group this thread is running under now.
    pub group_id: String,
    /// Absent from a worker older than the pool, which reported reader/transform pairs as one thread.
    #[serde(default)]
    pub kind: Option<ThreadKind>,
    pub partitions_owned: Vec<PartitionRef>,
    pub metrics: Vec<MetricSampleDto>,
}

/// Worker → leader: the worker's id and one report per thread.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    pub worker_id: String,
    /// What this worker has said about itself since the leader last acknowledged any. Oldest first, capped at
    /// [`LOGS_PER_HEARTBEAT`]; the rest ride the next beat.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<LogDto>,
    /// This worker process, told apart from its own previous lives, so a log's sequence number means
    /// something across a restart. Zero from a worker too old to send one.
    #[serde(default)]
    pub boot_id: u64,
    /// The group this worker runs. Stated by the worker rather than read off its thread reports, because a worker
    /// running nothing reports no threads -- and a worker that cannot deploy is exactly the one whose logs
    /// are worth finding. Empty from a worker too old to send one, and then absent from the wire entirely.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group_id: String,
    /// What this worker was started with (`smeltr.toml`'s `[worker.heartbeat] stop_after_unreachable_secs`), so
    /// the leader can show it without depending on a policy that only makes sense once contact is already lost.
    /// `None` if the worker was started with no limit -- it never stops on its own.
    #[serde(default)]
    pub stop_after_unreachable_secs: Option<u64>,
    /// The commit this worker is running the bundle of. `None` before its first deployment, and after one that
    /// stopped. Reported rather than assumed: a worker that has refused every commit since keeps running an older
    /// one, and nothing else on the wire tells the leader which.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// This worker process's own build (`version::VERSION`), so a mixed-version fleet is visible from the leader.
    /// Empty from a worker older than this field.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub smeltr_version: String,
    /// The release this worker's build is (`version::RELEASE`). `None` on any other build, and from a worker older
    /// than this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<String>,
    /// The leader-stored secrets this worker already holds, as name and version, so the leader answers with only
    /// what it lacks. Empty on a worker that holds none, which every worker is until its first answer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_versions: Vec<(String, i64)>,
    /// What this worker's supervisor is doing about what it runs. Absent from a worker too old to send one, and
    /// then absent from the wire entirely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervising: Option<Supervising>,
    /// What this worker process is spending against what it is allowed to run in. Absent from a worker too old to
    /// send one, and then absent from the wire entirely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceReport>,
    /// When the certificate authority this worker verifies the leader against stops being valid, in epoch
    /// milliseconds. Reported rather than read off the leader's own copy, because what stops a worker is the copy
    /// the worker holds -- one given an authority years ago is the one that goes quiet. `None` from a worker
    /// verifying against the platform's own roots, and from one older than this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_expires_millis: Option<i64>,
    /// The roots this worker verifies its leader against, by fingerprint. A date says when this worker stops being
    /// able to verify anything; this says which authorities it would accept, which is what a leader rotating its
    /// own has to know before it signs under the new one. Empty from a worker verifying against the platform's own
    /// roots, and from one older than this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authority_fingerprints: Vec<String>,
    /// Whether this worker's trust is its leader's to replace: it takes the authority its leader hands it, or
    /// runs inside the leader's own process and follows what that leader serves. False for one started with
    /// `SMELTR_LEADER_CA` elsewhere: its trust is what its operator manages, and the fingerprints above are theirs
    /// rather than a set the leader can ever bring into line with its own.
    ///
    /// True from a worker too old to send it, which is what a leader has always assumed of every worker.
    #[serde(default = "taken")]
    pub takes_authority: bool,
    /// Set on the last heartbeat a worker sends before it stops on purpose, once its runtime has drained. A worker
    /// that says this and then goes quiet has stopped rather than failed, and the leader says nothing about its
    /// silence until it comes back. False from a worker older than this field, which is every stop unannounced.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stopping: bool,
    /// What this worker does when its leader asks. Empty from a worker older than directives, which the leader
    /// then never asks; a worker inside the leader's own process leaves out a shutdown, which would end the leader.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepts: Vec<DirectiveAction>,
    /// The last directive this worker took, by id. The leader repeats a directive until a beat names it here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub took: Option<String>,
    /// How often this worker beats, as its own configuration sets it. The leader holds a source for this worker
    /// for three of these, or of its own, whichever is longer. Absent from a worker older than this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_interval_ms: Option<u64>,
    /// Every single-worker source this worker's deployment runs, whether or not it holds it: the workers that may
    /// be given its hold. Empty from a worker running none, and from one older than this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub single_worker: Vec<SourceKey>,
    /// What each source this worker holds has delivered since the leader last acknowledged any, oldest first. The
    /// ledger entries across every source are capped at [`LEDGER_PER_HEARTBEAT`]; the rest ride the next beat.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub progress: Vec<SourceProgress>,
    /// What this worker saw of its data since the leader last acknowledged any ([`StreamReport`]). Absent from a
    /// worker older than this field, and from one that ran nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streams: Option<StreamReport>,
    /// Which window of this worker process's life (`boot_id`) `streams` is, counted from 1. A window the leader
    /// has not acknowledged is sent again under the same number, and the leader records each number once. Absent
    /// from a worker older than this field, whose every window is recorded as it arrives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streams_window: Option<u64>,
    pub threads: Vec<ThreadHeartbeat>,
}

/// How many ledger entries, added and removed, one heartbeat carries. At [`MAX_LEDGER_ITEM`] and
/// [`MAX_LEDGER_ENTRY`] apiece that is about 11 MB, inside [`HEARTBEAT_BODY_LIMIT`] beside a full batch of logs.
pub const LEDGER_PER_HEARTBEAT: usize = 8192;

/// The longest name a ledger keeps for one item: S3's and Azure's longest key.
pub const MAX_LEDGER_ITEM: usize = 1024;

/// The longest record a ledger keeps of one item's read.
pub const MAX_LEDGER_ENTRY: usize = 256;

/// The longest cursor a source keeps.
pub const MAX_CURSOR: usize = 1024;

/// One single-worker source, as its progress is kept. The place is what the source reads, as its kind names it
/// (a bucket and prefix, a table), so a source pointed somewhere else starts afresh rather than skipping what it
/// never read.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceKey {
    pub source: String,
    pub place: String,
}

/// One item a source has read, by name, and the source's own record of the read: the version it read and how far.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LedgerEntry {
    pub item: String,
    pub entry: String,
}

/// What one held source has delivered: every destination has acknowledged it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceProgress {
    #[serde(flatten)]
    pub key: SourceKey,
    #[serde(flatten)]
    pub progress: crate::io::Progress,
}

/// A single-worker source the leader holds for this worker, and where it resumes. The ledger is fetched on its
/// own (`/api/ledger`), since it can be far larger than a heartbeat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hold {
    #[serde(flatten)]
    pub key: SourceKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

// -- streams ------------------------------------------------------------------

/// How many fields one Parser's shape names in one report, and how many keys one source's unmatched rows name. Past
/// either, a report counts what it left out instead of naming it (`fields_dropped`, `keys_dropped`): a field name
/// is data rather than structure, so without a bound a source inventing names would grow a heartbeat without one.
pub const STREAM_FIELDS_PER_PARSER: usize = 256;
pub const STREAM_KEYS_PER_SOURCE: usize = 64;
/// The longest field name or stream key a report carries. A longer one is counted as dropped.
pub const STREAM_NAME_LEN: usize = 256;

/// What a worker saw of the data flowing through it since it last reported (design doc, Observability): the fields
/// each Parser produced from one batch per interval, and the streams each source received that no route took.
/// Names and counts only, never a record's value. The worker keeps no baseline: it reports and forgets, and the
/// leader accumulates (`leader::series`, the stream tables). Empty where nothing ran.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamReport {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parsers: Vec<ParserShape>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unmatched: Vec<UnmatchedStreams>,
}

/// What one Parser produced from the batches it tallied, on one source's route. Keyed by the author's own ids, which
/// survive a redeploy where a `FunctionId` does not. The same Parser on two routes, or one pipeline on two sources, is
/// two shapes: each sees different rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParserShape {
    pub source: String,
    pub route: String,
    pub pipeline: String,
    pub step: String,
    /// Rows the Parser read in the tallied batches, which is every field's `present` denominator.
    pub rows: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<FieldShape>,
    /// Fields seen past [`STREAM_FIELDS_PER_PARSER`], or with a name past [`STREAM_NAME_LEN`], as rows.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub fields_dropped: u64,
}

/// One field a Parser produced: how many rows held it, and as what. The counts are `core::tally::TagCounts`'s, summed
/// over batches; `suggested` is the latest window's reading, since the leader can recompute it from the counts but
/// not a declared type.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldShape {
    pub name: String,
    /// Whether the Parser's declaration named it. A declared field's `suggested` is its declared type.
    pub declared: bool,
    pub suggested: String,
    pub present: u64,
    pub bools: u64,
    pub ints: u64,
    pub floats: u64,
    pub strings: u64,
    pub arrays: u64,
    pub objects: u64,
    /// Rows whose value would not coerce to the declared type. Zero unless `declared`.
    pub failed: u64,
    /// Rows whose value read as a time (`TagCounts::times`). Zero unless undeclared; absent from a build before it.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub times: u64,
}

/// The rows of one source that no route took, by the stream they belong to: what the source's kind files a record
/// under (`SourceMeta::stream_key`), so a sender a source has never seen before shows as a key it has never seen.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnmatchedStreams {
    pub source: String,
    /// Every unmatched row, keyed or not.
    pub rows: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<StreamKey>,
    /// Rows under a key past [`STREAM_KEYS_PER_SOURCE`], or past [`STREAM_NAME_LEN`].
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub keys_dropped: u64,
}

/// One stream's key and how many unmatched rows it had. An empty key is a record its source filed under nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamKey {
    pub key: String,
    pub rows: u64,
}

fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

impl StreamReport {
    pub fn is_empty(&self) -> bool {
        self.parsers.is_empty() && self.unmatched.is_empty()
    }

    /// This report and `later`, one: the counts sum, a shape's `declared` and `suggested` are the later reading, and
    /// what either left out stays left out. The caps hold on the result, so two capped reports fold into one capped
    /// report with the overflow counted rather than named.
    pub fn absorb(&mut self, later: StreamReport) {
        for shape in later.parsers {
            let same = |p: &ParserShape| {
                p.source == shape.source
                    && p.route == shape.route
                    && p.pipeline == shape.pipeline
                    && p.step == shape.step
            };
            match self.parsers.iter_mut().find(|p| same(p)) {
                Some(held) => held.absorb(shape),
                None => self.parsers.push(shape),
            }
        }
        for streams in later.unmatched {
            match self.unmatched.iter_mut().find(|u| u.source == streams.source) {
                Some(held) => held.absorb(streams),
                None => self.unmatched.push(streams),
            }
        }
    }
}

impl ParserShape {
    /// Fold `later`'s counts into this shape; see [`StreamReport::absorb`].
    pub fn absorb(&mut self, later: ParserShape) {
        self.rows += later.rows;
        self.fields_dropped += later.fields_dropped;
        for field in later.fields {
            match self.fields.iter().position(|f| f.name == field.name) {
                Some(at) => self.fields[at].absorb(field),
                None if self.fields.len() < STREAM_FIELDS_PER_PARSER => self.fields.push(field),
                None => self.fields_dropped += field.present,
            }
        }
    }
}

impl FieldShape {
    /// Fold `later`'s counts into this field; see [`StreamReport::absorb`].
    pub fn absorb(&mut self, later: FieldShape) {
        self.declared = later.declared;
        self.suggested = later.suggested;
        self.present += later.present;
        self.bools += later.bools;
        self.ints += later.ints;
        self.floats += later.floats;
        self.strings += later.strings;
        self.arrays += later.arrays;
        self.objects += later.objects;
        self.failed += later.failed;
        self.times += later.times;
    }
}

impl UnmatchedStreams {
    /// Fold `later`'s counts into this source's; see [`StreamReport::absorb`].
    pub fn absorb(&mut self, later: UnmatchedStreams) {
        self.rows += later.rows;
        self.keys_dropped += later.keys_dropped;
        for key in later.keys {
            match self.keys.iter().position(|k| k.key == key.key) {
                Some(at) => self.keys[at].rows += key.rows,
                None if self.keys.len() < STREAM_KEYS_PER_SOURCE => self.keys.push(key),
                None => self.keys_dropped += key.rows,
            }
        }
    }
}

/// What [`HeartbeatRequest::takes_authority`] means from a worker that does not say.
fn taken() -> bool {
    true
}

/// Leader → worker: what to run is `main`, which the worker fetches for itself -- naming a commit would put the
/// leader between a worker and a ref it may not own.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    /// Every capture the leader still wants from this worker, whole on every answer. The leader wants nothing more
    /// from this worker of a session an answer leaves out -- the capture is over, this worker has answered, or it was
    /// saved, discarded, swept, or lost to a leader that restarted. The worker lets go of such a session still
    /// filling, and of what it was lent for it; one it has finished stays until its report is sent. Empty whenever
    /// nobody is debugging, which is what keeps capture off the steady-state path: a worker holds no capture
    /// machinery until a session names it. An absent key is the empty list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub captures: Vec<Invited>,
    /// Credentials this worker's group needs and does not hold. Empty whenever it is up to date, which is every
    /// heartbeat but the few after a rotation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<SecretDelivery>,
    /// The authorities to verify this leader by, PEM, sent whenever this worker reports holding something other
    /// than what the leader publishes. A worker is reached only when it asks, so this is the one moment a trust
    /// store can be changed: it rides the connection the worker's own token authenticates, verified against the
    /// authority the worker already holds.
    ///
    /// Whole, and replacing: a root this leaves out is one the worker stops trusting. That is how a retired
    /// authority reaches a fleet.
    ///
    /// Absent whenever the worker already holds what is published, which is almost always -- an authority nobody
    /// is replacing is a kilobyte no beat should carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<String>,
    /// What an operator asked this worker to do, repeated on every answer until the worker's beat says it took it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directive: Option<Directive>,
    /// Every single-worker source this worker holds, whole on every answer. A source this leaves out is one this
    /// worker stops reading. An absent key is the empty list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holds: Vec<Hold>,
}

/// One thing an operator asked of one worker. The id tells a repeat from a new request: a worker acts on each id
/// once, however many answers carry it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Directive {
    pub id: String,
    pub action: DirectiveAction,
}

/// What a directive asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DirectiveAction {
    /// End the deployment and start it again, in the same process.
    Restart,
    /// End the deployment, say the worker is stopping, and exit: what a SIGTERM does.
    Shutdown,
    /// An action from a newer build, which this one does not do.
    #[serde(other)]
    Unknown,
}

/// One leader-stored secret, sent to a worker whose group's bundle names it.
///
/// A value leaves the leader here and in [`CaptureInvitation::secrets`], and nowhere else. It travels under TLS on
/// the channel the worker's own token authenticates, and lives in worker memory from there -- as an environment
/// variable's value always has.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretDelivery {
    pub name: String,
    pub version: i64,
    pub value: String,
}

/// Names the version, never the value (`plan::topology::HeldSecrets` does the same for the same reason). Written
/// here rather than on each type that carries one, so a container's derived `Debug` is safe without knowing it
/// holds a secret -- every `{:?}` reaching a value has to pass through this.
impl std::fmt::Debug for SecretDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretDelivery({} v{})", self.name, self.version)
    }
}

/// One capture a worker is asked to run: which source or destination, what to keep, and what bounds it. Exactly one
/// of `source` and `destination` is set.
///
/// A source's configuration travels with the invitation rather than being named. A capture is most useful on a
/// source nobody is running yet -- one an author just wrote on a branch -- and a worker follows `main`, so it has no
/// bundle to look that source up in. A destination is named: a capture there reads only what a running deployment
/// sends.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureInvitation {
    pub session: String,
    /// The worker group this capture is for. A worker runs only the invitations naming its own group.
    pub group: String,
    /// The source as the author has it. A worker already reading a source of this name taps what is flowing, unless
    /// the capture reads the source itself (`reads_source_itself`); one that is not opens this configuration itself
    /// and never commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<crate::config_file::SourceDto>,
    /// A deployed destination, by the author's name for it. A worker whose deployment sends to it copies each row its
    /// egress thread takes; one whose deployment does not answers that it runs no such destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,
    /// The identity that source names, where it names one. A worker has no bundle to look it up in either.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<crate::config_file::IdentityDto>,
    /// A `Bool` expression over the record and the source's declared fields, compiled the way a route condition is
    /// (`plan::topology`). `None` keeps everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// Where a stream source's capture starts reading, when its author chose a start other than the latest. A
    /// capture with one reads the source itself under its own group on every worker, a deployed one included,
    /// since what a deployment taps is only what arrives now. `None` reads from the latest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<crate::config_file::StartPositionDto>,
    /// Bounds, each holding back a different thing: how many records this worker may return, how much memory its
    /// buffer may hold, and how long it may wait. The first one reached ends the capture here, and an answer that
    /// no longer lists it ends it sooner.
    pub max_matches: usize,
    pub max_bytes: usize,
    pub deadline_secs: u64,
    /// The credentials this capture's own source names, resolved by the leader when the capture was opened.
    ///
    /// A capture's whole use is a source the deployed bundle does not name -- one an author just wrote -- so its
    /// credentials are not what a worker is entitled to hold. They arrive with the session, open that one source,
    /// and go when it ends: they never enter the worker's `HeldSecrets`, and are never reported back as held.
    /// Empty for a source whose credentials are all worker-local, which the worker has already.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<SecretDelivery>,
}

impl CaptureInvitation {
    /// Whether this capture reads its source itself on a worker whose deployment already reads it, rather than
    /// tapping what the deployment reads: an SQL Run, which reads its query as its author has it now, or a capture
    /// that starts somewhere other than the latest.
    pub fn reads_source_itself(&self) -> bool {
        self.start.is_some() || self.source.as_ref().is_some_and(|s| s.kind.runs_its_own_query())
    }
}

/// One entry of [`HeartbeatResponse::captures`] as this build reads it. An invitation this build cannot read -- a
/// newer leader of the same release line naming a source field or kind this build does not know -- is kept apart
/// rather than failing the whole answer, which carries the worker's secrets, authority and directives too. The
/// worker answers it with a failed report, so the capture says which worker could not take part.
#[derive(Debug, Clone)]
pub enum Invited {
    Readable(Box<CaptureInvitation>),
    Unreadable { session: String },
}

impl Invited {
    pub fn session(&self) -> &str {
        match self {
            Invited::Readable(invitation) => &invitation.session,
            Invited::Unreadable { session } => session,
        }
    }
}

impl Serialize for Invited {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Invited::Readable(invitation) => invitation.serialize(serializer),
            Invited::Unreadable { session } => serde_json::json!({ "session": session }).serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Invited {
    /// Unreadable only when the entry still names its session: without one, there is nothing to answer, and the
    /// answer is malformed rather than newer.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entry = serde_json::Value::deserialize(deserializer)?;
        let session = entry.get("session").and_then(|s| s.as_str()).map(str::to_string);
        match (serde_json::from_value::<CaptureInvitation>(entry), session) {
            (Ok(invitation), _) => Ok(Invited::Readable(Box::new(invitation))),
            (Err(_), Some(session)) => Ok(Invited::Unreadable { session }),
            (Err(e), None) => Err(serde::de::Error::custom(e)),
        }
    }
}

/// What one worker captured. Posted to the leader, which merges every worker's answer into one sample.
///
/// Posted more than once: a worker still filling posts its growth so far, with `finished: None`, on the same
/// schedule it checks in on everything else. `finished: Some(reason)` is the last one -- why it stopped, for the
/// leader to show beside the count -- and the leader stops waiting on this worker once it sees one.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureReport {
    pub session: String,
    pub worker_id: String,
    /// What the worker holds for the session, as batches: one at a source, one per route at a destination.
    pub batches: Vec<CaptureBatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished: Option<CaptureEnd>,
    /// What Run found out about an SQL source's query, beyond its rows. `None` from any other source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<SqlRun>,
    /// The fields the newest Avro writer's schema among the records offered the session declares, its filter's or
    /// not, in its order. Empty from a source that reads no Avro, or before one has read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub avro: Vec<AvroField>,
}

/// One field an Avro writer's schema declares: a leaf, a nested record's named with dots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AvroField {
    pub name: String,
    /// The schema's own spelling of its type, such as `long` or `union[null, string]`.
    pub avro_type: String,
    /// The type it reads as, spelled as a source declares one.
    pub suggested: String,
}

/// What Run found out about an SQL source's query: the columns its result has, and what the database plans for a
/// poll.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlRun {
    pub columns: Vec<SqlColumn>,
    /// `None` until the plan is known, and for a mode that reads the whole result every poll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<SqlVerdict>,
}

/// One column of a query's result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlColumn {
    pub name: String,
    /// The database's own name for its type.
    pub database_type: String,
    /// The type a field reads it as, spelled as a pipeline declares one.
    pub suggested: String,
    /// The table it is read from, when it is a plain column of one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Whether that table has a unique index or primary key on this column alone.
    #[serde(default)]
    pub unique: bool,
}

/// What the database plans for a poll's statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SqlVerdict {
    /// Every relation is reached through an index, or is small. Both fields default, so a verdict from a build that
    /// sent neither still reads.
    Indexed {
        /// Each index the plan reads through, by name, in plan order, once.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        indexes: Vec<String>,
        /// The database's estimate of the rows the poll returns.
        #[serde(default)]
        rows: u64,
    },
    /// The poll reads the whole of `relation`, about `rows` rows.
    FullScan { relation: String, rows: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Which bound ended a worker's capture. Named for the bound so the leader can say why a sample is the size it is.
pub enum CaptureEnd {
    Matches,
    Bytes,
    Deadline,
    /// The source would not open, the filter would not compile, or polling it failed -- with the reason as the
    /// worker found it, standing in for the log that would otherwise be the only place it appeared.
    /// `CaptureReport::batches` is what was captured before it stopped.
    Failed(String),
}

/// What a capture saw, as columns: each named and typed once, then rows of values in column order. The same at a
/// source and at a destination: at a source the columns are the record's fields and then its `__source` fields, at a
/// destination the fields the route's pipeline writes, then every `__` field its pipeline holds
/// (`worker::capture::internal_columns`), then the `__destination` fields that place the row there
/// (`DESTINATION_COLUMNS`).
///
/// Text, not bytes: a byte source's `_raw` is decoded with the source's own encoding (design doc, Columnar data model)
/// before it leaves, the debt a paste has already paid.
///
/// On the wire a column whose value is the same on every row is sent once, with its name, and its rows carry nothing
/// for it (`WireBatch`). In memory every row holds every value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(into = "WireBatch", try_from = "WireBatch")]
pub struct CaptureBatch {
    pub columns: Vec<CaptureColumn>,
    pub rows: Vec<CapturedRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureColumn {
    pub name: String,
    /// The column's type as a declaration spells it (`DataType::spell`).
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CapturedRow {
    /// One per column, in column order. A field the row does not have is null.
    pub values: Vec<serde_json::Value>,
    /// The columns whose value is not their type: the value is the text as the source spelled it, and the pipeline
    /// read it as a field-parse failure.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<usize>,
    /// The worker that sent the row. Set by the leader, which folds every worker's batches into one capture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
}

/// The columns a destination batch adds after the route's fields, in this order: the topic, index or key prefix the
/// row chose there; its document id there; and the route that made it.
pub const DESTINATION_COLUMNS: [&str; 3] = ["__destination.placement", "__destination.id", "__destination.route"];

impl CaptureBatch {
    pub fn new(columns: Vec<CaptureColumn>) -> CaptureBatch {
        CaptureBatch {
            columns,
            rows: Vec::new(),
        }
    }

    /// The bytes the batch's names and types cost, once. What capture limits count with each row's `size`.
    pub fn header_size(&self) -> usize {
        self.columns.iter().map(|c| c.name.len() + c.ty.len()).sum()
    }

    /// The bytes this batch adds to a capture: its names and types once, and every row.
    pub fn size(&self) -> usize {
        self.header_size() + self.rows.iter().map(CapturedRow::size).sum::<usize>()
    }

    /// The column named `name`.
    pub fn column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

impl CapturedRow {
    /// The bytes this row adds to a capture: each value as it travels, a text value its text.
    pub fn size(&self) -> usize {
        self.values.iter().map(CapturedRow::value_size).sum()
    }

    /// The bytes one value adds: a text value its text, any other its JSON.
    fn value_size(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::String(text) => text.len(),
            other => other.to_string().len(),
        }
    }
}

/// A batch as it travels: a column that is the same on every row carries that `value`, and is left out of every row's
/// `values`. A batch of one row sends nothing as constant: there is nothing to save.
#[derive(Serialize, Deserialize)]
struct WireBatch {
    columns: Vec<WireColumn>,
    rows: Vec<CapturedRow>,
}

#[derive(Serialize, Deserialize)]
struct WireColumn {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<serde_json::Value>,
}

impl From<CaptureBatch> for WireBatch {
    fn from(b: CaptureBatch) -> WireBatch {
        let constant: Vec<bool> = (0..b.columns.len())
            .map(|c| {
                // A null is never constant: `"value": null` reads back as no value at all.
                b.rows.len() > 1
                    && b.rows[0].values.get(c).is_some_and(|v| !v.is_null())
                    && b.rows
                        .iter()
                        .all(|r| !r.failed.contains(&c) && r.values.get(c) == b.rows[0].values.get(c))
            })
            .collect();
        let columns = b
            .columns
            .into_iter()
            .enumerate()
            .map(|(c, col)| WireColumn {
                name: col.name,
                ty: col.ty,
                value: constant[c].then(|| b.rows[0].values.get(c).cloned().unwrap_or_default()),
            })
            .collect();
        // A failed column is never constant, so its index among the columns a row still carries is its index less
        // the constants before it.
        let packed = |c: usize| c - constant[..c].iter().filter(|k| **k).count();
        let rows = b
            .rows
            .into_iter()
            .map(|r| CapturedRow {
                values: r
                    .values
                    .into_iter()
                    .enumerate()
                    .filter(|(c, _)| !constant.get(*c).copied().unwrap_or(false))
                    .map(|(_, v)| v)
                    .collect(),
                failed: r.failed.iter().map(|c| packed(*c)).collect(),
                worker: r.worker,
            })
            .collect();
        WireBatch { columns, rows }
    }
}

/// What one value costs once it is copied onto a row, beyond its text: a constant that is empty still takes a value's
/// room on every row.
const EXPANDED_VALUE_COST: usize = 16;

impl TryFrom<WireBatch> for CaptureBatch {
    type Error = String;

    /// Refused where its constants, copied onto every row, would cost more than any capture may hold: a worker bounds
    /// what it sends by that, so only a report no worker made gets there, and it would be read before anything checks
    /// who sent it.
    fn try_from(w: WireBatch) -> Result<CaptureBatch, String> {
        let constants: usize = w
            .columns
            .iter()
            .filter_map(|c| c.value.as_ref())
            .map(|v| CapturedRow::value_size(v) + EXPANDED_VALUE_COST)
            .sum();
        if constants.saturating_mul(w.rows.len()) > crate::leader::capture::MAX_BYTES_CEILING {
            return Err(format!(
                "a batch whose constants over its {} rows would hold more than a capture may",
                w.rows.len()
            ));
        }
        let carried: Vec<usize> = (0..w.columns.len()).filter(|c| w.columns[*c].value.is_none()).collect();
        let rows = w
            .rows
            .into_iter()
            .map(|r| {
                let mut values = r.values.into_iter();
                CapturedRow {
                    values: w
                        .columns
                        .iter()
                        .map(|col| match &col.value {
                            Some(v) => v.clone(),
                            None => values.next().unwrap_or_default(),
                        })
                        .collect(),
                    failed: r.failed.iter().filter_map(|c| carried.get(*c).copied()).collect(),
                    worker: r.worker,
                }
            })
            .collect();
        Ok(CaptureBatch {
            columns: w
                .columns
                .into_iter()
                .map(|c| CaptureColumn { name: c.name, ty: c.ty })
                .collect(),
            rows,
        })
    }
}

mod client {
    use super::{CAPTURE_BUDGET, HeartbeatRequest, HeartbeatResponse, LOOKUP_FILE_BUDGET, REQUEST_BUDGET};

    /// The POST failed or its response did not parse. Never fatal: the caller tries again next interval. Carries
    /// the status the leader answered with, where it answered, and whether the leader's certificate was refused.
    #[derive(Debug)]
    pub struct HeartbeatError(String, Option<u16>, bool);

    impl HeartbeatError {
        /// Whether the leader answered that it does not know this worker's credential: never issued, or its worker
        /// forgotten. A revoked one is answered otherwise.
        pub fn credential_unknown(&self) -> bool {
            self.1 == Some(401)
        }

        /// Whether the leader's certificate is one this worker's roots do not accept. Retrying changes nothing until
        /// the roots or the certificate do.
        pub fn certificate_untrusted(&self) -> bool {
            self.2
        }

        /// What went wrong, without the "heartbeat failed" the display puts in front.
        pub fn reason(&self) -> &str {
            &self.0
        }
    }

    impl std::fmt::Display for HeartbeatError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "heartbeat failed: {}", self.0)
        }
    }

    impl std::error::Error for HeartbeatError {}

    /// The worker-side sender: one per process, one request per interval carrying every thread's report.
    pub struct HeartbeatClient {
        heartbeat_url: String,
        /// Where a finished capture goes. The same token gates it: a capture is the worker answering the leader, on
        /// the leader's own invitation.
        capture_url: String,
        /// Sent as `Authorization: Bearer <token>` on every call.
        token: String,
        agent: ureq::Agent,
        /// The same configuration as `agent` on a budget sized to a capture rather than a beat.
        capture_agent: ureq::Agent,
        /// Where the leader's lookup store is served, by oid.
        lookup_url: String,
        /// Where the ledger of a source this worker holds is served. Fetched on the lookup budget: a ledger, like a
        /// lookup file, can be far larger than a beat.
        ledger_url: String,
        /// The same configuration again, on a budget sized to a lookup file.
        lookup_agent: ureq::Agent,
    }

    /// A failed attempt, and whether the connection failed rather than the leader answered.
    struct SendFailure {
        retryable: bool,
        error: HeartbeatError,
    }

    /// An agent that verifies nothing, for the one request a worker makes before it has anything to verify with:
    /// fetching the authority its enrollment code names.
    ///
    /// Safe only because of what the caller does with the answer. The fingerprint in the enrollment code is checked
    /// against the bytes that come back, so an attacker in the way of this request can serve whatever they like and
    /// every byte of it is refused. Used for anything else, this is a worker that trusts the network.
    ///
    /// It gives up after `budget` like every other agent here: this is a worker's first request, and one that
    /// waits forever on an address that accepts and never answers is a first boot that never reports why.
    pub fn agent_without_verification(budget: std::time::Duration) -> ureq::Agent {
        let tls_config = ureq::tls::TlsConfig::builder().disable_verification(true).build();
        agent_with(tls_config, budget)
    }

    /// An agent that accepts a leader whose certificate chains to a root of [`leader_roots`], and gives up after
    /// `budget`.
    ///
    /// `None` is the public roots, which is what a leader behind a proxy holding a publicly issued certificate needs,
    /// and what a leader on plain HTTP does not use at all.
    ///
    /// `leader_ca_pem` may hold more than one authority. That is what lets an authority be replaced without
    /// stopping the fleet: a worker given the old and the new accepts a leader that has moved and one that has not.
    pub fn agent_trusting(
        leader_ca_pem: Option<&[u8]>,
        trust_public: bool,
        budget: std::time::Duration,
    ) -> Result<ureq::Agent, String> {
        Ok(agent_with(tls_trusting(leader_ca_pem, trust_public)?, budget))
    }

    /// An agent that gives up after `budget`, trusting what `tls` trusts.
    ///
    /// Every agent that talks to the leader is built here. `ureq` bounds no part of a request on its own, so an
    /// agent built anywhere else waits on a leader that accepts and never answers for as long as the socket stays
    /// open.
    fn agent_with(tls: ureq::tls::TlsConfig, budget: std::time::Duration) -> ureq::Agent {
        ureq::Agent::new_with_config(
            ureq::Agent::config_builder()
                .timeout_global(Some(budget))
                .tls_config(tls)
                .build(),
        )
    }

    /// What [`agent_trusting`] trusts, as a configuration rather than an agent, for a caller that builds more than
    /// one agent from it.
    fn tls_trusting(leader_ca_pem: Option<&[u8]>, trust_public: bool) -> Result<ureq::tls::TlsConfig, String> {
        let roots = leader_roots(leader_ca_pem, trust_public)?
            .iter()
            .map(|der| ureq::tls::Certificate::from_der(der.as_ref()).to_owned())
            .collect();
        Ok(ureq::tls::TlsConfig::builder()
            .root_certs(ureq::tls::RootCerts::Specific(std::sync::Arc::new(roots)))
            .build())
    }

    /// Every root a worker verifies its leader by: the authorities in `leader_ca_pem`, and the public roots too when
    /// `trust_public` (`io::trust::trusting`), as an identity trusts. The heartbeat and git's fetch both trust this,
    /// so the two never disagree about a leader.
    pub fn leader_roots(
        leader_ca_pem: Option<&[u8]>,
        trust_public: bool,
    ) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
        let Some(pem) = leader_ca_pem else {
            return Ok(crate::io::trust::trusting(Vec::new(), trust_public));
        };

        let mut authorities = Vec::new();
        for item in ureq::tls::parse_pem(pem) {
            match item.map_err(|e| e.to_string())? {
                ureq::tls::PemItem::Certificate(cert) => {
                    authorities.push(rustls::pki_types::CertificateDer::from(cert.der().to_vec()))
                }
                // An operator who reached for the combined file their tooling calls the CA has named the wrong
                // one, and a worker copies this into its state directory for git to use as well. Skipping the key
                // says nothing about a mistake worth hearing about; a trust store is certificates.
                ureq::tls::PemItem::PrivateKey(_) => {
                    return Err("holds a private key. Point this at the certificate alone".to_string());
                }
                _ => return Err("holds something that is not a certificate".to_string()),
            }
        }
        // A file holding none would leave only the public roots, or an empty root store, and every request would
        // fail at the handshake with a message about the leader rather than about the file.
        if authorities.is_empty() {
            return Err("holds no certificate".to_string());
        }
        Ok(crate::io::trust::trusting(authorities, trust_public))
    }

    impl HeartbeatClient {
        /// `leader_ca_pem` and `trust_public` are what the leader's certificate must chain to; see
        /// [`agent_trusting`].
        pub fn new(
            leader_url: impl AsRef<str>,
            token: impl Into<String>,
            leader_ca_pem: Option<&[u8]>,
            trust_public: bool,
        ) -> Result<HeartbeatClient, HeartbeatError> {
            // One authority, parsed once, trusted by both agents: what a leader is judged by does not depend on
            // which call is being made.
            let tls_config = tls_trusting(leader_ca_pem, trust_public)
                .map_err(|e| HeartbeatError(format!("the leader's certificate authority: {e}"), None, false))?;
            let base = leader_url.as_ref().trim_end_matches('/');
            Ok(HeartbeatClient {
                heartbeat_url: format!("{base}/api/heartbeat"),
                capture_url: format!("{base}/api/capture-report"),
                token: token.into(),
                agent: agent_with(tls_config.clone(), REQUEST_BUDGET),
                capture_agent: agent_with(tls_config.clone(), CAPTURE_BUDGET),
                lookup_url: format!("{base}/api/lookup-store"),
                ledger_url: format!("{base}/api/ledger"),
                lookup_agent: agent_with(tls_config, LOOKUP_FILE_BUDGET),
            })
        }

        /// The bytes of the lookup file `oid` names in the leader's store, as they arrive. The caller checks them
        /// against the pointer it asked by.
        pub fn lookup_file(&self, oid: &str) -> Result<impl std::io::Read + use<>, HeartbeatError> {
            self.lookup_agent
                .get(format!("{}/{oid}", self.lookup_url))
                .header("Authorization", &format!("Bearer {}", self.token))
                .call()
                .map(|response| response.into_body().into_reader())
                .map_err(|e| HeartbeatError(e.to_string(), None, false))
        }

        /// The ledger the leader keeps for a source it holds for this worker, as it arrives: one
        /// [`super::LedgerEntry`] per line. The leader refuses it for a source this worker does not hold now.
        pub fn fetch_ledger(&self, key: &super::SourceKey) -> Result<impl std::io::Read + use<>, HeartbeatError> {
            self.lookup_agent
                .get(&self.ledger_url)
                .query("source", &key.source)
                .query("place", &key.place)
                .header("Authorization", &format!("Bearer {}", self.token))
                .call()
                .map(|response| response.into_body().into_reader())
                .map_err(|e| HeartbeatError(e.to_string(), None, false))
        }

        /// Hand one finished capture to the leader.
        ///
        /// One attempt. The caller holds the report until this returns `Ok` and offers it again if it does not;
        /// the leader keeps the first answer from each worker and ignores the rest, so a report arriving twice
        /// cannot double a sample.
        pub fn report_capture(&self, report: &super::CaptureReport) -> Result<(), HeartbeatError> {
            self.capture_agent
                .post(&self.capture_url)
                .header("Authorization", &format!("Bearer {}", self.token))
                .send_json(report)
                .map(|_| ())
                .map_err(|e| HeartbeatError(e.to_string(), None, false))
        }

        /// One heartbeat, retried once if the connection failed: a server may close a pooled idle connection as the
        /// next request reuses it, and `ureq` does not retry a POST itself.
        ///
        /// Safe only because the leader replaces each thread's last metrics rather than summing them, so a request that
        /// arrives twice overwrites rather than double-counts. Anything downstream that accumulates these samples needs
        /// an idempotency key first. A status code is the leader's answer and is not retried.
        pub fn send(&self, req: &HeartbeatRequest) -> Result<HeartbeatResponse, HeartbeatError> {
            match self.send_once(req) {
                Err(e) if e.retryable => self.send_once(req).map_err(|e| e.error),
                other => other.map_err(|e| e.error),
            }
        }

        fn send_once(&self, req: &HeartbeatRequest) -> Result<HeartbeatResponse, SendFailure> {
            let mut response = self
                .agent
                .post(&self.heartbeat_url)
                .header("Authorization", &format!("Bearer {}", self.token))
                .send_json(req)
                .map_err(|e| SendFailure {
                    // The exchange did not complete, as opposed to the leader having answered.
                    retryable: matches!(
                        e,
                        ureq::Error::Io(_) | ureq::Error::Timeout(_) | ureq::Error::Protocol(_)
                    ),
                    error: HeartbeatError(
                        e.to_string(),
                        match e {
                            ureq::Error::StatusCode(status) => Some(status),
                            _ => None,
                        },
                        matches!(e, ureq::Error::Rustls(rustls::Error::InvalidCertificate(_))),
                    ),
                })?;
            response
                .body_mut()
                .read_json::<HeartbeatResponse>()
                .map_err(|e| SendFailure {
                    // Served; re-sending would be a second heartbeat.
                    retryable: false,
                    error: HeartbeatError(e.to_string(), None, false),
                })
        }
    }
}

pub use client::{HeartbeatClient, HeartbeatError, agent_trusting, agent_without_verification, leader_roots};

#[cfg(test)]
mod trust_tests {
    use super::{REQUEST_BUDGET, agent_trusting, leader_roots};

    /// As an identity trusts: the authority, and the public roots unless the worker says not to.
    #[test]
    fn a_worker_trusts_its_leaders_authority_and_the_public_roots_unless_told_not_to() {
        let authority = rcgen::generate_simple_self_signed(vec!["leader.test".to_string()])
            .unwrap()
            .cert;
        let pem = authority.pem();
        let public = crate::io::trust::public();

        let alone = leader_roots(Some(pem.as_bytes()), false).unwrap();
        assert_eq!(alone, vec![authority.der().clone()]);
        let with_public = leader_roots(Some(pem.as_bytes()), true).unwrap();
        assert_eq!(with_public.len(), public.len() + 1);
        assert!(with_public.contains(authority.der()));
        assert_eq!(leader_roots(None, true).unwrap(), public);
        assert!(leader_roots(None, false).unwrap().is_empty());
    }

    #[test]
    fn a_worker_given_no_authority_falls_back_to_the_platforms_own_roots() {
        assert!(agent_trusting(None, true, REQUEST_BUDGET).is_ok());
    }

    #[test]
    fn a_file_holding_no_certificate_is_refused_by_name() {
        let refused = agent_trusting(Some(b"not a certificate"), true, REQUEST_BUDGET).expect_err("nothing to trust");
        assert!(refused.contains("holds no certificate"), "{refused}");
    }

    /// A private key is what an operator copies when they reach for the wrong file in the leader's `tls/`
    /// directory. It is not a certificate, and saying so beats a handshake failing later.
    #[test]
    fn a_file_holding_only_a_key_is_refused_too() {
        let key = rcgen::KeyPair::generate().unwrap().serialize_pem();
        let refused =
            agent_trusting(Some(key.as_bytes()), true, REQUEST_BUDGET).expect_err("a key is not an authority");
        assert!(refused.contains("holds a private key"), "{refused}");
    }

    /// The harder version of the same mistake: the file does hold the certificate, so keeping only that and
    /// saying nothing would work, and leave a worker copying the signing key into its state directory.
    #[test]
    fn a_file_holding_a_certificate_and_its_key_is_refused() {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let both = format!("{}{}", cert.pem(), key.serialize_pem());

        let refused = agent_trusting(Some(both.as_bytes()), true, REQUEST_BUDGET).expect_err("a key came with it");
        assert!(refused.contains("holds a private key"), "{refused}");
    }
}

#[cfg(test)]
mod tests {
    /// A build that knew no index names sent a bare `indexed`; a leader that reads them must still read it.
    #[test]
    fn an_indexed_verdict_without_its_fields_still_reads() {
        let old: super::SqlVerdict = serde_json::from_str(r#"{"type":"indexed"}"#).unwrap();
        assert_eq!(
            old,
            super::SqlVerdict::Indexed {
                indexes: Vec::new(),
                rows: 0
            }
        );
        let new = super::SqlVerdict::Indexed {
            indexes: vec!["orders_pkey".into()],
            rows: 20,
        };
        let text = serde_json::to_string(&new).unwrap();
        assert_eq!(text, r#"{"type":"indexed","indexes":["orders_pkey"],"rows":20}"#);
        assert_eq!(serde_json::from_str::<super::SqlVerdict>(&text).unwrap(), new);
    }

    /// The bound that keeps one verbose destination from wedging a worker. Without it, a refusal reason carrying a
    /// response body makes a request the leader answers with a status code -- which `send` does not retry, so the
    /// same oversized body is rebuilt every beat and that worker never heartbeats again.
    #[test]
    fn what_rides_the_wire_is_bounded_however_much_a_call_site_said() {
        let event = crate::logs::Event {
            timestamp: std::time::UNIX_EPOCH,
            level: tracing::Level::ERROR,
            site: "runtime.rows_rejected".into(),
            message: "x".repeat(10_000),
            fields: (0..64)
                .map(|i| (format!("field{i}").into(), crate::logs::Value::Str("y".repeat(10_000))))
                .collect(),
            worker: None,
            thread: None,
            local_only: false,
            seq: 1,
        };

        let dto = LogDto::from(&event);
        assert!(
            dto.message.chars().count() <= MAX_TEXT_LEN + 1,
            "the message is cut: {}",
            dto.message.len()
        );
        assert_eq!(dto.fields.len(), MAX_LOG_FIELDS, "and so is the number of fields");
        for (name, value) in &dto.fields {
            let FieldValueDto::Str(text) = value else {
                panic!("{name} should be a string")
            };
            assert!(text.chars().count() <= MAX_TEXT_LEN + 1, "every value is cut too");
        }

        // The arithmetic the body limit rests on: a whole batch of the worst event there can be, leaving room
        // for everything else that rides the same request. A worker with 256 reader threads reporting 32
        // metrics each -- far more than one should have -- measures a megabyte, so two is an allowance
        // nothing has come near.
        const BESIDE_THE_LOGS: usize = 2 * 1024 * 1024;
        let worst = serde_json::to_string(&vec![dto; LOGS_PER_HEARTBEAT]).unwrap().len();
        assert!(
            worst + BESIDE_THE_LOGS < HEARTBEAT_BODY_LIMIT,
            "a full batch is {worst}, and with {BESIDE_THE_LOGS} beside it must fit {HEARTBEAT_BODY_LIMIT}"
        );
    }

    /// A rename or a retagging here breaks a worker against a leader that has not been restarted with it, and
    /// every other fixture in the tree sends an empty vector behind `skip_serializing_if`.
    #[test]
    fn a_log_serializes_to_the_wire_spelling() {
        let dto = LogDto {
            seq: 3,
            timestamp_ms: 1_757_689_445_123,
            level: "error".to_string(),
            site: "runtime.rows_rejected".to_string(),
            message: "a destination refused rows".to_string(),
            thread: Some("egress-1".to_string()),
            boot_id: None,
            fields: vec![
                ("subject".to_string(), FieldValueDto::Str("d1".to_string())),
                ("rows".to_string(), FieldValueDto::Uint(4000)),
            ],
        };
        let json = serde_json::to_value(&dto).unwrap();
        assert_eq!(json["seq"], 3);
        assert_eq!(json["timestampMs"], 1_757_689_445_123i64);
        assert_eq!(json["site"], "runtime.rows_rejected");
        assert_eq!(json["fields"][0][0], "subject");
        assert_eq!(json["fields"][0][1], serde_json::json!({"kind": "str", "value": "d1"}));
        assert_eq!(json["fields"][1][1], serde_json::json!({"kind": "uint", "value": 4000}));

        let back: LogDto = serde_json::from_value(json).unwrap();
        assert_eq!(back.seq, dto.seq);
        assert_eq!(back.fields.len(), 2);
    }

    use super::*;

    use crate::metrics::MetricsHandle;
    use std::sync::Arc;

    /// A successful heartbeat: peek, ship, commit.
    fn heartbeat_ok(registry: &Metrics) -> HashMap<String, Vec<MetricSampleDto>> {
        let peeked = peek_for_heartbeat(registry);
        let split = split_by_thread(&peeked);
        commit_for_heartbeat(registry, &peeked);
        split
    }

    /// A heartbeat whose send failed: read, but never commit.
    fn heartbeat_failed(registry: &Metrics) {
        let _ = peek_for_heartbeat(registry);
    }

    #[test]
    fn a_heartbeat_carries_every_sample_as_a_dto() {
        let registry = Arc::new(Metrics::new());
        let t0 = MetricsHandle::new(registry.clone(), "t0");
        t0.incr("smeltr_rows_polled_total", &[("source", "0")], 5);
        t0.gauge("smeltr_source_exhausted", &[("source", "0")], 1.0);

        let by_thread = heartbeat_ok(&registry);
        let dtos = &by_thread["t0"];
        assert_eq!(dtos.len(), 2);
        let rows_polled = dtos.iter().find(|d| d.metric == "smeltr_rows_polled_total").unwrap();
        assert!(rows_polled.labels.contains(&("source".to_string(), "0".to_string())));
        assert!(matches!(rows_polled.value, MetricValueDto::Counter(5)));
    }

    #[test]
    fn samples_are_split_back_out_per_thread() {
        let registry = Arc::new(Metrics::new());
        MetricsHandle::new(registry.clone(), "t0").incr("smeltr_rows_polled_total", &[], 5);
        MetricsHandle::new(registry.clone(), "t1").incr("smeltr_rows_polled_total", &[], 7);

        let by_thread = heartbeat_ok(&registry);
        assert_eq!(by_thread.len(), 2);
        assert!(matches!(by_thread["t0"][0].value, MetricValueDto::Counter(5)));
        assert!(matches!(by_thread["t1"][0].value, MetricValueDto::Counter(7)));
    }

    /// An idle counter still reports, at 0: the leader tells idle from a worker that stopped beating.
    #[test]
    fn a_second_read_with_no_new_writes_reports_zero() {
        let registry = Arc::new(Metrics::new());
        MetricsHandle::new(registry.clone(), "t0").incr("smeltr_rows_polled_total", &[], 5);
        assert_eq!(heartbeat_ok(&registry)["t0"].len(), 1);
        let second = heartbeat_ok(&registry);
        assert!(matches!(
            second["t0"][..],
            [MetricSampleDto {
                value: MetricValueDto::Counter(0),
                ..
            }]
        ));
    }

    /// A failed send leaves its window owed, so the next heartbeat reports
    /// the whole span.
    #[test]
    fn counts_from_a_failed_heartbeat_are_reported_by_the_next_one() {
        let registry = Arc::new(Metrics::new());
        let t0 = MetricsHandle::new(registry.clone(), "t0");

        t0.incr("smeltr_rows_polled_total", &[], 5);
        heartbeat_failed(&registry);
        t0.incr("smeltr_rows_polled_total", &[], 3);

        let recovered = &heartbeat_ok(&registry)["t0"];
        assert!(
            matches!(recovered[0].value, MetricValueDto::Counter(8)),
            "expected the failed window's 5 plus the new 3, got {:?}",
            recovered[0].value
        );
    }

    #[test]
    fn a_delivered_heartbeats_counts_are_not_reported_twice() {
        let registry = Arc::new(Metrics::new());
        let t0 = MetricsHandle::new(registry.clone(), "t0");

        t0.incr("smeltr_rows_polled_total", &[], 5);
        assert!(matches!(
            heartbeat_ok(&registry)["t0"][0].value,
            MetricValueDto::Counter(5)
        ));
        t0.incr("smeltr_rows_polled_total", &[], 3);
        assert!(matches!(
            heartbeat_ok(&registry)["t0"][0].value,
            MetricValueDto::Counter(3)
        ));
    }

    /// The heartbeat and a `MetricsSource` each see every increment: a source's read moves its own cursor and
    /// never the heartbeat's.
    #[test]
    fn two_subscribers_each_see_every_increment() {
        let registry = Arc::new(Metrics::new());
        MetricsHandle::new(registry.clone(), "t0").incr("smeltr_rows_polled_total", &[], 5);

        let source = registry.take_delta(&mut crate::metrics::Cursor::default());
        let heartbeat = registry.peek_delta(crate::metrics::SUBSCRIBER_HEARTBEAT);

        let five = |read: &[MetricSample]| {
            matches!(
                read,
                [MetricSample {
                    value: MetricValue::Counter(5),
                    ..
                }]
            )
        };
        assert!(five(&source), "{source:?}");
        assert!(
            five(&heartbeat),
            "the source's read took the heartbeat's count: {heartbeat:?}"
        );
    }

    /// A worker older than the transform pool sends no `kind`. It still enrolls, heartbeats and receives a directive;
    /// what it loses is being counted against a stage.
    #[test]
    fn a_heartbeat_without_a_thread_kind_is_still_read() {
        let json = r#"{"workerId":"worker-1","threads":[{"threadId":"t0","groupId":"prod-orders","partitionsOwned":[],"metrics":[]}]}"#;
        let req: HeartbeatRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.threads[0].kind, None);
    }

    /// A worker older than the supervisor split sends no `supervising`, and one that has not deployed sends none
    /// either. Both read as a worker whose recovery state is simply not known, rather than as a refusal to parse.
    #[test]
    fn a_heartbeat_without_a_supervising_block_is_still_read() {
        let json = r#"{"workerId":"worker-1","threads":[]}"#;
        let req: HeartbeatRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.supervising, None);
        assert_eq!(req.resources, None);
    }

    /// The spelling the leader reads this by, pinned the same way [`Supervising`] is.
    #[test]
    fn a_resource_report_serializes_to_the_wire_spelling() {
        let json = serde_json::to_value(ResourceReport {
            cpu_percent: Some(42.5),
            cpu_limit_cores: Some(2.5),
            cpu_limited: true,
            memory_used_bytes: 104_857_600,
            memory_limit_bytes: Some(536_870_912),
            memory_limited: true,
            disk_free_bytes: Some(21_474_836_480),
            disk_total_bytes: Some(107_374_182_400),
        })
        .unwrap();
        for key in [
            "cpuPercent",
            "cpuLimitCores",
            "cpuLimited",
            "memoryUsedBytes",
            "memoryLimitBytes",
            "memoryLimited",
            "diskFreeBytes",
            "diskTotalBytes",
        ] {
            assert!(json.get(key).is_some(), "{key} is not on the wire: {json}");
        }
        // Every optional field absent at once: a worker's first beat, a host with neither a
        // container limit nor a readable total, and a filesystem that would not answer.
        assert_eq!(
            serde_json::to_value(ResourceReport::default()).unwrap(),
            serde_json::json!({"cpuLimited": false, "memoryUsedBytes": 0, "memoryLimited": false})
        );
    }

    /// The spelling the leader reads these by, pinned the same way every other field on this request is: a rename
    /// here is a field the other binary silently stops seeing.
    #[test]
    fn a_supervising_block_serializes_to_the_wire_spelling() {
        let json = serde_json::to_value(Supervising {
            desired_commit: Some("c0ffee".to_string()),
            phase: Some("starting".to_string()),
            phase_since_ms: Some(17),
            discovered_ms: Some(18),
            attempted_ms: Some(19),
            incarnation: Some(3),
            failure: Some("a reason".to_string()),
            listens: vec![Listening {
                source: "web".to_string(),
                transport: crate::plan::topology::Transport::Tcp,
                address: "0.0.0.0:8088".parse().unwrap(),
            }],
            host_name: Some("host-a".to_string()),
        })
        .unwrap();
        for key in [
            "desiredCommit",
            "phase",
            "phaseSinceMs",
            "discoveredMs",
            "attemptedMs",
            "incarnation",
            "failure",
            "listens",
            "hostName",
        ] {
            assert!(json.get(key).is_some(), "{key} is not on the wire: {json}");
        }
        assert_eq!(
            json["listens"][0],
            serde_json::json!({"source": "web", "transport": "tcp", "address": "0.0.0.0:8088"})
        );
        // Every field is optional, so one that says nothing takes no room on a request that is sent every beat.
        assert_eq!(
            serde_json::to_value(Supervising::default()).unwrap(),
            serde_json::json!({})
        );
    }

    /// The streams a heartbeat carries are the same two-binary contract: their keys are pinned, and a heartbeat
    /// without them is one from a worker that ran nothing, or an older one.
    #[test]
    fn a_stream_report_has_a_wire_spelling_and_is_optional() {
        let report = StreamReport {
            parsers: vec![ParserShape {
                source: "orders".into(),
                route: "to-es".into(),
                pipeline: "normalize".into(),
                step: "parse".into(),
                rows: 3,
                fields: vec![FieldShape {
                    name: "total".into(),
                    declared: true,
                    suggested: "f64".into(),
                    present: 3,
                    floats: 3,
                    ..Default::default()
                }],
                fields_dropped: 1,
            }],
            unmatched: vec![UnmatchedStreams {
                source: "hec".into(),
                rows: 2,
                keys: vec![StreamKey {
                    key: "okta:system".into(),
                    rows: 2,
                }],
                keys_dropped: 0,
            }],
        };
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["parsers"][0]["fieldsDropped"], 1);
        assert_eq!(json["parsers"][0]["fields"][0]["suggested"], "f64");
        assert_eq!(json["unmatched"][0]["keys"][0]["key"], "okta:system");
        assert!(
            json["unmatched"][0].get("keysDropped").is_none(),
            "a zero is left off the wire"
        );
        let back: StreamReport = serde_json::from_value(json).unwrap();
        assert_eq!(back, report);

        let bare: HeartbeatRequest = serde_json::from_value(serde_json::json!({
            "workerId": "w1", "bootId": 1, "groupId": "g", "smeltrVersion": "", "threads": [], "logs": []
        }))
        .unwrap();
        assert!(bare.streams.is_none());
        assert!(!serde_json::to_string(&bare).unwrap().contains("streams"));
    }

    /// A worker and its leader are deployed separately, so the JSON spelling is the contract between two binaries that
    /// may be different builds. Asserting the keys pins `rename_all` and the tagged `MetricValueDto`; a round trip
    /// through one build's own serde would pass no matter what they were renamed to.
    #[test]
    fn a_heartbeat_request_serializes_to_the_wire_spelling() {
        let req = HeartbeatRequest {
            heartbeat_interval_ms: None,
            single_worker: Vec::new(),
            progress: Vec::new(),
            streams: None,
            streams_window: None,
            stopping: false,
            authority_fingerprints: Vec::new(),
            takes_authority: true,
            logs: Vec::new(),
            boot_id: 0,
            group_id: "prod-orders".to_string(),
            secret_versions: Vec::new(),
            supervising: None,
            resources: None,
            worker_id: "worker-1".to_string(),
            stop_after_unreachable_secs: Some(300),
            commit: Some("a1b2c3d".to_string()),
            smeltr_version: "2026.9.20+main.a1b2c3d".to_string(),
            release: Some("1.2.0".to_string()),
            authority_expires_millis: None,
            threads: vec![ThreadHeartbeat {
                kind: Some(ThreadKind::Reader),
                thread_id: "t0".to_string(),
                group_id: "prod-orders".to_string(),
                partitions_owned: vec![PartitionRef {
                    topic: "orders".to_string(),
                    partition: 3,
                }],
                metrics: vec![MetricSampleDto {
                    metric: "smeltr_rows_polled_total".to_string(),
                    labels: vec![("source".to_string(), "0".to_string())],
                    value: MetricValueDto::Counter(12),
                }],
            }],
            accepts: Vec::new(),
            took: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"workerId":"worker-1","bootId":0,"groupId":"prod-orders","stopAfterUnreachableSecs":300,"commit":"a1b2c3d","smeltrVersion":"2026.9.20+main.a1b2c3d","release":"1.2.0","takesAuthority":true,"threads":[{"threadId":"t0","groupId":"prod-orders","kind":"reader","partitionsOwned":[{"topic":"orders","partition":3}],"metrics":[{"metric":"smeltr_rows_polled_total","labels":[["source","0"]],"value":{"kind":"counter","value":12}}]}]}"#
        );
        let back: HeartbeatRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.worker_id, "worker-1");
        assert_eq!(back.stop_after_unreachable_secs, Some(300));
        assert_eq!(back.commit.as_deref(), Some("a1b2c3d"));
        assert_eq!(back.smeltr_version, "2026.9.20+main.a1b2c3d");
        assert_eq!(back.release.as_deref(), Some("1.2.0"));
        assert_eq!(back.threads[0].partitions_owned[0].partition, 3);
        assert!(matches!(back.threads[0].metrics[0].value, MetricValueDto::Counter(12)));

        // A beat from a worker too old to send it. A leader has always replaced such a worker's authority, and
        // reading the absence as a refusal would leave it verifying by a root nothing signs any more.
        let older: HeartbeatRequest = serde_json::from_str(&json.replace(r#""takesAuthority":true,"#, "")).unwrap();
        assert!(older.takes_authority);

        // The key a worker's logs ride under, pinned separately because the fixture above carries none and an
        // empty list is left off the wire. It is the same two-binary contract as everything else here.
        let carrying = HeartbeatRequest {
            heartbeat_interval_ms: None,
            single_worker: Vec::new(),
            progress: Vec::new(),
            streams: None,
            streams_window: None,
            stopping: false,
            authority_fingerprints: Vec::new(),
            takes_authority: true,
            logs: vec![LogDto {
                seq: 0,
                timestamp_ms: 1,
                level: "warn".to_string(),
                site: "worker.started".to_string(),
                message: "worker starting".to_string(),
                thread: None,
                boot_id: None,
                fields: Vec::new(),
            }],
            ..req
        };
        let json = serde_json::to_string(&carrying).unwrap();
        assert!(json.contains(r#""logs":[{"seq":0,"#), "{json}");
    }

    /// The other direction of the same contract. The answer names no commit and no thread: what to run is `main`,
    /// which the worker fetches for itself.
    #[test]
    fn a_heartbeat_response_serializes_to_the_wire_spelling() {
        let resp = HeartbeatResponse {
            holds: Vec::new(),
            authority: None,
            captures: Vec::new(),
            secrets: Vec::new(),
            directive: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{}"#);
        // Absent is the empty list: a worker lets go of every capture it holds still filling.
        let back: HeartbeatResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.captures.len(), 0);
    }

    /// `Failed` carries the reason the worker found, as a bare string rather than an object, so an analyst reads what
    /// broke rather than the word "failed" alone.
    #[test]
    fn a_failed_capture_end_carries_its_reason_on_the_wire() {
        let end = CaptureEnd::Failed("the broker refused the credential named in KAFKA_SASL_PASSWORD".to_string());
        let json = serde_json::to_string(&end).unwrap();
        assert_eq!(
            json,
            r#"{"failed":"the broker refused the credential named in KAFKA_SASL_PASSWORD"}"#
        );
        assert_eq!(serde_json::from_str::<CaptureEnd>(&json).unwrap(), end);
        assert_eq!(serde_json::to_string(&CaptureEnd::Matches).unwrap(), r#""matches""#);
    }

    /// `MetricValueDto::Text` never comes out of the registry -- `Metrics` has no string-valued kind -- so a caller
    /// builds one directly for a fact with no natural number, and it has to survive the same wire round trip a
    /// counter or a gauge does.
    fn column(name: &str, ty: &str) -> CaptureColumn {
        CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        }
    }

    fn row(values: serde_json::Value) -> CapturedRow {
        CapturedRow {
            values: values.as_array().unwrap().clone(),
            ..CapturedRow::default()
        }
    }

    /// Workers and leaders one release apart still trade heartbeats, and a field the newer one adds is one the
    /// older one has never heard of.
    #[test]
    fn a_capture_batch_from_a_newer_worker_still_reads() {
        let batch: CaptureBatch = serde_json::from_str(
            r#"{"columns":[{"name":"_raw","type":"str","added_later":1}],"rows":[{"values":["a"],"added_later":1}]}"#,
        )
        .unwrap();
        assert_eq!(batch.rows[0].values, [serde_json::json!("a")]);
    }

    /// A column the same on every row travels once, with its name, and reads back onto every row; a failed value is
    /// never folded into a constant, and keeps naming its own column.
    #[test]
    fn a_constant_column_travels_once_and_reads_back_on_every_row() {
        let mut batch = CaptureBatch::new(vec![
            column("_raw", "str"),
            column("__source.topic", "str"),
            column("n", "i64"),
            column("gone", "str"),
        ]);
        batch.rows = vec![
            row(serde_json::json!(["a", "orders", 1, null])),
            row(serde_json::json!(["b", "orders", "x", null])),
        ];
        batch.rows[1].failed = vec![2];
        let json = serde_json::to_string(&batch).unwrap();
        assert_eq!(
            json,
            r#"{"columns":[{"name":"_raw","type":"str"},{"name":"__source.topic","type":"str","value":"orders"},{"name":"n","type":"i64"},{"name":"gone","type":"str"}],"rows":[{"values":["a",1,null]},{"values":["b","x",null],"failed":[1]}]}"#
        );
        let back: CaptureBatch = serde_json::from_str(&json).unwrap();
        assert_eq!(back, batch);
    }

    /// A report no worker made, whose constants would copy onto every row past what any capture may hold, is refused
    /// as it is read.
    #[test]
    fn a_report_whose_constants_would_outgrow_any_capture_is_refused() {
        let constant = "x".repeat(1024 * 1024);
        let rows = vec![serde_json::json!({"values": []}); 1000];
        let wire = serde_json::json!({
            "columns": [{"name": "_raw", "type": "str", "value": constant}],
            "rows": rows,
        });
        let err = serde_json::from_value::<CaptureBatch>(wire).unwrap_err();
        assert!(err.to_string().contains("more than a capture may"), "{err}");

        let empties = serde_json::json!({
            "columns": (0..100_000).map(|i| serde_json::json!({"name": format!("c{i}"), "type": "str", "value": ""}))
                .collect::<Vec<_>>(),
            "rows": vec![serde_json::json!({"values": []}); 1000],
        });
        assert!(
            serde_json::from_value::<CaptureBatch>(empties).is_err(),
            "empty constants still cost a value each"
        );
    }

    /// One row has nothing to share, so every value travels with it.
    #[test]
    fn a_batch_of_one_row_sends_no_constant() {
        let mut batch = CaptureBatch::new(vec![column("_raw", "str")]);
        batch.rows = vec![row(serde_json::json!(["a"]))];
        assert_eq!(
            serde_json::to_string(&batch).unwrap(),
            r#"{"columns":[{"name":"_raw","type":"str"}],"rows":[{"values":["a"]}]}"#
        );
    }

    /// A batch costs its names and types once, and each row its values.
    #[test]
    fn a_batch_counts_its_names_once_and_its_values_per_row() {
        let mut batch = CaptureBatch::new(vec![column("_raw", "str"), column("n", "i64")]);
        batch.rows = vec![row(serde_json::json!(["ab", 10])), row(serde_json::json!(["c", 7]))];
        assert_eq!(
            batch.header_size(),
            "_raw".len() + "str".len() + "n".len() + "i64".len()
        );
        assert_eq!(batch.size(), batch.header_size() + (2 + 2) + (1 + 1));
    }

    /// A capture at a destination names it and carries no source.
    #[test]
    fn a_destination_capture_round_trips_on_the_wire() {
        let invitation: CaptureInvitation = serde_json::from_str(
            r#"{"session":"s1","group":"g","destination":"es","maxMatches":5,"maxBytes":100,"deadlineSecs":10}"#,
        )
        .unwrap();
        assert!(invitation.source.is_none());
        assert_eq!(invitation.destination.as_deref(), Some("es"));
        let json = serde_json::to_string(&invitation).unwrap();
        assert!(!json.contains("\"source\""), "{json}");
    }

    #[test]
    fn a_text_metric_value_round_trips_on_the_wire() {
        let sample = MetricSampleDto {
            metric: "smeltr_source_blocked_reason".to_string(),
            labels: vec![("source".to_string(), "3".to_string())],
            value: MetricValueDto::Text("destination 1 refused a record and is set to block".to_string()),
        };
        let json = serde_json::to_string(&sample).unwrap();
        assert_eq!(
            json,
            r#"{"metric":"smeltr_source_blocked_reason","labels":[["source","3"]],"value":{"kind":"text","value":"destination 1 refused a record and is set to block"}}"#
        );
        let back: MetricSampleDto = serde_json::from_str(&json).unwrap();
        assert!(
            matches!(back.value, MetricValueDto::Text(t) if t == "destination 1 refused a record and is set to block")
        );
    }

    /// A newer worker of the same release line may send a kind of metric this build does not read. That sample is
    /// set apart, and the rest of the beat still arrives.
    #[test]
    fn a_metric_kind_from_a_newer_build_is_set_apart() {
        let thread: ThreadHeartbeat = serde_json::from_value(serde_json::json!({
            "threadId": "t0",
            "groupId": "g",
            "partitionsOwned": [],
            "metrics": [
                {"metric": "a", "labels": [], "value": {"kind": "histogram", "value": {"buckets": [1, 2]}}},
                {"metric": "b", "labels": [], "value": {"kind": "seconds", "value": 0.5}},
                {"metric": "c", "labels": [], "value": {"kind": "counter", "value": 3}},
            ],
        }))
        .unwrap();
        assert!(matches!(thread.metrics[0].value, MetricValueDto::Unknown(_)));
        assert!(matches!(thread.metrics[1].value, MetricValueDto::Unknown(_)));
        assert!(matches!(thread.metrics[2].value, MetricValueDto::Counter(3)));
        assert!(thread.metrics[0].clone().into_sample().is_none());
        // The leader stores a report as JSON, so a sample it cannot read is written back as it came.
        let json = serde_json::to_value(&thread.metrics[1]).unwrap();
        assert_eq!(json["value"], serde_json::json!({"kind": "seconds", "value": 0.5}));
    }

    /// A newer leader of the same release line may name a source this build cannot read. That entry is set apart,
    /// and everything else in the answer still arrives.
    #[test]
    fn an_invitation_this_build_cannot_read_does_not_cost_the_answer() {
        let resp: HeartbeatResponse = serde_json::from_value(serde_json::json!({
            "captures": [
                {
                    "session": "s1", "group": "g",
                    "source": {"id": "probe", "type": "metrics", "bucketSeconds": 5},
                    "maxMatches": 1, "maxBytes": 1, "deadlineSecs": 1
                },
                {
                    "session": "s2", "group": "g",
                    "source": {"id": "probe", "type": "aKindFromANewerBuild"},
                    "maxMatches": 1, "maxBytes": 1, "deadlineSecs": 1
                }
            ],
            "authority": "-----BEGIN CERTIFICATE-----"
        }))
        .unwrap();
        assert!(matches!(&resp.captures[0], Invited::Readable(i) if i.session == "s1"));
        assert!(matches!(&resp.captures[1], Invited::Unreadable { session } if session == "s2"));
        assert!(resp.authority.is_some(), "the rest of the answer still arrives");

        assert!(
            serde_json::from_value::<HeartbeatResponse>(serde_json::json!({"captures": [{"group": "g"}]})).is_err(),
            "an entry with no session is nothing to answer"
        );
    }

    /// The rule that makes every container safe without knowing it holds one. A `CaptureInvitation` derives
    /// `Debug`, is held for the session's whole life on the leader and the worker both, and is printed by any
    /// `{:?}` that reaches it -- so the redaction lives on the value's own type, where nothing can route around it.
    #[test]
    fn a_secret_never_prints_its_value_however_it_is_reached() {
        let delivery = SecretDelivery {
            name: "kafka-cred".to_string(),
            version: 4,
            value: "hunter2-the-actual-password".to_string(),
        };
        let direct = format!("{delivery:?}");
        assert!(!direct.contains("hunter2"), "{direct}");
        assert!(direct.contains("kafka-cred") && direct.contains("v4"), "{direct}");

        let invitation = CaptureInvitation {
            session: "s1".to_string(),
            group: "g".to_string(),
            source: serde_json::from_str(r#"{"id":"probe","type":"metrics","bucketSeconds":5}"#).unwrap(),
            destination: None,
            identity: None,
            filter: None,
            start: None,
            max_matches: 1,
            max_bytes: 1,
            deadline_secs: 1,
            secrets: vec![delivery.clone()],
        };
        let nested = format!("{invitation:?}");
        assert!(
            !nested.contains("hunter2"),
            "a container's derived Debug must not be able to reach it: {nested}"
        );

        // Through a response, a vector, an Option -- every shape a caller might have in hand.
        let response = HeartbeatResponse {
            holds: Vec::new(),
            authority: None,
            captures: vec![Invited::Readable(Box::new(invitation))],
            secrets: vec![delivery.clone()],
            directive: None,
        };
        assert!(!format!("{response:?}").contains("hunter2"));
        assert!(!format!("{:?}", Some(vec![delivery])).contains("hunter2"));
    }

    /// Serialization is the opposite obligation: the value has to survive the wire, or the worker cannot open the
    /// source. Redaction is for a human reading a log, never for the protocol.
    #[test]
    fn a_secret_still_serializes_its_value_to_the_wire() {
        let json = serde_json::to_string(&SecretDelivery {
            name: "kafka-cred".to_string(),
            version: 4,
            value: "hunter2".to_string(),
        })
        .unwrap();
        assert_eq!(json, r#"{"name":"kafka-cred","version":4,"value":"hunter2"}"#);
    }
}
