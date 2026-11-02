//! The runtime in a process of its own, and the frames the supervisor talks to it in.
//!
//! A deployment is the one part of a worker that runs code a bundle chose, against connectors nobody here
//! controls. When it will not stop, nothing in the same process can make it: a Rust thread cannot be killed, and
//! starting a replacement beside an abandoned reader is two consumers on one source. What can be made to stop is
//! a process, so that is what a deployment is.
//!
//! The supervisor keeps the worker's identity, its heartbeat and its discovery. The child holds one deployment
//! and nothing that outlives it. Everything between them is a frame on a pipe:
//!
//! ```text
//!   supervisor                              runtime child
//!       |--- Start: commit, group, secrets ----->|   loads, compiles, opens connectors
//!       |<-- Ready ------------------------------|   only now is it running
//!       |<-- Report: threads, logs, progress ----|   on its own interval, while it runs
//!       |--- Capture: an invitation ------------>|
//!       |<-- Captured: a finished session -------|
//!       |--- Listed: captures still wanted ----->|   whenever the leader's list changes
//!       |--- Holds: sources this worker holds -->|   whenever the leader's list changes (`super::held`)
//!       |--- Backlogged: progress past bound --->|   whenever that changes
//!       |--- Stop ------------------------------>|   cooperative, with a deadline
//!       |<-- Stopped ----------------------------|
//! ```
//!
//! Secrets ride the pipe, which no other process on the host can read, rather than a command line or an
//! environment every child of this process would inherit.
//!
//! The child learns its supervisor is gone from end-of-file on that pipe, which is the containment unix offers;
//! on Windows the job object does it (`crate::process`). Neither is what ends a runtime that has stopped
//! answering -- that is the supervisor killing the process tree, and the reason this boundary exists.

use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::heartbeat::{CaptureInvitation, CaptureReport, LogDto, MetricSampleDto, MetricValueDto, PartitionRef};

/// Set on the child, and read before a leader or a worker is started. There is no argument parser here: a role is
/// configuration, and this is one more role.
pub const RUNTIME_ENV: &str = "SMELTR_RUNTIME_CHILD";

/// The supervisor's end of the control pipe.
///
/// Every frame is written on a thread of its own and waited for with a deadline, because a write to a runtime
/// that has stopped reading blocks: a pipe holds only what the operating system buffers, and a start carrying a
/// large secret set or an invitation carrying its own is past that. No schedule here may wait on a runtime
/// without a deadline -- that is the whole reason the deployment is a process.
///
/// A frame that timed out leaves the writer blocked on it, and the frames behind it queued. Both end when the
/// caller kills the tree, which closes the pipe.
struct Control {
    jobs: std::sync::mpsc::Sender<(ToRuntime, std::sync::mpsc::Sender<std::io::Result<()>>)>,
}

impl Control {
    fn over(mut stdin: std::process::ChildStdin) -> std::io::Result<Control> {
        let (jobs, queued) = std::sync::mpsc::channel::<(ToRuntime, std::sync::mpsc::Sender<std::io::Result<()>>)>();
        std::thread::Builder::new()
            .name("smeltr-runtime-control".to_string())
            .spawn(move || {
                for (frame, wrote) in queued {
                    let _ = wrote.send(send(&mut stdin, &frame));
                }
            })?;
        Ok(Control { jobs })
    }

    /// `Err` is a frame that did not go, a runtime that did not take it off the pipe inside `budget` included.
    ///
    /// A caller that gets `Err` owes the tree a kill: the writer may be blocked on that frame, and the pipe
    /// closing is the only thing that ends the block. Every caller today reaches `kill_tree` on this path, by
    /// dropping the handle or by stopping on its own deadline; a new one must too.
    fn send_within(&self, frame: ToRuntime, budget: Duration) -> std::io::Result<()> {
        let (wrote, written) = std::sync::mpsc::channel();
        self.jobs
            .send((frame, wrote))
            .map_err(|_| std::io::Error::other("this runtime's control pipe is closed"))?;
        match written.recv_timeout(budget) {
            Ok(outcome) => outcome,
            Err(_) => Err(std::io::Error::other(
                "a runtime did not take a frame off its control pipe in time",
            )),
        }
    }
}

/// The file inside a worker's state directory whose lock stands for "a runtime is running here".
///
/// Held by the runtime rather than the supervisor, because the case it answers is the supervisor being gone: an
/// orphan finishes its drain with nobody watching, and a service manager can start a replacement supervisor in
/// that time. The lock outlives the supervisor that spawned it and is released by the operating system when the
/// runtime itself exits, so the replacement's first runtime refuses to start until the orphan really has.
const RUNTIME_LOCK: &str = "runtime.lock";

/// How long an orphaned runtime gives its own drain before ending the process. Only reached when the supervisor
/// has gone: while one is there, its own hard deadline is what ends a drain that will not finish.
const ORPHAN_DRAIN_BUDGET: Duration = Duration::from_secs(120);

/// How long a runtime has to take one frame off its control pipe. It bounds a runtime that has stopped reading,
/// not one that is slow: what a frame asks for happens after it is read, under a budget of its own.
pub(crate) const CONTROL_FRAME_BUDGET: Duration = Duration::from_secs(30);

/// Set once this runtime has been signaled to stop. The report loop reads it beside the supervisor's own `Stop`.
static SIGNALED: AtomicBool = AtomicBool::new(false);

/// What a runtime does with SIGTERM or SIGINT: drain, exactly as a `Stop` frame would have it.
///
/// A service manager that signals every process in a unit -- systemd's default -- reaches the runtime beside its
/// supervisor, and a runtime that exited at once there would be the one stop in a worker that skipped the drain.
/// The supervisor sends `Stop` as well, and the second ask changes nothing. A second signal is somebody who will
/// not wait for the drain.
pub fn stop_on_signal() {
    if SIGNALED.swap(true, Ordering::Relaxed) {
        crate::logs::stop();
        std::process::exit(1);
    }
}

/// How often a running child reports what it is doing. The supervisor's heartbeat reads whatever arrived last, so
/// this is what bounds how stale a thread's facts can be on the wire.
const REPORT_INTERVAL: Duration = Duration::from_millis(500);

/// What the supervisor says to a runtime.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToRuntime {
    /// Run this commit. Sent once, immediately after the child starts.
    Start(Box<StartSpec>),
    /// Serve this capture, either by tapping a source already being read or by opening one of its own.
    Capture(Box<CaptureInvitation>),
    /// Every capture session the leader still lists for this worker. The runtime lets go of each one still filling
    /// that this leaves out. A struct variant: the enum is tagged by `kind`.
    Listed { sessions: Vec<String> },
    /// What the supervisor recorded and said since it last sent this, for a metrics or logs source here to read
    /// beside this process's own. Sent once per heartbeat.
    Mirror {
        samples: Vec<MetricSampleDto>,
        #[serde(default)]
        logs: Vec<LogDto>,
    },
    /// Every single-worker source the leader holds for this worker. The runtime reads each one it deploys while
    /// this names it, and drops each one it leaves out. `seq` grows with every frame, and each report names the
    /// last one applied.
    Holds { seq: u64, holds: Vec<super::held::Holding> },
    /// Whether the progress the supervisor holds for the leader is past its bound. While it is, held sources read
    /// nothing new (`super::held::BACKLOG_FULL`). Sent when it changes, and to a runtime as it starts.
    Backlogged { full: bool },
    /// Drain and exit. The supervisor kills the process tree if this is not answered in time.
    Stop,
}

/// What one runtime process holds for as long as it runs, shared by the deployment it starts and the supervisor's
/// commands it obeys: the captures it serves, the secrets it was sent, its metrics registry and the sources it holds.
#[derive(Clone, Default)]
pub struct Runtime {
    pub captures: Arc<super::capture::Captures>,
    pub secrets: crate::plan::topology::HeldSecrets,
    pub registry: Arc<crate::metrics::Metrics>,
    pub held: Arc<super::held::Held>,
}

/// Everything a runtime needs to become one. No path to a working tree: a commit is read out of the object store.
#[derive(Debug, Serialize, Deserialize)]
pub struct StartSpec {
    /// This worker's own directory. The runtime locks a file in it for as long as it lives, so no second runtime
    /// can read the same sources.
    pub state_dir: std::path::PathBuf,
    pub repo_dir: std::path::PathBuf,
    pub commit: String,
    pub group_id: String,
    pub thread_limit: usize,
    /// `WorkerConfig::memory`.
    #[serde(default)]
    pub memory: Option<usize>,
    pub worker_id: String,
    /// Only what this commit names. A runtime is not given credentials its own bundle does not quote.
    pub secrets: Vec<crate::heartbeat::SecretDelivery>,
    /// Which incarnation this is. Echoed on every frame the child sends, so a message from one the supervisor has
    /// replaced is recognized and dropped rather than attributed to its successor.
    pub incarnation: u64,
}

/// What a runtime says about itself.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FromRuntime {
    /// Loaded, compiled, connectors open, threads running. Not a heartbeat: it is said once, and says the startup
    /// steps completed rather than that the process is alive.
    Ready { incarnation: u64 },
    /// It could not start. The supervisor keeps whatever was running and retries the latest target.
    Failed { incarnation: u64, reason: String },
    /// What is running, as of now.
    Report {
        incarnation: u64,
        report: Box<RuntimeReport>,
    },
    /// A capture session that has ended, for the supervisor to hand to the leader.
    Captured {
        incarnation: u64,
        report: Box<CaptureReport>,
    },
    /// The drain finished and the process is about to exit.
    Stopped { incarnation: u64 },
}

/// One reader's published facts: what its thread keeps current behind a lock, and what crosses to the supervisor as
/// a snapshot whose age is the report interval.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReaderReport {
    pub thread_id: String,
    pub partitions: Vec<PartitionRef>,
    pub held: Option<MetricSampleDto>,
    pub connection_error: Option<MetricSampleDto>,
}

/// Everything the heartbeat says about a running deployment: what it is doing now, and what it has gathered since
/// the last report.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RuntimeReport {
    pub state: RuntimeState,
    pub gathered: Gathered,
}

/// What a deployment is doing now. A later report's replaces an earlier one's.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RuntimeState {
    pub readers: Vec<ReaderReport>,
    pub pool_thread_ids: Vec<String>,
    pub egress_ids: Vec<crate::core::id::DestId>,
    pub egress_refusals: Vec<(crate::core::id::DestId, Vec<MetricSampleDto>)>,
    /// What the metrics' subject labels resolve against: the topology they were written under.
    pub subjects: crate::metrics::Subjects,
    /// Every single-worker source this deployment runs, held here or not.
    pub single_worker: Vec<crate::heartbeat::SourceKey>,
    /// Every single-worker source this runtime holds now, as of the holds frame numbered `holds_applied`. One the
    /// supervisor sent in that frame or before and that this leaves out is a grant that failed.
    pub holding: Vec<crate::heartbeat::SourceKey>,
    pub holds_applied: u64,
}

/// What a deployment gathered since its last report, owed to the leader. A later report's adds to an earlier one's,
/// since the leader has had neither, and outlives the runtime that gathered it.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Gathered {
    /// The runtime's own registry since the last report. Committed on send rather than on the leader's
    /// acknowledgment: a runtime that is killed loses at most one window of counts, which is the trade
    /// Observability already makes for metrics and never makes for data.
    pub metrics: Vec<MetricSampleDto>,
    /// Each carries the boot id of the runtime that numbered it, so the leader dedupes a batch that arrives twice
    /// by sequence number, and two runtimes' sequences never collide under one key.
    pub logs: Vec<LogDto>,
    /// What held sources have delivered since the last report, oldest first. Owed to the supervisor once written,
    /// as the logs are, and carried on until the leader acknowledges it.
    pub progress: Vec<crate::heartbeat::SourceProgress>,
    /// What the deployment saw of its streams since the last report (`streams::Streams::drain`). Owed to the
    /// supervisor once written, as the logs are, and carried on until the leader acknowledges it.
    pub streams: Option<crate::heartbeat::StreamReport>,
}

impl RuntimeReport {
    /// This report brought up to date by a later one. What a thread is doing is whatever it is doing now; what it
    /// gathered is both windows, because the leader has had neither.
    pub fn followed_by(self, next: RuntimeReport) -> RuntimeReport {
        RuntimeReport {
            state: next.state,
            gathered: self.gathered.followed_by(next.gathered),
        }
    }
}

impl Gathered {
    /// Two windows the leader has not seen, as one.
    pub fn followed_by(self, next: Gathered) -> Gathered {
        // Two windows of streams are one: the counts sum (`StreamReport::absorb`).
        let streams = match (self.streams, next.streams) {
            (Some(mut earlier), Some(later)) => {
                earlier.absorb(later);
                Some(earlier)
            }
            (earlier, later) => earlier.or(later),
        };
        let mut metrics = self.metrics;
        // By series, so a report costs its own length rather than that times what is held: what waits for the
        // leader can be every series a large deployment has.
        let mut at: std::collections::HashMap<(String, Vec<(String, String)>), usize> = metrics
            .iter()
            .enumerate()
            .map(|(i, held)| ((held.metric.clone(), held.labels.clone()), i))
            .collect();
        for sample in next.metrics {
            let series = (sample.metric.clone(), sample.labels.clone());
            match at.get(&series).map(|&i| &mut metrics[i]) {
                // A counter arrives as the increase since the runtime last reported, so two windows the leader
                // has not seen are one increase. A gauge and a fact are whatever they last were.
                Some(held) => match (&mut held.value, sample.value) {
                    (MetricValueDto::Counter(held), MetricValueDto::Counter(added)) => *held += added,
                    (MetricValueDto::FloatCounter(held), MetricValueDto::FloatCounter(added)) => *held += added,
                    (held, newer) => *held = newer,
                },
                None => {
                    at.insert(series, metrics.len());
                    metrics.push(sample);
                }
            }
        }
        let mut logs = self.logs;
        logs.extend(next.logs);
        // The buffer a log came from is bounded and so is this: what a runtime says faster than its worker can
        // deliver is dropped at the oldest end, which is where the least useful of it is.
        let overflow = logs.len().saturating_sub(UNDELIVERED_LOGS);
        logs.drain(..overflow);
        if overflow > 0 {
            logs_dropped(overflow);
        }
        // Each is a step the leader applies in order, so none is merged or dropped.
        let mut progress = self.progress;
        progress.extend(next.progress);
        Gathered {
            metrics,
            logs,
            progress,
            streams,
        }
    }
}

/// How many of a runtime's logs a supervisor holds for it while the leader is not taking them. Several beats'
/// worth, so an unlucky heartbeat costs nothing, and bounded because a runtime that cannot stop talking must not
/// become a supervisor that cannot stop growing.
const UNDELIVERED_LOGS: usize = 4 * crate::heartbeat::LOGS_PER_HEARTBEAT;

/// How many ledger entries one report carries. A beat's worth at their longest is past what a frame may weigh, so
/// a quarter of one: the rest ride the reports that follow, one interval apart.
const LEDGER_PER_REPORT: usize = crate::heartbeat::LEDGER_PER_HEARTBEAT / 4;

/// How much of a report's frame its progress may take, as JSON. Entries escape to several times their length, so
/// a count alone does not bound it; half a frame leaves the rest to the threads, counts and logs beside it, and a
/// report refused for its size gives its progress back to the next.
const PROGRESS_BYTES_PER_REPORT: usize = MAX_FRAME / 2;

/// How much of a mirror frame its logs may take, as JSON. A count alone does not bound it, since a character
/// escapes to as many as six bytes; half a frame leaves the rest to the metrics beside them, and what does not fit
/// rides the next one.
pub(crate) const MIRROR_LOG_BYTES: usize = MAX_FRAME / 2;

// -- the wire ----------------------------------------------------------------

/// One frame per line, length-prefixed by the line itself: JSON never contains a bare newline, so a reader can
/// resynchronize without a framing header of its own.
fn send<T: Frame>(out: &mut impl Write, frame: &T) -> std::io::Result<()> {
    match write_frame(out, frame) {
        Ok(()) => Ok(()),
        Err(Refused::TooLarge { bytes, limit }) => Err(std::io::Error::other(format!(
            "a {bytes} byte frame is past the {limit} byte limit"
        ))),
        Err(Refused::Write(e)) => Err(e),
    }
}

/// Why a frame did not go, told apart because the answers differ: a frame too large will be too large every time
/// it is built, and one the pipe refused may not be.
enum Refused {
    TooLarge { bytes: usize, limit: usize },
    Write(std::io::Error),
}

fn write_frame<T: Frame>(out: &mut impl Write, frame: &T) -> Result<(), Refused> {
    let encoded = serde_json::to_string(frame).map_err(|e| Refused::Write(std::io::Error::other(e)))?;
    let limit = frame.limit();
    if encoded.len() > limit {
        return Err(Refused::TooLarge {
            bytes: encoded.len(),
            limit,
        });
    }
    out.write_all(encoded.as_bytes()).map_err(Refused::Write)?;
    out.write_all(b"\n").map_err(Refused::Write)?;
    out.flush().map_err(Refused::Write)
}

/// What one frame may weigh. A report grows with the threads and the log batch it carries, both already bounded;
/// this is the backstop that keeps a bug in either from becoming an allocation the reader cannot refuse.
const MAX_FRAME: usize = 8 * 1024 * 1024;

/// What a finished capture may weigh on the pipe: what the leader will accept from the supervisor afterward.
const MAX_CAPTURE_FRAME: usize = crate::leader::capture::REPORT_BODY_LIMIT;

/// What a frame carrying secrets may weigh. A start carries the value of every secret its commit names, and an
/// invitation carries the ones its own source names; nothing bounds either set in aggregate. Refusing such a
/// frame would be permanent, because every retry builds the same one, so the secret set is not what refuses it.
const MAX_SECRET_BEARING_FRAME: usize = crate::leader::capture::REPORT_BODY_LIMIT;

/// A frame knows its own weight limit, because a capture is allowed what a report is not.
trait Frame: Serialize {
    fn limit(&self) -> usize {
        MAX_FRAME
    }
}

impl Frame for ToRuntime {
    fn limit(&self) -> usize {
        match self {
            ToRuntime::Start(_) | ToRuntime::Capture(_) => MAX_SECRET_BEARING_FRAME,
            ToRuntime::Stop
            | ToRuntime::Listed { .. }
            | ToRuntime::Mirror { .. }
            | ToRuntime::Holds { .. }
            | ToRuntime::Backlogged { .. } => MAX_FRAME,
        }
    }
}

impl Frame for FromRuntime {
    fn limit(&self) -> usize {
        match self {
            FromRuntime::Captured { .. } => MAX_CAPTURE_FRAME,
            _ => MAX_FRAME,
        }
    }
}

fn frames<T: serde::de::DeserializeOwned>(from: impl std::io::Read) -> impl Iterator<Item = T> {
    BufReader::with_capacity(64 * 1024, from)
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| match serde_json::from_str::<T>(&line) {
            Ok(frame) => Some(frame),
            Err(e) => {
                frame_unreadable(&e);
                None
            }
        })
}

/// A frame the other process wrote that this one cannot read: a build this one does not know, most likely. Said
/// once per change and on the interval, with how many since; the console sees it even when nothing else does.
fn frame_unreadable(e: &serde_json::Error) {
    static UNREAD: std::sync::Mutex<(crate::logs::Relog, u64)> = std::sync::Mutex::new((crate::logs::Relog::new(), 0));
    let mut unread = UNREAD.lock().unwrap_or_else(|e| e.into_inner());
    unread.1 += 1;
    let (failing, count) = &mut *unread;
    if let Some(error) = failing.fail_due(e.to_string()) {
        let count = *count;
        crate::logs::while_delivering(|| {
            tracing::warn!(target: "runtime.frame_unreadable", count, error, "a message between the supervisor and the runtime could not be read, so what it carried is lost");
        });
        unread.1 = 0;
    }
}

/// Runtime logs the supervisor could not deliver before its buffer filled, said once per outage with the count.
fn logs_dropped(dropped: usize) {
    static DROPPING: std::sync::Mutex<(crate::logs::Relog, u64)> =
        std::sync::Mutex::new((crate::logs::Relog::new(), 0));
    let mut dropping = DROPPING.lock().unwrap_or_else(|e| e.into_inner());
    dropping.1 += dropped as u64;
    if dropping.0.fail_due("dropping".to_string()).is_some() {
        let dropped = dropping.1;
        crate::logs::while_delivering(|| {
            tracing::warn!(target: "worker.runtime_logs_dropped", dropped, "the runtime's logs outgrew what this worker can hold for the leader, so the oldest were dropped");
        });
        dropping.1 = 0;
    }
}

// -- the child ---------------------------------------------------------------

/// The runtime process. Reads frames on stdin, writes them on stdout, and never returns: the supervisor decides
/// when this ends, either by asking or by ending the process.
pub fn run_runtime_child() -> ! {
    crate::io::receiver::adopt_inherited();
    let mut incoming = frames::<ToRuntime>(std::io::stdin());

    let Some(ToRuntime::Start(spec)) = incoming.next() else {
        // Nothing to run and nobody to tell: the supervisor is gone, or said something this build does not know.
        eprintln!("smeltr: the supervisor sent no start message this build can read, so the runtime ends");
        std::process::exit(1);
    };
    let incarnation = spec.incarnation;
    crate::logs::set_worker(&spec.worker_id);

    // Before anything is opened. A supervisor confirms a runtime dead before starting its replacement, but a
    // supervisor that died itself confirmed nothing: its orphan drains on alone, and a service manager can have a
    // replacement supervisor running well inside that window. This is what stops that replacement's first runtime
    // reading the same sources beside it -- refused here rather than prevented there, because the supervisor
    // cannot see a process it never started.
    let lock_path = spec.state_dir.join(RUNTIME_LOCK);
    let _running = match crate::leader::lock::hold(&lock_path) {
        Ok(Some(held)) => held,
        Ok(None) => {
            let _ = send(
                &mut std::io::stdout().lock(),
                &FromRuntime::Failed {
                    incarnation,
                    reason: format!(
                        "a runtime this worker started earlier is still running: it holds {}. It is ending, and \
                         this target is tried again until it has.",
                        lock_path.display()
                    ),
                },
            );
            std::process::exit(1);
        }
        Err(e) => {
            let _ = send(
                &mut std::io::stdout().lock(),
                &FromRuntime::Failed {
                    incarnation,
                    reason: format!("{}: {e}", lock_path.display()),
                },
            );
            std::process::exit(1);
        }
    };

    let mut runtime = Runtime::default();
    runtime.secrets.take(spec.secrets.clone());
    let Runtime {
        captures,
        registry,
        held,
        ..
    } = &runtime;

    // Started before the deployment it serves, not after: a supervisor that dies while startup is stalled shows
    // up only as end of file on the pipe this thread reads, and nothing else arms the deadline that ends an
    // orphan. A runtime that could not be contained until it had finished starting is one that a startup which
    // never finishes leaves running forever.
    let stopping = Arc::new(AtomicBool::new(false));
    {
        let (stopping, runtime) = (stopping.clone(), runtime.clone());
        std::thread::spawn(move || {
            for frame in incoming {
                if !obey(frame, &runtime) {
                    stopping.store(true, Ordering::Relaxed);
                    return;
                }
            }
            // End of file on the command pipe is the supervisor gone. A runtime with nobody to report to is a
            // consumer nobody can stop, so it stops itself -- and bounds its own stop, because the deadline that
            // would otherwise end it belonged to the process that has just disappeared.
            stopping.store(true, Ordering::Relaxed);
            tracing::warn!(target: "runtime.orphaned", budget_secs = ORPHAN_DRAIN_BUDGET.as_secs(), "the supervisor is gone, so the runtime stops itself");
            let armed = std::thread::Builder::new().spawn(|| {
                std::thread::sleep(ORPHAN_DRAIN_BUDGET);
                // The cooperative stop joins threads that may never return, which is the hang this boundary
                // exists for. Orphaned, there is no supervisor left to kill the tree, so it ends itself.
                eprintln!(
                    "smeltr: the supervisor is gone and the runtime did not finish stopping in time, so it ends now"
                );
                std::process::exit(1);
            });
            if armed.is_err() {
                // No thread to arm the deadline that would otherwise end a stop that hangs: an orphan whose
                // drain never finishes would hold `runtime.lock` forever with nothing left to end it, which is
                // worse than ending now, ahead of the budget, while the stop it might have let finish is lost.
                std::process::exit(1);
            }
        });
    }

    let out = std::io::stdout();
    let deployment = match super::start_deployment(&spec, &runtime) {
        Ok(d) => d,
        Err(reason) => {
            let _ = send(&mut out.lock(), &FromRuntime::Failed { incarnation, reason });
            std::process::exit(1);
        }
    };
    // Before Ready: the supervisor hands this runtime captures once it reads Ready, and a capture of a source this
    // deployment reads must find it deployed and be tapped, not opened cold beside it.
    let facts = deployment.facts();
    captures.set_deployed(&facts.subjects);
    let _ = send(&mut out.lock(), &FromRuntime::Ready { incarnation });

    // After `start_deployment`, so a `smeltr logs` source has a ring to read this into: one exists only once a
    // deployed logs source is running, and a supervisor's own offer, recorded separately, cannot stand in for
    // this process's -- it is this one that opens the deployment's connections.
    let (suites, groups) = crate::crypto::offered();
    tracing::debug!(
        target: "runtime.tls_offered",
        suites = suites.join(", "),
        groups = groups.join(", "),
        "this runtime offers these TLS algorithms"
    );

    while !stopping.load(Ordering::Relaxed) && !SIGNALED.load(Ordering::Relaxed) {
        deployment.relog_memory_warning();
        deployment.note_pools();
        let mut locked = out.lock();
        let pending = collect_report(&facts, registry, held);
        let owed = Pending {
            report: RuntimeReport::default(),
            peeked: pending.peeked,
            logs: pending.logs,
        };
        let streams = pending.report.gathered.streams.clone();
        match send_report(&mut locked, incarnation, pending.report, held) {
            Ok(()) => owed.committed(registry),
            // A window nothing can carry is dropped rather than kept: the same collection would build the same
            // frame on every interval, and holding it would stop this runtime ever reporting again. Metrics are
            // where this system trades durability for simplicity, and losing a window of them is that trade;
            // never reporting is not.
            // Progress is not part of that trade: it goes back for the next report, which a cap keeps it inside.
            Err(Refused::TooLarge { bytes, limit }) => {
                owed.committed(registry);
                tracing::warn!(
                    target: "runtime.report_too_large",
                    bytes,
                    limit,
                    "a report was past what one frame may carry, so this window of counts and logs is dropped"
                );
            }
            // The pipe, not the frame. Offered again as it is.
            Err(Refused::Write(e)) => {
                if let Some(streams) = streams {
                    facts.streams.give_back(streams);
                }
                tracing::warn!(
                    target: "runtime.report_not_written",
                    error = %e,
                    "a report did not reach the supervisor, so its counts and logs are offered again"
                )
            }
        }
        for report in captures.finished(&spec.worker_id) {
            let session = report.session.clone();
            if send(
                &mut locked,
                &FromRuntime::Captured {
                    incarnation,
                    report: Box::new(report),
                },
            )
            .is_ok()
            {
                captures.sent(&session);
            }
        }
        // A session still filling, offered as it grows. Best effort, on the same pipe and the same interval as
        // everything else here: a frame this write loses is not retried, since the next tick's larger growth --
        // or the eventual final report, which always carries everything -- covers it. Nothing a progress report
        // ends by not reaching the supervisor is unrecoverable.
        for report in captures.progress(&spec.worker_id) {
            let _ = send(
                &mut locked,
                &FromRuntime::Captured {
                    incarnation,
                    report: Box::new(report),
                },
            );
        }
        drop(locked);
        std::thread::sleep(REPORT_INTERVAL);
    }

    if SIGNALED.load(Ordering::Relaxed) {
        tracing::info!(
            target: "runtime.stopping_on_signal",
            "this runtime was signaled to stop, so it is draining"
        );
    }
    super::stop_deployment(deployment);
    // A capture still filling ends here, with what it has: a leader that invited it again would get a fresh
    // session in the replacement and never the records this one already held.
    captures.end_all("the runtime serving this capture stopped");
    let mut locked = out.lock();
    for report in captures.finished(&spec.worker_id) {
        let _ = send(
            &mut locked,
            &FromRuntime::Captured {
                incarnation,
                report: Box::new(report),
            },
        );
    }
    // One last report, so what the drain committed is not lost with the process that committed it, and as many
    // more as the progress it yielded needs.
    // A report that does not go hands its progress back, so a last report too large for its logs leaves the
    // progress to the reports that carry nothing else.
    let pending = collect_report(&facts, registry, held);
    let _ = send_report(&mut locked, incarnation, pending.report, held);
    loop {
        let progress = held.take_progress(LEDGER_PER_REPORT, PROGRESS_BYTES_PER_REPORT);
        if progress.is_empty() {
            break;
        }
        let report = RuntimeReport {
            state: held_state(held),
            gathered: Gathered {
                progress,
                ..Default::default()
            },
        };
        // A report that does not go keeps its progress, though a pipe that refused a frame of this size will take
        // nothing more: the supervisor is gone.
        if send_report(&mut locked, incarnation, report, held).is_err() {
            break;
        }
    }
    let _ = send(&mut locked, &FromRuntime::Stopped { incarnation });
    drop(locked);
    crate::logs::stop();
    std::process::exit(0);
}

/// Act on one frame from the supervisor. `false` is a stop: the caller stops reading and drains.
fn obey(frame: ToRuntime, runtime: &Runtime) -> bool {
    let Runtime {
        captures,
        secrets,
        registry,
        held,
    } = runtime;
    match frame {
        ToRuntime::Stop => {
            tracing::info!(target: "runtime.stopping", "the supervisor asked the runtime to stop");
            return false;
        }
        ToRuntime::Capture(invitation) => super::serve_capture(captures, *invitation, registry, secrets),
        ToRuntime::Listed { sessions } => captures.drop_unlisted(&sessions.into_iter().collect()),
        ToRuntime::Mirror { samples, logs } => {
            registry.mirror(samples.into_iter().filter_map(MetricSampleDto::into_sample));
            let offered = logs.len();
            let events: Vec<_> = logs.into_iter().filter_map(LogDto::into_event).collect();
            if events.len() < offered {
                tracing::debug!(target: "runtime.mirror_skipped", count = offered - events.len(), "mirrored logs of a severity this build does not know were skipped");
            }
            crate::logs::mirror(events);
        }
        ToRuntime::Holds { seq, holds } => held.apply(seq, holds),
        ToRuntime::Backlogged { full } => held.set_upstream_backlogged(full),
        ToRuntime::Start(_) => {}
    }
    true
}

/// A report and what it will cost to have sent it: the counts and the events it carries, still owed until the
/// frame is on the pipe.
struct Pending {
    report: RuntimeReport,
    peeked: Vec<crate::metrics::MetricSample>,
    logs: usize,
}

impl Pending {
    /// The window is the supervisor's now, so this runtime stops offering it.
    ///
    /// Committed here rather than on the leader's acknowledgment: the supervisor is one more hop and a killed
    /// runtime cannot be asked again, so a window is owed to the supervisor instead of to the leader. Called only
    /// where the frame was written -- a frame refused for its size, or lost to a closed pipe, is a window nothing
    /// would ever offer again.
    fn committed(self, registry: &Arc<crate::metrics::Metrics>) {
        registry.commit_delta(crate::metrics::SUBSCRIBER_HEARTBEAT, &self.peeked);
        crate::logs::delivered(self.logs);
    }
}

/// What this runtime is doing, as of now. Nothing is committed here; see [`Pending`]. The progress it carries is
/// taken from `held`, and the caller hands it back if the report does not go.
fn collect_report(
    facts: &super::DeployedFacts,
    registry: &Arc<crate::metrics::Metrics>,
    held: &super::held::Held,
) -> Pending {
    super::record_log_counts(registry, "runtime");
    super::record_memory_gauge(registry, "runtime");
    let peeked = registry.peek_delta(crate::metrics::SUBSCRIBER_HEARTBEAT);
    let metrics: Vec<MetricSampleDto> = peeked.iter().cloned().map(MetricSampleDto::from).collect();
    let boot_id = crate::logs::boot_id();
    let logs = crate::logs::pending_map(crate::heartbeat::LOGS_PER_HEARTBEAT, |event: &crate::logs::Event| {
        LogDto {
            boot_id: Some(boot_id),
            ..LogDto::from(event)
        }
    });
    let owed_logs = logs.len();

    let report = RuntimeReport {
        state: RuntimeState {
            readers: facts.readers.iter().map(|r| r.lock().unwrap().clone()).collect(),
            pool_thread_ids: facts.pool_thread_ids.clone(),
            egress_ids: facts.egress_ids.clone(),
            egress_refusals: facts
                .egress_ids
                .iter()
                .map(|id| (*id, facts.refusals.heartbeat_facts(*id)))
                .collect(),
            subjects: (*facts.subjects).clone(),
            ..held_state(held)
        },
        gathered: Gathered {
            metrics,
            logs,
            progress: held.take_progress(LEDGER_PER_REPORT, PROGRESS_BYTES_PER_REPORT),
            streams: facts.streams.drain(),
        },
    };
    Pending {
        report,
        peeked,
        logs: owed_logs,
    }
}

/// What `held` says of the single-worker sources this deployment runs.
fn held_state(held: &super::held::Held) -> RuntimeState {
    RuntimeState {
        single_worker: held.keys(),
        // Read before what it holds: a frame applied between the two reads leaves this behind what `holding`
        // shows, never ahead of it, so no grant is judged failed before it was tried.
        holds_applied: held.applied(),
        holding: held.holding(),
        ..Default::default()
    }
}

/// Write `report` to the supervisor. A report that does not go hands its progress back to `held`, for the next.
fn send_report(
    out: &mut impl Write,
    incarnation: u64,
    report: RuntimeReport,
    held: &super::held::Held,
) -> Result<(), Refused> {
    let progress = report.gathered.progress.clone();
    let sent = write_frame(
        out,
        &FromRuntime::Report {
            incarnation,
            report: Box::new(report),
        },
    );
    if sent.is_err() {
        held.untake_progress(progress);
    }
    sent
}

// -- the supervisor's side ---------------------------------------------------

/// A runtime the supervisor owns: one process, one incarnation, killable as a tree.
pub struct RuntimeHandle {
    process: crate::process::Contained,
    control: Control,
    /// The thread decoding this runtime's frames, joined once the tree is dead. A runtime writes its final report
    /// and its last captures just before exiting, so the process ending is not the reader having finished with
    /// what it wrote: without this join the supervisor could replace the inbox while those frames were still
    /// being decoded into it.
    reader: Option<std::thread::JoinHandle<()>>,
    pub incarnation: u64,
    pub commit: String,
}

/// What a supervisor learns from a running child, kept where its heartbeat can read it without waiting.
///
/// Reports accumulate rather than replace one another. A runtime reports several times between heartbeats, and
/// each report carries the counts and the logs since its own last one, so keeping only the newest would drop
/// every window in between -- including all of them while the deployer is busy stopping something.
#[derive(Default)]
pub struct RuntimeInbox {
    pub report: std::sync::Mutex<Option<RuntimeReport>>,
    pub captured: std::sync::Mutex<Vec<CaptureReport>>,
    pub ready: AtomicBool,
    pub failed: std::sync::Mutex<Option<String>>,
    pub exited: AtomicBool,
}

/// This executable, for spawning a runtime from.
///
/// Resolved once and kept. An upgrade that replaces the file in place leaves `current_exe` answering the original
/// path with ` (deleted)` appended, which is not a path anything can spawn -- so a supervisor that never resolved
/// it before the replacement would refuse every deployment for the rest of its life, having already stopped what
/// was running. Holding the first answer covers a supervisor that started before the replacement; dropping the
/// suffix covers one that started after, since the replacement is at the name without it.
fn runtime_binary() -> std::io::Result<std::path::PathBuf> {
    static RESOLVED: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    if let Some(held) = RESOLVED.get() {
        return Ok(held.clone());
    }
    let path = without_deleted_suffix(std::env::current_exe()?);
    Ok(RESOLVED.get_or_init(|| path).clone())
}

/// `path` with ` (deleted)` dropped from its file name, when that names a file that is there. Anything else is
/// answered unchanged: a binary genuinely called that is not this one's business to rename.
fn without_deleted_suffix(path: std::path::PathBuf) -> std::path::PathBuf {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return path;
    };
    let Some(real) = name.strip_suffix(" (deleted)") else {
        return path;
    };
    let candidate = path.with_file_name(real);
    if candidate.exists() { candidate } else { path }
}

/// mimalloc's delay before it hands freed pages back, in milliseconds.
#[cfg(windows)]
const PURGE_DELAY_ENV: &str = "MIMALLOC_PURGE_DELAY";

impl RuntimeHandle {
    /// Starts a runtime and hands it its target, and the held sockets of the receivers it serves (`listens`). Returns as soon as the
    /// process exists: readiness is a frame, not a spawn.
    pub fn start(
        spec: StartSpec,
        inbox: &Arc<RuntimeInbox>,
        sockets: &crate::io::receiver::Sockets,
        listens: &[crate::plan::topology::Listen],
    ) -> std::io::Result<RuntimeHandle> {
        let mut cmd = std::process::Command::new(runtime_binary()?);
        cmd.env(RUNTIME_ENV, "1");
        // The supervisor serves the metrics endpoint, so its password is the supervisor's alone to hold.
        cmd.env_remove(crate::startup_config::WORKER_METRICS_ENDPOINT_PASSWORD_ENV);
        // mimalloc hands freed pages back only after a delay, and a runtime under load frees faster than the delay
        // passes, so what it waits to hand back is memory nothing counts. Measured at about a sixth of a loaded
        // runtime's peak. An operator's own setting stands.
        #[cfg(windows)]
        if std::env::var_os(PURGE_DELAY_ENV).is_none() {
            cmd.env(PURGE_DELAY_ENV, "0");
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let (named, fds) = sockets.handed(listens);
            if !fds.is_empty() {
                cmd.env(crate::io::receiver::SOCKETS_ENV, named);
                // SAFETY: runs in the child between fork and exec, and calls only `fcntl`, which is
                // async-signal-safe. Clearing close-on-exec there hands these sockets to this child alone, not to
                // every process this one starts.
                unsafe {
                    cmd.pre_exec(move || {
                        for &fd in &fds {
                            if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                                return Err(std::io::Error::last_os_error());
                            }
                        }
                        Ok(())
                    });
                }
            }
        }
        #[cfg(not(unix))]
        let _ = (sockets, listens);
        RuntimeHandle::start_as(cmd, spec, inbox)
    }

    /// The same, against a command the caller chose. What a test uses to stand a real process where a runtime
    /// would be -- one that answers nothing, or holds a pipe open, or will not die politely.
    pub(crate) fn start_as(
        mut cmd: std::process::Command,
        spec: StartSpec,
        inbox: &Arc<RuntimeInbox>,
    ) -> std::io::Result<RuntimeHandle> {
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut process = crate::process::Contained::spawn(&mut cmd)?;
        // A failure past this point drops `process`, and dropping a `Contained` that was not confirmed dead kills
        // its tree: what was started is ended before the failure is reported, on every such path.
        let (control, reader, incarnation, commit) = RuntimeHandle::attach(&mut process, spec, inbox)?;
        Ok(RuntimeHandle {
            process,
            control,
            reader: Some(reader),
            incarnation,
            commit,
        })
    }

    /// Hand a spawned process its target and start reading what it says.
    fn attach(
        process: &mut crate::process::Contained,
        spec: StartSpec,
        inbox: &Arc<RuntimeInbox>,
    ) -> std::io::Result<(Control, std::thread::JoinHandle<()>, u64, String)> {
        let stdin = process.take_stdin().ok_or_else(|| std::io::Error::other("no stdin"))?;
        let stdout = process
            .take_stdout()
            .ok_or_else(|| std::io::Error::other("no stdout"))?;
        let stderr = process.take_stderr();

        let (incarnation, commit) = (spec.incarnation, spec.commit.clone());
        // The first frame, over the same bounded path as every other: nothing has a deadline of its own yet here,
        // the startup budget begins once this returns, and the supervisor has already stopped what was running.
        let control = Control::over(stdin)?;
        control.send_within(ToRuntime::Start(Box::new(spec)), CONTROL_FRAME_BUDGET)?;

        // Drained on a thread of its own, so a full pipe can never be what stops this child being controlled.
        let reader = {
            let inbox = inbox.clone();
            std::thread::Builder::new()
                .name("smeltr-runtime-reader".to_string())
                .spawn(move || read_from_runtime(stdout, &inbox, incarnation))?
        };
        if let Some(stderr) = stderr {
            std::thread::Builder::new()
                .name("smeltr-runtime-stderr".to_string())
                .spawn(move || {
                    for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                        // Already formatted by the child's own console layer, so it is passed through rather than
                        // parsed: what a runtime says about itself reads the same wherever it ran.
                        eprintln!("{line}");
                    }
                })?;
        }
        Ok((control, reader, incarnation, commit))
    }

    /// Ask this runtime to serve a capture.
    pub fn capture(&mut self, invitation: CaptureInvitation) -> std::io::Result<()> {
        self.control
            .send_within(ToRuntime::Capture(Box::new(invitation)), CONTROL_FRAME_BUDGET)
    }

    /// Tell this runtime every capture session the leader still lists.
    pub fn listed(&mut self, sessions: Vec<String>) -> std::io::Result<()> {
        self.control
            .send_within(ToRuntime::Listed { sessions }, CONTROL_FRAME_BUDGET)
    }

    /// Tell this runtime every single-worker source the leader holds for this worker, in frame `seq`.
    pub fn holds(&mut self, seq: u64, holds: Vec<super::held::Holding>) -> std::io::Result<()> {
        self.control
            .send_within(ToRuntime::Holds { seq, holds }, CONTROL_FRAME_BUDGET)
    }

    /// Tell this runtime whether the progress the supervisor holds for the leader is past its bound.
    pub fn backlogged(&mut self, full: bool) -> std::io::Result<()> {
        self.control
            .send_within(ToRuntime::Backlogged { full }, CONTROL_FRAME_BUDGET)
    }

    /// Hand this runtime what the supervisor recorded and said, to mirror.
    pub fn mirror(&mut self, samples: Vec<MetricSampleDto>, logs: Vec<LogDto>) -> std::io::Result<()> {
        self.control
            .send_within(ToRuntime::Mirror { samples, logs }, CONTROL_FRAME_BUDGET)
    }

    /// Stop, and do not come back without it. Cooperative first; at the hard deadline the process tree is killed
    /// and reaped, because a replacement may not start while an old reader might still be consuming.
    ///
    /// `Err` is a termination that could not be confirmed: the caller stays blocked from activating anything and
    /// says so, rather than claiming a replacement over a runtime that may still be running.
    pub fn stop(&mut self, graceful: Duration, hard: Duration, inbox: &RuntimeInbox) -> std::io::Result<()> {
        if let Err(e) = self.control.send_within(ToRuntime::Stop, CONTROL_FRAME_BUDGET) {
            tracing::debug!(target: "worker.stop_not_sent", error = %e, "the runtime could not be asked to stop, so it is ended at the deadline");
        }
        let deadline = Instant::now() + graceful;
        while Instant::now() < deadline {
            // Asked, not reaped: the kill below is aimed at the id this child holds only while it is unreaped.
            if inbox.exited.load(Ordering::Relaxed) || self.process.ended()? {
                // The runtime is gone, which says nothing about what it spawned. Killing the group costs nothing
                // when it is already empty and is the difference between a confirmed ending and a descendant
                // still holding a source while a replacement starts.
                return self.kill_and_confirm(hard);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        tracing::warn!(
            target: "worker.runtime_drain_timed_out",
            subject = %self.commit,
            graceful = ?graceful,
            "a runtime did not drain in time, so its process tree is being killed"
        );
        self.kill_and_confirm(hard)
    }

    /// Kill the tree and confirm within `hard` that it is gone.
    ///
    /// The wait is polled rather than blocked on, because the thing being waited for is a process that may not be
    /// answering. A runtime says `Stopped` before it exits and its pipe closes before the kernel lets go, so
    /// arriving here is not the same as the process having ended; one stuck in an uninterruptible call takes the
    /// signal only when it leaves, and a blocking wait would sit through the budget this exists to enforce.
    ///
    /// Confirmation rests on the pipe, with the limits `process`'s module doc states: a descendant that closed or
    /// redirected its own copy of it before wedging is no longer a writer this waits on.
    fn kill_and_confirm(&mut self, hard: Duration) -> std::io::Result<()> {
        self.process.kill_tree()?;
        let deadline = Instant::now() + hard;
        // The reader first: it finishes when the pipe reaches end-of-file, which is every writer in the tree
        // gone, descendants included. Only then is the child reaped, since reaping releases the id the group is
        // named by and there must be nothing left to signal by then.
        while !self.reader_finished() {
            if Instant::now() >= deadline {
                return Err(unconfirmed());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        match self
            .process
            .reap_within(deadline.saturating_duration_since(Instant::now()))?
        {
            Some(_) => {
                self.join_reader();
                Ok(())
            }
            None => Err(unconfirmed()),
        }
    }

    /// Whether the process has gone on its own -- and if it has, whether anything it spawned is still here.
    ///
    /// A runtime that ended is not a tree that has: a descendant still holding a source would run beside whatever
    /// replaces it, which is the overlap this boundary exists to rule out. So an exit is followed by the kill
    /// anything else here gets. The child is reaped by then, and the group is named by its id; that id stays
    /// pinned for as long as any member of the group lives, which is exactly when there is something to kill.
    pub fn exited(&mut self) -> Exit {
        // Asked without reaping, so the kill that follows names an id this child still holds.
        if !matches!(self.process.ended(), Ok(true)) {
            return Exit::Running;
        }
        // Gone is the tree, not the process. A descendant holds its own copy of the output pipe, so a reader
        // still running is a descendant still here -- and starting a replacement beside it is the overlap this
        // boundary exists to rule out. Answering unconfirmed keeps the handle with the deployer, which asks
        // again next round and, at its own deadline, ends it and reports an ending it cannot confirm. The child
        // stays unreaped meanwhile, which is what keeps its id ours to signal.
        if self.process.kill_tree().is_err() || !self.reader_finished() {
            return Exit::Unconfirmed;
        }
        let Ok(Some(status)) = self.process.reap_within(Duration::ZERO) else {
            return Exit::Unconfirmed;
        };
        self.join_reader();
        Exit::Confirmed(status.to_string())
    }
}

/// What [`RuntimeHandle::exited`] found. A caller that only asks "is it gone" cannot tell a process still running
/// from one whose tree it dare not consider dead -- and only the second of those is [`Unconfirmed`](Exit::Unconfirmed),
/// the state the Control plane's process boundary (design doc) has a worker report rather than recover from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    /// Neither observed nor asked for: this handle is still what it was deployed as.
    Running,
    /// The process ended, but its tree did not confirm: the group kill failed, or something is still holding its
    /// pipe open. A descendant may still be consuming, so nothing may take this handle's place.
    Unconfirmed,
    /// The process ended, its tree is gone, and it has been reaped: how it ended, as the system says it.
    Confirmed(String),
}

/// The one error every unconfirmed ending here reports. A caller that sees it keeps the handle: a tree it cannot
/// confirm dead is one it must not replace.
fn unconfirmed() -> std::io::Error {
    std::io::Error::other("a runtime process would not end, so its exit could not be confirmed")
}

impl RuntimeHandle {
    /// Whether the frame reader has finished, which is the pipe having reached end-of-file. It is asked rather
    /// than waited on: a descendant the kill could not reach holds its own copy of that pipe open, and a join
    /// there would outlast any deadline. Answering `false` at a deadline is an ending this worker cannot confirm,
    /// which is a state it reports rather than one it waits out.
    fn reader_finished(&self) -> bool {
        self.reader.as_ref().is_none_or(|reader| reader.is_finished())
    }

    /// Take what the reader collected. Only called where [`reader_finished`](Self::reader_finished) already said
    /// yes, so it returns at once.
    fn join_reader(&mut self) {
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn read_from_runtime(stdout: std::process::ChildStdout, inbox: &Arc<RuntimeInbox>, incarnation: u64) {
    read_from_runtime_frames(stdout, inbox, incarnation);
    // The pipe closed, which is the process gone however it went.
    inbox.exited.store(true, Ordering::Relaxed);
}

/// The frames themselves, separated from the pipe they usually arrive on so a test can hand them over directly.
fn read_from_runtime_frames(from: impl std::io::Read, inbox: &Arc<RuntimeInbox>, incarnation: u64) {
    for frame in frames::<FromRuntime>(from) {
        // A frame from an incarnation this supervisor has replaced says nothing about the one running now.
        let from = match &frame {
            FromRuntime::Ready { incarnation } => *incarnation,
            FromRuntime::Failed { incarnation, .. } => *incarnation,
            FromRuntime::Report { incarnation, .. } => *incarnation,
            FromRuntime::Captured { incarnation, .. } => *incarnation,
            FromRuntime::Stopped { incarnation } => *incarnation,
        };
        if from != incarnation {
            continue;
        }
        match frame {
            FromRuntime::Ready { .. } => inbox.ready.store(true, Ordering::Relaxed),
            FromRuntime::Failed { reason, .. } => *inbox.failed.lock().unwrap() = Some(reason),
            FromRuntime::Report { report, .. } => {
                let mut held = inbox.report.lock().unwrap();
                match held.take() {
                    Some(previous) => *held = Some(previous.followed_by(*report)),
                    None => *held = Some(*report),
                }
            }
            FromRuntime::Captured { report, .. } => inbox.captured.lock().unwrap().push(*report),
            // Said here, where it can still reach the leader: the runtime sends nothing after this.
            FromRuntime::Stopped { .. } => {
                tracing::info!(target: "runtime.stopped", incarnation, "the runtime stopped");
                inbox.exited.store(true, Ordering::Relaxed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_round_trips_through_the_wire() {
        let mut buffer = Vec::new();
        send(&mut buffer, &FromRuntime::Ready { incarnation: 7 }).expect("a frame is written");
        let read: Vec<FromRuntime> = frames(buffer.as_slice()).collect();
        assert!(matches!(read.as_slice(), [FromRuntime::Ready { incarnation: 7 }]));
    }

    /// A line that is not a frame this build knows is skipped rather than ending the stream: a child and a
    /// supervisor of different versions still talk about what they share.
    #[test]
    fn an_unreadable_line_does_not_end_the_stream() {
        let wire = b"{\"kind\":\"nonsense\"}\nnot json at all\n{\"kind\":\"stopped\",\"incarnation\":3}\n";
        let read: Vec<FromRuntime> = frames(wire.as_slice()).collect();
        assert!(matches!(read.as_slice(), [FromRuntime::Stopped { incarnation: 3 }]));
    }

    fn spec() -> StartSpec {
        StartSpec {
            state_dir: std::path::PathBuf::from("/nonexistent"),
            repo_dir: std::path::PathBuf::from("/nonexistent"),
            commit: "c0ffee".to_string(),
            group_id: "g".to_string(),
            thread_limit: 1,
            memory: None,
            worker_id: "w1".to_string(),
            secrets: vec![],
            incarnation: 1,
        }
    }

    /// The central mechanism, against a process that genuinely will not cooperate rather than a mock that returns
    /// a timeout: it reads nothing, answers nothing, and would outlive any deadline. The cooperative stop expires,
    /// the process tree is killed, and the exit is confirmed.
    #[test]
    fn a_runtime_that_answers_nothing_is_killed_and_its_exit_confirmed() {
        let inbox = Arc::new(RuntimeInbox::default());
        let cmd = crate::process::shell("while true; do sleep 1; done", "ping -n 31 127.0.0.1 >nul");
        let mut handle = RuntimeHandle::start_as(cmd, spec(), &inbox).expect("a runtime stands up");

        let started = Instant::now();
        handle
            .stop(Duration::from_millis(200), Duration::from_secs(10), &inbox)
            .expect("a process that will not drain is still ended");
        assert!(
            started.elapsed() < Duration::from_secs(9),
            "the kill did not follow the graceful budget"
        );
        assert!(
            matches!(handle.exited(), Exit::Confirmed(_)),
            "a stopped runtime was not confirmed dead"
        );
    }

    /// A descendant that leaves the group survives the kill this worker can send, so the process it belonged to
    /// is not this worker's to confirm dead. `exited` says so rather than reporting a handle it cannot tell from
    /// one still running -- the ambiguity `RuntimeHandle::exited` exists to remove.
    #[test]
    #[cfg(unix)]
    fn a_descendant_that_leaves_the_group_is_exited_but_unconfirmed() {
        let inbox = Arc::new(RuntimeInbox::default());
        let mut cmd = std::process::Command::new("sh");
        // `setsid` puts the sleep in a session -- and so a process group -- of its own; the group kill this
        // worker sends is a signal to *its* group, and never reaches one.
        cmd.arg("-c").arg("read -r _; setsid sleep 30 & exit 0");
        let mut handle = RuntimeHandle::start_as(cmd, spec(), &inbox).expect("a runtime stands up");

        let unconfirmed = (0..500).any(|_| {
            std::thread::sleep(Duration::from_millis(10));
            matches!(handle.exited(), Exit::Unconfirmed)
        });
        assert!(
            unconfirmed,
            "a descendant that escaped the group was not reported as unconfirmed"
        );
        // Asked again, it still is: the descendant that stranded it is thirty seconds from going anywhere.
        assert!(
            matches!(handle.exited(), Exit::Unconfirmed),
            "an unconfirmed exit resolved into something else on its own"
        );
    }

    /// Saying it has stopped is not having stopped: the child writes `Stopped` and closes its pipe before the
    /// kernel is finished with it. The exit is confirmed against the process rather than taken from the flag, and
    /// the confirmation is bounded, so a process the OS will not let go of expires the budget instead of the
    /// supervisor.
    #[test]
    #[cfg(unix)]
    fn a_runtime_that_reports_its_exit_early_is_still_confirmed_within_the_budget() {
        let inbox = Arc::new(RuntimeInbox::default());
        let mut cmd = std::process::Command::new("sh");
        cmd.arg("-c").arg("while true; do sleep 1; done");
        let mut handle = RuntimeHandle::start_as(cmd, spec(), &inbox).expect("a runtime stands up");
        let pid = handle.process.id() as i32;
        // What a `Stopped` frame does, arriving while the process is still very much here.
        inbox.exited.store(true, Ordering::Relaxed);

        let started = Instant::now();
        handle
            .stop(Duration::from_secs(10), Duration::from_secs(5), &inbox)
            .expect("a runtime that reported its exit is confirmed gone");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "confirming a reported exit took longer than the hard budget"
        );
        // SAFETY: signal 0 delivers nothing and only asks whether the id is still addressable.
        let gone = (0..200).any(|_| {
            let alive = unsafe { libc::kill(pid, 0) } == 0;
            if alive {
                std::thread::sleep(Duration::from_millis(10));
            }
            !alive
        });
        assert!(gone, "a runtime that only said it had stopped was left running");
    }

    /// A runtime writes its last frames and exits at once, so the process ending says nothing about the reader
    /// having finished with them. They are still owed: the child marked those captures sent and committed those
    /// counts, and the supervisor replaces the inbox as soon as the stop returns.
    #[test]
    fn frames_written_just_before_an_exit_are_in_the_inbox_when_the_stop_returns() {
        const WRITTEN: usize = 300;
        let inbox = Arc::new(RuntimeInbox::default());
        let frame = serde_json::to_string(&FromRuntime::Captured {
            incarnation: 1,
            report: Box::new(crate::heartbeat::CaptureReport {
                session: "s1".to_string(),
                worker_id: "w1".to_string(),
                batches: vec![],
                finished: Some(crate::heartbeat::CaptureEnd::Bytes),
                sql: None,
                avro: Vec::new(),
            }),
        })
        .expect("a frame encodes");
        // Single quotes around JSON for `sh`, which contains none of its own. `cmd` echoes it bare, and it holds
        // none of `& | < > ^ %`.
        // Enough frames that the reader has real work outstanding when the process ends. The window itself is
        // too narrow to lose a race on demand, so what is asserted below is the join rather than a timing.
        // It reads its start frame first, as a runtime does: a fake that exits without reading has the supervisor
        // writing to a pipe nobody holds, and that is a failed start rather than the ending this is about.
        let cmd = crate::process::shell(
            &format!(
                "read -r _; i=0; while [ $i -lt {WRITTEN} ]; do printf '%s\\n' '{frame}'; i=$((i+1)); done; exit 0"
            ),
            &format!("set /p _= & for /l %i in (1,1,{WRITTEN}) do @echo {frame}"),
        );
        let mut handle = RuntimeHandle::start_as(cmd, spec(), &inbox).expect("a runtime stands up");

        handle
            .stop(Duration::from_secs(10), Duration::from_secs(10), &inbox)
            .expect("a runtime that exited on its own is confirmed gone");
        assert!(
            handle.reader.is_none(),
            "the stop returned with the frame reader still running, so the inbox may still be filling"
        );
        assert_eq!(
            inbox.captured.lock().unwrap().len(),
            WRITTEN,
            "frames the runtime wrote before exiting did not reach the inbox"
        );
    }

    /// A runtime that exits but leaves something of its own behind still ends: the pipe a descendant holds open
    /// is what `Command::output` would wait on forever, and the tree kill closes it.
    #[test]
    fn a_runtime_that_leaves_a_descendant_holding_its_pipe_is_still_ended() {
        let inbox = Arc::new(RuntimeInbox::default());
        // Reads its start frame first, as a runtime does; see the test above.
        let cmd = crate::process::shell(
            "read -r _; sleep 30 & exit 0",
            "set /p _= & start /b ping -n 31 127.0.0.1 & exit 0",
        );
        let mut handle = RuntimeHandle::start_as(cmd, spec(), &inbox).expect("a runtime stands up");

        let started = Instant::now();
        handle
            .stop(Duration::from_millis(200), Duration::from_secs(10), &inbox)
            .expect("a descendant holding a pipe does not prevent an ending");
        assert!(
            started.elapsed() < Duration::from_secs(9),
            "the descendant outlasted the budget"
        );
    }

    /// A runtime the supervisor has replaced says nothing about the one running now, so what it says is dropped
    /// rather than credited to its successor.
    #[test]
    fn a_frame_from_a_replaced_incarnation_is_ignored() {
        let inbox = Arc::new(RuntimeInbox::default());
        let wire = concat!(
            "{\"kind\":\"ready\",\"incarnation\":1}\n",
            "{\"kind\":\"stopped\",\"incarnation\":1}\n",
        );
        // This supervisor is on its second runtime; both frames above belong to the first.
        read_from_runtime_frames(wire.as_bytes(), &inbox, 2);
        assert!(
            !inbox.ready.load(Ordering::Relaxed),
            "a replaced runtime reported ready"
        );

        read_from_runtime_frames(wire.as_bytes(), &inbox, 1);
        assert!(
            inbox.ready.load(Ordering::Relaxed),
            "the running runtime's own frame was dropped"
        );
    }

    fn counter(metric: &str, value: u64) -> MetricSampleDto {
        MetricSampleDto {
            metric: metric.to_string(),
            labels: vec![("thread".to_string(), "reader-0".to_string())],
            value: MetricValueDto::Counter(value),
        }
    }

    /// A runtime reports several times between heartbeats, and each report carries the counts since its own last
    /// one. Keeping only the newest would drop every window in between -- so a counter is the sum of what the
    /// leader has not been told, and what the threads are doing is whatever they are doing now.
    #[test]
    fn reports_waiting_for_one_heartbeat_add_up_rather_than_replace_each_other() {
        let first = RuntimeReport {
            state: RuntimeState {
                pool_thread_ids: vec!["pool-0".to_string()],
                ..Default::default()
            },
            gathered: Gathered {
                metrics: vec![counter("smeltr_rows_in_total", 3)],
                logs: vec![],
                ..Default::default()
            },
        };
        let second = RuntimeReport {
            state: RuntimeState {
                pool_thread_ids: vec!["pool-0".to_string(), "pool-1".to_string()],
                ..Default::default()
            },
            gathered: Gathered {
                metrics: vec![counter("smeltr_rows_in_total", 4), counter("smeltr_rows_out_total", 1)],
                logs: vec![],
                ..Default::default()
            },
        };

        let merged = first.followed_by(second);
        let rows_in = merged
            .gathered
            .metrics
            .iter()
            .find(|m| m.metric == "smeltr_rows_in_total")
            .expect("the counter survives");
        assert!(
            matches!(rows_in.value, MetricValueDto::Counter(7)),
            "two windows the leader has not seen are one increase: {:?}",
            rows_in.value
        );
        assert_eq!(
            merged.gathered.metrics.len(),
            2,
            "a counter only the later report saw is kept"
        );
        assert_eq!(
            merged.state.pool_thread_ids.len(),
            2,
            "what the threads are is the newer answer, not both answers"
        );
    }

    /// A gauge is whatever it last was, never a sum: adding two readings of the same dial reports a number
    /// nothing ever measured.
    #[test]
    fn a_gauge_is_the_newer_reading_rather_than_the_sum() {
        let dial = |v: f64| MetricSampleDto {
            metric: "smeltr_queue_depth".to_string(),
            labels: vec![],
            value: MetricValueDto::Gauge(v),
        };
        let merged = RuntimeReport {
            gathered: Gathered {
                metrics: vec![dial(4.0)],
                ..Default::default()
            },
            ..Default::default()
        }
        .followed_by(RuntimeReport {
            gathered: Gathered {
                metrics: vec![dial(9.0)],
                ..Default::default()
            },
            ..Default::default()
        });
        assert!(matches!(merged.gathered.metrics[0].value, MetricValueDto::Gauge(v) if v == 9.0));
    }

    #[test]
    fn a_frame_past_the_limit_is_refused_rather_than_written() {
        let huge = "x".repeat(MAX_FRAME);
        let refused = send(
            &mut Vec::new(),
            &FromRuntime::Failed {
                incarnation: 1,
                reason: huge,
            },
        );
        assert!(refused.is_err(), "a frame past the limit was written anyway");
    }

    /// An in-place upgrade unlinks the running binary, and Linux then answers its path with a suffix no spawn
    /// accepts. The replacement is at the name without it.
    #[test]
    fn a_replaced_binary_resolves_to_the_file_that_replaced_it() {
        let dir = std::env::temp_dir().join(format!("smeltr-exe-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let real = dir.join("smeltr");
        std::fs::write(&real, b"#!/bin/sh\ntrue\n").unwrap();

        let deleted = dir.join("smeltr (deleted)");
        assert_eq!(without_deleted_suffix(deleted), real, "the replacement was not found");

        // A file that really is called this is left alone, since it is the one that exists.
        let named = dir.join("held (deleted)");
        std::fs::write(&named, b"x").unwrap();
        assert_eq!(without_deleted_suffix(named.clone()), named);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The leader may invite a capture of its whole ceiling, and what it invited has to fit the pipe home.
    #[test]
    fn a_capture_at_the_leader_s_ceiling_crosses_the_pipe() {
        let report = crate::heartbeat::CaptureReport {
            session: "s".to_string(),
            worker_id: "w".to_string(),
            batches: vec![crate::heartbeat::CaptureBatch {
                columns: vec![crate::heartbeat::CaptureColumn {
                    name: "_raw".to_string(),
                    ty: "str".to_string(),
                }],
                rows: vec![crate::heartbeat::CapturedRow {
                    values: vec![serde_json::Value::String(
                        "\"".repeat(crate::leader::capture::MAX_BYTES_CEILING),
                    )],
                    failed: vec![],
                    worker: None,
                }],
            }],
            finished: Some(crate::heartbeat::CaptureEnd::Bytes),
            sql: None,
            avro: Vec::new(),
        };
        let mut written = Vec::new();
        send(
            &mut written,
            &FromRuntime::Captured {
                incarnation: 1,
                report: Box::new(report),
            },
        )
        .expect("a capture at the ceiling fits its frame");
        assert!(
            written.len() > MAX_FRAME,
            "the capture did not need the larger frame limit"
        );
    }

    /// A start carrying more secret material than an ordinary frame allows still reaches the runtime. Refusing it
    /// would be permanent: the supervisor has already stopped what was running, and every retry builds the same
    /// frame from the same secrets.
    #[test]
    fn a_start_carrying_a_large_secret_set_is_not_refused() {
        let mut spec = spec();
        spec.secrets = (0..4)
            .map(|i| crate::heartbeat::SecretDelivery {
                name: format!("s{i}"),
                version: 1,
                value: "x".repeat(3 * 1024 * 1024),
                chain: None,
            })
            .collect();
        let mut written = Vec::new();
        send(&mut written, &ToRuntime::Start(Box::new(spec))).expect("a start with large secrets fits its frame");
        assert!(
            written.len() > MAX_FRAME,
            "the start did not need the larger frame limit"
        );
    }

    /// The listing is a struct variant, so it carries the `kind` tag every frame on this pipe is read by.
    #[test]
    fn a_listed_frame_names_its_sessions_on_the_wire() {
        let mut buffer = Vec::new();
        send(
            &mut buffer,
            &ToRuntime::Listed {
                sessions: vec!["s1".into()],
            },
        )
        .expect("a listing is written");
        assert_eq!(
            String::from_utf8(buffer.clone()).unwrap(),
            "{\"kind\":\"listed\",\"sessions\":[\"s1\"]}\n"
        );
        let read: Vec<ToRuntime> = frames(buffer.as_slice()).collect();
        match read.as_slice() {
            [ToRuntime::Listed { sessions }] => assert_eq!(sessions, &["s1".to_string()]),
            other => panic!("expected one listing, got {other:?}"),
        }
    }

    /// A listing lets go of every session still filling that it leaves out, and a stop is the one frame that ends
    /// the read.
    #[test]
    fn obeying_a_listing_lets_go_of_what_it_leaves_out() {
        let invitation = |session: &str| {
            serde_json::from_value::<CaptureInvitation>(serde_json::json!({
                "session": session,
                "group": "g",
                "source": {"id": "m", "type": "metrics", "bucketSeconds": 10},
                "maxMatches": 10,
                "maxBytes": 1000,
                "deadlineSecs": 60
            }))
            .expect("an invitation")
        };
        let runtime = Runtime::default();
        let captures = &runtime.captures;
        captures.admit(&[invitation("s1"), invitation("s2")]);

        assert!(obey(
            ToRuntime::Listed {
                sessions: vec!["s1".into()]
            },
            &runtime
        ));
        assert!(captures.holds("s1"), "a listed session was let go");
        assert!(!captures.holds("s2"), "an unlisted session still filling was kept");

        assert!(!obey(ToRuntime::Stop, &runtime), "a stop did not end the read");
    }

    fn key(source: &str) -> crate::heartbeat::SourceKey {
        crate::heartbeat::SourceKey {
            source: source.to_string(),
            place: "p".to_string(),
        }
    }

    /// A holds frame crosses the pipe whole, and the runtime grants what it names to the source it deploys under
    /// that key.
    #[test]
    fn obeying_holds_grants_the_sources_they_name() {
        let ledger = std::env::temp_dir().join(format!("smeltr-obey-holds-{}.ndjson", std::process::id()));
        std::fs::write(&ledger, "").unwrap();
        let holding = super::super::held::Holding {
            hold: crate::heartbeat::Hold {
                key: key("listing"),
                cursor: Some("c1".to_string()),
            },
            ledger_file: Some(ledger),
        };
        let mut buffer = Vec::new();
        send(
            &mut buffer,
            &ToRuntime::Holds {
                seq: 1,
                holds: vec![holding.clone()],
            },
        )
        .expect("a holds frame is written");
        let read: Vec<ToRuntime> = frames(buffer.as_slice()).collect();
        let [ToRuntime::Holds { holds, .. }] = read.as_slice() else {
            panic!("expected one holds frame, got {read:?}")
        };
        assert_eq!(holds, &vec![holding]);

        let runtime = Runtime::default();
        let opened = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut source = runtime.held.source(key("listing"), {
            let opened = opened.clone();
            Box::new(move || {
                opened.fetch_add(1, Ordering::Relaxed);
                Ok(Box::new(crate::io::fake::FakeSource::default()) as Box<dyn crate::io::Source>)
            })
        });
        assert!(obey(read.into_iter().next().unwrap(), &runtime));
        crate::io::Source::poll_batch(&mut source, 1, 1, Duration::from_millis(1)).expect("a poll");
        assert_eq!(opened.load(Ordering::Relaxed), 1, "a granted source was not opened");
    }

    /// Progress waiting for one heartbeat is every report's, in the order the runtime committed it: the leader
    /// applies each in turn.
    #[test]
    fn reports_waiting_for_one_heartbeat_keep_all_their_progress_in_order() {
        let progress = |cursor: &str| crate::heartbeat::SourceProgress {
            key: key("listing"),
            progress: crate::io::Progress {
                cursor: Some(cursor.to_string()),
                ..Default::default()
            },
        };
        let merged = RuntimeReport {
            state: RuntimeState {
                single_worker: vec![key("listing")],
                ..Default::default()
            },
            gathered: Gathered {
                progress: vec![progress("1")],
                ..Default::default()
            },
        }
        .followed_by(RuntimeReport {
            state: RuntimeState {
                single_worker: vec![key("listing"), key("other")],
                ..Default::default()
            },
            gathered: Gathered {
                progress: vec![progress("2")],
                ..Default::default()
            },
        });
        assert_eq!(merged.gathered.progress, vec![progress("1"), progress("2")]);
        assert_eq!(merged.state.single_worker, vec![key("listing"), key("other")]);
    }

    /// What the supervisor sends lands in the runtime's mirror, for a metrics or logs source to read, and nowhere
    /// a heartbeat would report it from again.
    #[test]
    fn obeying_a_mirror_fills_the_registrys_mirror_and_the_logs_readers_only() {
        let _taps = crate::logs::TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let reader = crate::logs::tap();
        let runtime = Runtime::default();
        let registry = &runtime.registry;
        let sample = MetricSampleDto {
            metric: "smeltr_logs_dropped_total".into(),
            labels: vec![("process".into(), "supervisor".into())],
            value: crate::heartbeat::MetricValueDto::Counter(2),
        };

        assert!(obey(
            ToRuntime::Mirror {
                samples: vec![sample],
                logs: vec![LogDto {
                    seq: 3,
                    // Said after the reader opened, which is all a reader is handed.
                    timestamp_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64 + 1_000)
                        .unwrap_or_default(),
                    level: "info".into(),
                    site: "mirror.obey.test".into(),
                    message: "a supervisor said something".into(),
                    thread: None,
                    boot_id: None,
                    fields: vec![("subject".into(), crate::heartbeat::FieldValueDto::Str("w1".into()))],
                }],
            },
            &runtime
        ));
        let mirrored = registry.take_mirrored_delta(&mut Default::default());
        assert_eq!(mirrored.len(), 1, "{mirrored:?}");
        assert_eq!(mirrored[0].key.name, "smeltr_logs_dropped_total");
        assert!(
            registry.peek_delta(crate::metrics::SUBSCRIBER_HEARTBEAT).is_empty(),
            "a mirrored sample would reach the leader twice"
        );
        let said: Vec<_> = reader
            .drain(usize::MAX)
            .into_iter()
            .filter(|e| e.site == "mirror.obey.test")
            .collect();
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(
            said[0].fields,
            vec![("subject".into(), crate::logs::Value::Str("w1".into()))]
        );
    }
}
