//! Live heartbeat state: what each worker thread last reported. Every read (`snapshot`, `workers_in`) and every
//! write serves the in-memory maps directly, so a lookup never waits on disk. `Registry::default` keeps nothing
//! beyond that, rebuilt from the next round of heartbeats like before. `Registry::open` additionally mirrors writes
//! into its own database (`series::RegistryStore`) and loads it back at construction, so a restart no longer loses
//! what a heartbeat already said -- the two durable shapes: `latest_state`, one row per (worker, thread) upserted
//! every beat; and the series, `report.metrics` aggregated by subject into fixed buckets, rolled up and expired
//! (`series::Tier`).
//!
//! [`Registry`] also times the warning catalog (`leader::warnings`, design doc, Control plane): a fact read fresh
//! from a heartbeat has no memory of when it began, so [`FactKey`] and [`Registry::observe_facts`] are the leader's
//! only record of that. Not a warning table -- a key and a millisecond, forgotten the instant its fact is, and now
//! durable for the same reason `latest_state` is: a fact that began before a restart must keep its true start.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::heartbeat::{MetricValueDto, ThreadHeartbeat, ThreadKind};

use super::series::{HeartbeatRows, LatestRow, RegistryStore, SeriesPoint};
use super::store::StoreError;
use super::warnings;

/// What a worker process says about itself on each heartbeat, apart from its threads' reports.
#[derive(Debug, Clone, Default)]
pub struct WorkerFacts {
    /// What the process was started with. Durable, so a worker that fell silent before a restart still answers
    /// what it would do about being unable to reach the leader.
    pub stop_after_unreachable_secs: Option<u64>,
    /// The commit whose bundle the process reported running.
    pub commit: Option<String>,
    /// The process's own build. Empty from a worker older than this field.
    pub smeltr_version: String,
    /// The release that build is. `None` on any other build, and from a worker older than this field.
    pub release: Option<String>,
    /// What the worker's supervisor said it was doing. Not persisted: it describes a moment rather than a state a
    /// restart should remember.
    pub supervising: Option<crate::heartbeat::Supervising>,
    /// What the process was spending against what it was allowed to run in. Not persisted, for the same reason
    /// `supervising` is not: a live reading a restarted leader cannot refresh is worse than none.
    pub resources: Option<crate::heartbeat::ResourceReport>,
    /// When the authority the worker verifies the leader against expires. Durable: the worker this warns about is
    /// one that can no longer reach the leader to say it again.
    pub authority_expires_millis: Option<i64>,
    /// When the worker said it was stopping, where that is the last thing it said. Durable for the same reason as
    /// the expiry: a worker that stopped says nothing more.
    pub stopped_at_ms: Option<i64>,
}

impl WorkerFacts {
    /// What `req` says about its worker, heard at `now_ms`.
    pub fn of(req: &crate::heartbeat::HeartbeatRequest, now_ms: i64) -> WorkerFacts {
        WorkerFacts {
            stop_after_unreachable_secs: req.stop_after_unreachable_secs,
            commit: req.commit.clone(),
            smeltr_version: req.smeltr_version.clone(),
            release: req.release.clone(),
            supervising: req.supervising.clone(),
            resources: req.resources,
            authority_expires_millis: req.authority_expires_millis,
            stopped_at_ms: req.stopping.then_some(now_ms),
        }
    }
}

/// One thread's last report, and when it arrived. The report is the worker's own `ThreadHeartbeat`: the leader adds
/// arrival time and nothing else, so a field added to the wire type reaches `/api/health` without a hop to teach.
struct ThreadState {
    report: ThreadHeartbeat,
    /// What this thread's worker process said about itself, copied onto every thread it reports -- the same way
    /// `report.group_id` already is, a per-worker fact repeated because there is no row keyed by worker alone.
    worker: WorkerFacts,
    last_seen: Instant,
}

/// One thread as `GET /api/health` serves it: the thread's own report, plus what is about the worker process behind
/// it rather than the thread -- which worker sent it, how long ago, what it was started with, and what it runs.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadHealth {
    pub worker_id: String,
    /// The only liveness signal: the leader keeps no online flag.
    pub last_seen_millis_ago: u64,
    /// This worker's own `stop_after_unreachable_secs` (`smeltr.toml`, `[worker]`), as reported on its most recent
    /// heartbeat. `None` is a worker that reported no limit: it keeps running however long the leader is away.
    pub stop_after_unreachable_secs: Option<u64>,
    /// The commit whose bundle this worker reported running on its most recent heartbeat. `None` from a worker
    /// running nothing -- one that has not deployed yet, or whose deployment stopped -- and from one older than
    /// this field. What the fleet is on is the worker's own answer: a commit it fetched but refused never becomes
    /// a deployment, so the leader cannot read it off `main`.
    pub commit: Option<String>,
    /// This worker's own build, as reported on its most recent heartbeat. Empty from a worker older than this
    /// field.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub smeltr_version: String,
    /// The release that build is, as reported on its most recent heartbeat. `None` on any other build, and from a
    /// worker older than this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<String>,
    /// What this worker's supervisor says it is doing. `None` from a worker older than this field, and from one
    /// that has not deployed since it started. A heartbeat says the supervisor is healthy; this is what says
    /// whether anything is running what it was told to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervising: Option<crate::heartbeat::Supervising>,
    /// What this worker process was spending against what it was allowed to run in, as reported on its most
    /// recent heartbeat. `None` from a worker older than this field, and from one whose first heartbeat has not
    /// yet produced a CPU rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<crate::heartbeat::ResourceReport>,
    /// When the certificate authority this worker verifies the leader against expires, as it reported on its most
    /// recent heartbeat. `None` from a worker on plain HTTP, and from one older than this field.
    pub authority_expires_millis: Option<i64>,
    /// When this worker said it was stopping, where that is the last thing it said. Its silence since is a stop
    /// it announced rather than a failure. `None` for a worker that has said anything else since.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_at_millis: Option<i64>,
    #[serde(flatten)]
    pub report: ThreadHeartbeat,
}

/// One instance of a fixed catalog entry (`leader::warnings::Warning`'s underlying condition): which entry, about
/// which subject. Not a warning itself -- carries no severity and no sentence, only enough to say "seen before, and
/// since when".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactKey {
    // Owned rather than `&'static str`: `RegistryStore::load_fact_clock` hands back an entry name it read from
    // disk, which cannot borrow as `'static`. The public constructor still takes one, since every catalog entry
    // name is a literal -- only reconstruction from a durable row needs the owned form.
    entry: String,
    subject: String,
}

impl FactKey {
    pub fn new(entry: &'static str, subject: impl Into<String>) -> FactKey {
        FactKey {
            entry: entry.to_string(),
            subject: subject.into(),
        }
    }
}

pub struct Registry {
    threads: Mutex<HashMap<(String, String), ThreadState>>,
    /// What each worker last said it would verify this leader by.
    ///
    /// Keyed by worker rather than by thread, unlike everything else here: a worker running nothing reports no
    /// thread at all, and that is exactly the worker a rotation must not sign away from. In memory only -- a
    /// leader restarted mid-rotation waits for the fleet to say it again, which is one heartbeat interval and is
    /// the more current answer in any case.
    authorities: Mutex<HashMap<String, (Vec<String>, bool)>>,
    /// When each currently-true catalog fact was first observed, in epoch milliseconds. See [`Registry::observe_facts`].
    fact_clock: Mutex<HashMap<FactKey, i64>>,
    /// The durable mirror. `None` for `Registry::default` -- every read and write above stays in-memory-only, as
    /// it always has; `Registry::open` is what wires this in.
    store: Option<Arc<RegistryStore>>,
    /// When the next look for gone ephemeral workers is due, in epoch milliseconds (`leader::forget`).
    next_sweep_ms: std::sync::atomic::AtomicI64,
    /// When the next look for progress no deployed source reads is due, in epoch milliseconds
    /// (`leader::prune_source_progress`).
    next_prune_ms: std::sync::atomic::AtomicI64,
    /// When this registry began taking heartbeats: when this leader started.
    started: Instant,
    /// Who reads each single-worker source, settled on every beat.
    pub holds: super::holds::Holds,
}

impl Default for Registry {
    fn default() -> Self {
        Registry {
            threads: Default::default(),
            authorities: Default::default(),
            fact_clock: Default::default(),
            store: None,
            next_sweep_ms: Default::default(),
            next_prune_ms: Default::default(),
            started: Instant::now(),
            holds: Default::default(),
        }
    }
}

impl Registry {
    /// Opens (or creates) the registry's own database at `path` and rebuilds in-memory state from what is durable
    /// there: every thread's last report, and every fact's true start time -- so a restart no longer resets either.
    /// `path` is the caller's to choose, the way `sqlite_store::SqliteStore::open` already leaves `leader.db`'s
    /// path to its caller; a sibling of that file (`registry.db`) rather than a table inside it is the point (see
    /// `series` module docs).
    pub fn open(path: &std::path::Path) -> Result<Registry, StoreError> {
        let store = RegistryStore::open(path)?;
        let now_ms = warnings::now_millis();

        let mut threads = HashMap::new();
        for row in store.load_latest_state()? {
            match serde_json::from_str::<ThreadHeartbeat>(&row.report_json) {
                Ok(report) => {
                    threads.insert(
                        (row.worker_id, row.thread_id),
                        ThreadState {
                            report,
                            worker: WorkerFacts {
                                stop_after_unreachable_secs: row.stop_after_unreachable_secs,
                                commit: row.commit,
                                smeltr_version: row.smeltr_version,
                                release: row.release,
                                supervising: row
                                    .supervising_json
                                    .as_deref()
                                    .and_then(|json| serde_json::from_str(json).ok()),
                                // Not durable: a restarted leader has nothing to reload here until the next beat.
                                resources: None,
                                authority_expires_millis: row.authority_expires_millis,
                                stopped_at_ms: row.stopped_at_ms,
                            },
                            last_seen: recovered_instant(row.last_seen_ms, now_ms),
                        },
                    );
                }
                Err(e) => tracing::warn!(
                    target: "registry.latest_state_unreadable",
                    subject = %row.worker_id,
                    thread_id = %row.thread_id,
                    error = %e,
                    "dropping an unreadable latest-state row"
                ),
            }
        }

        let mut fact_clock = HashMap::new();
        for (entry, subject, started_ms) in store.load_fact_clock()? {
            fact_clock.insert(FactKey { entry, subject }, started_ms);
        }

        Ok(Registry {
            threads: Mutex::new(threads),
            authorities: Mutex::new(HashMap::new()),
            fact_clock: Mutex::new(fact_clock),
            store: Some(Arc::new(store)),
            next_sweep_ms: Default::default(),
            next_prune_ms: Default::default(),
            started: Instant::now(),
            holds: Default::default(),
        })
    }

    /// How long this registry has been taking heartbeats. A silence that began before then began while this leader
    /// was not there to hear it.
    pub fn up_for(&self) -> Duration {
        self.started.elapsed()
    }

    /// This registry as if it had been taking heartbeats since `started`.
    #[cfg(test)]
    pub(crate) fn started_at(mut self, started: Instant) -> Self {
        self.started = started;
        self
    }

    /// A thread this database has queryable series for (`series::RegistryStore::points`), for whoever reads E2/E6
    /// next. `None` for an ephemeral `Registry::default`, the same as every other durability-dependent capability
    /// here.
    pub fn series(&self) -> Option<&RegistryStore> {
        self.store.as_deref()
    }

    /// One heartbeat: every thread it reported, durable and in memory. `reports` is the whole request's, because
    /// the durable mirror is written in one transaction -- a half-written heartbeat is a claim about a window that
    /// never held. `worker.stopped_at_ms` is set by a heartbeat saying the worker is stopping, and by no other.
    ///
    /// A stopping heartbeat is the worker's last word, sent once its threads have ended. Its rows become exactly
    /// the reports it carried about the worker itself: what its threads last said is history, and history owns no
    /// partition and blocks no source. What those threads counted still reaches the series.
    ///
    /// `window` is what the beat gathered beside its samples, and which window of the worker's life both are: a
    /// window already recorded adds neither again (`RegistryStore::record_heartbeat`).
    ///
    /// An error is the durable mirror unwritten, window and all; the live view is updated either way.
    pub fn record_heartbeat(
        &self,
        worker_id: &str,
        reports: Vec<ThreadHeartbeat>,
        worker: WorkerFacts,
        window: super::series::Window<'_>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let stopping = worker.stopped_at_ms.is_some();
        let persisted = match &self.store {
            Some(store) => persist_heartbeat(store, worker_id, &reports, &worker, window, now_ms),
            None => Ok(()),
        };
        let mut threads = self.threads.lock().unwrap();
        let last_seen = Instant::now();
        if stopping {
            threads.retain(|(w, _), _| w != worker_id);
        }
        // What a worker says about itself rides on every one of its rows, and a heartbeat carrying no thread at
        // all still said it. Without this a worker with nothing deployed can never correct what its rows claim
        // about the authority it holds, and a warning drawn from the value it has replaced stands forever.
        //
        // `last_seen` is not touched here: liveness is the thread's own, and a thread that stopped reporting is
        // not alive because the process behind it is.
        for (_, state) in threads.iter_mut().filter(|((w, _), _)| w == worker_id) {
            state.worker = worker.clone();
        }
        for report in reports
            .into_iter()
            .filter(|report| !stopping || about_the_worker(report))
        {
            threads.insert(
                (worker_id.to_string(), report.thread_id.clone()),
                ThreadState {
                    report,
                    worker: worker.clone(),
                    last_seen,
                },
            );
        }
        persisted
    }

    /// Drops everything held about `worker_id`, in memory and durably: the leader has forgotten it.
    pub fn forget_worker(&self, worker_id: &str) {
        if let Some(store) = &self.store
            && let Err(e) = store.forget_worker(worker_id)
        {
            tracing::warn!(
                target: "registry.worker_not_forgotten",
                subject = %worker_id,
                error = %e,
                "a forgotten worker's rows could not be dropped from disk, so they come back at the next restart"
            );
        }
        self.threads
            .lock()
            .unwrap()
            .retain(|(worker, _), _| worker != worker_id);
        self.authorities.lock().unwrap().remove(worker_id);
    }

    /// Whether a look for gone ephemeral workers is due at `now_ms`, and if so, that the next is `every` later.
    /// Asked on every heartbeat and every read of the fleet; true for one caller per `every`.
    pub fn sweep_due(&self, now_ms: i64, every: Duration) -> bool {
        due(&self.next_sweep_ms, now_ms, every)
    }

    /// Whether a look for the progress of sources no bundle reads is due at `now_ms`, as [`Registry::sweep_due`].
    pub fn prune_due(&self, now_ms: i64, every: Duration) -> bool {
        due(&self.next_prune_ms, now_ms, every)
    }

    /// Records which authorities a worker would accept, as it just reported them, and whether it takes one from
    /// this leader at all.
    pub fn record_authority(&self, worker_id: &str, fingerprints: Vec<String>, takes_authority: bool) {
        self.authorities
            .lock()
            .unwrap()
            .insert(worker_id.to_string(), (fingerprints, takes_authority));
    }

    /// The workers that verify this leader by an authority their operator gave them, and so take none it hands
    /// out. A rotation reaches them through their operator or not at all.
    pub fn not_taking_authority(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .authorities
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, (_, takes))| !takes)
            .map(|(worker, _)| worker.clone())
            .collect();
        names.sort();
        names
    }

    /// What each of `expected` would verify this leader by, one entry per worker.
    ///
    /// The question a rotation asks: signing under a root any of these lists lacks strands that worker. A worker
    /// in `expected` that has said nothing holds nothing, which is never ready -- how long to wait for one that is
    /// quiet is the operator's deadline, and nothing here second-guesses it.
    ///
    /// Who to expect is the caller's, because the answer is the enrolled roster and this holds only what has been
    /// said out loud.
    ///
    /// A worker that takes no authority from this leader is left out: it can never hold what this leader hands
    /// out, so waiting on it is waiting for the deadline. [`Self::not_taking_authority`] names it to the operator
    /// instead, whose job the incoming root is.
    pub fn authorities_held(&self, expected: &HashSet<String>) -> Vec<Vec<String>> {
        self.holders(expected).into_iter().map(|(_, held)| held).collect()
    }

    /// [`Self::authorities_held`], each list beside the worker it is for, in id order.
    pub fn holders(&self, expected: &HashSet<String>) -> Vec<(String, Vec<String>)> {
        let held = self.authorities.lock().unwrap();
        let mut holders: Vec<(String, Vec<String>)> = expected
            .iter()
            .filter_map(|worker| match held.get(worker) {
                Some((_, false)) => None,
                Some((held, true)) => Some((worker.clone(), held.clone())),
                None => Some((worker.clone(), Vec::new())),
            })
            .collect();
        holders.sort();
        holders
    }

    /// Every worker this leader has heard from since it started, whether or not it reported a thread.
    ///
    /// Beside the enrolled roster rather than instead of it: a worker heartbeating is evidence the roster cannot
    /// contradict, and one enrolled but never heard from is exactly the worker a rotation must not sign away from.
    pub fn known_workers(&self) -> HashSet<String> {
        let mut known: HashSet<String> = self.authorities.lock().unwrap().keys().cloned().collect();
        known.extend(self.threads.lock().unwrap().keys().map(|(worker, _)| worker.clone()));
        known
    }

    /// Whether `worker_id` has heartbeated within `within` without saying it was stopping.
    pub fn heartbeating(&self, worker_id: &str, within: std::time::Duration) -> bool {
        self.threads.lock().unwrap().iter().any(|((worker, _), s)| {
            worker == worker_id && s.worker.stopped_at_ms.is_none() && s.last_seen.elapsed() <= within
        })
    }

    /// Every worker in `group` heard from within `within`.
    ///
    /// What a capture opened against that group expects to hear back from. A worker that has not heartbeated inside
    /// the window is not expected -- it may be gone -- and one that enrolls later is not either; the capture's
    /// deadline is what covers both.
    pub fn workers_in(&self, group: &str, within: std::time::Duration) -> HashSet<String> {
        self.threads
            .lock()
            .unwrap()
            .iter()
            .filter(|((_, _), s)| s.report.group_id == group && s.last_seen.elapsed() <= within)
            .map(|((worker_id, _), _)| worker_id.clone())
            .collect()
    }

    /// Every thread the leader has heard from.
    pub fn snapshot(&self) -> Vec<ThreadHealth> {
        let map = self.threads.lock().unwrap();
        // One clock reading for every row, so the threads of one heartbeat report one age.
        let now = Instant::now();
        map.iter()
            .map(|((worker_id, _), s)| ThreadHealth {
                worker_id: worker_id.clone(),
                last_seen_millis_ago: now.duration_since(s.last_seen).as_millis() as u64,
                stop_after_unreachable_secs: s.worker.stop_after_unreachable_secs,
                commit: s.worker.commit.clone(),
                smeltr_version: s.worker.smeltr_version.clone(),
                release: s.worker.release.clone(),
                supervising: s.worker.supervising.clone(),
                resources: s.worker.resources,
                authority_expires_millis: s.worker.authority_expires_millis,
                stopped_at_millis: s.worker.stopped_at_ms,
                report: s.report.clone(),
            })
            .collect()
    }

    /// What one worker said about itself since the last beat the leader answered. Best-effort: the worker keeps
    /// them until this request succeeds, so a write that fails here costs a round trip rather than the events.
    pub fn record_logs(
        &self,
        worker_id: &str,
        boot_id: u64,
        group_id: Option<&str>,
        events: &[crate::heartbeat::LogDto],
    ) {
        let Some(store) = &self.store else { return };
        // Console-only for the duration: a store that will not take a batch says so, and that saying must not
        // join the queue it was failing to empty.
        crate::logs::while_delivering(|| {
            if let Err(e) = store.record_logs(worker_id, boot_id, group_id, events) {
                tracing::warn!(
                    target: "registry.logs_not_persisted",
                    subject = %worker_id,
                    events = events.len(),
                    error = %e,
                    "logs from a heartbeat could not be persisted, so that much of the fleet's record is \
                     missing"
                );
            }
        });
    }

    /// Every fact the clock holds for one catalog entry: what a caller that cannot yet say whether those facts still
    /// hold passes back to [`Registry::observe_facts`], so their start times are kept rather than forgotten.
    pub fn facts_of(&self, entry: &str) -> Vec<FactKey> {
        let clock = self.fact_clock.lock().unwrap();
        clock.keys().filter(|k| k.entry == entry).cloned().collect()
    }

    /// Reconcile the fact clock against `active`, everything a catalog entry currently reads as true. A key seen
    /// for the first time is stamped `now_ms`; one no longer present is forgotten -- so a fact that stops is a fact
    /// this clock has no memory of, same as the warning it times (design doc, Control plane: "derived, never
    /// stored"). Returns
    /// every active key's start time, for `warnings::derive` to attach to the warning it becomes.
    ///
    /// Call this on every heartbeat, not only when a warning is read: a start time is only as accurate as how often
    /// this runs, and nothing else ticks this clock (design doc, Control plane: "recomputed ... on every
    /// heartbeat").
    pub fn observe_facts(&self, active: &HashSet<FactKey>, now_ms: i64) -> HashMap<FactKey, i64> {
        let mut clock = self.fact_clock.lock().unwrap();
        let removed: Vec<(FactKey, i64)> = clock
            .iter()
            .filter(|(k, _)| !active.contains(k))
            .map(|(k, start)| (k.clone(), *start))
            .collect();
        clock.retain(|k, _| active.contains(k));
        let mut inserted: Vec<FactKey> = Vec::new();
        for k in active {
            if !clock.contains_key(k) {
                clock.insert(k.clone(), now_ms);
                inserted.push(k.clone());
            }
        }
        // The one place every issue with a start time begins and ends, so the log has each one's history once
        // the Issues page no longer shows it. `issue` marks the line as an issue's own, which the catalog's
        // "worker logged warnings" entry leaves out (`warnings::recent_logs`): it would be the same issue twice.
        for k in &inserted {
            let it = super::warnings::logged_issue(&k.entry, &k.subject);
            tracing::warn!(
                target: "issues.began",
                issue = it.issue,
                group = it.group,
                subject = it.subject,
                commit = it.commit,
                "an issue began"
            );
        }
        for (k, start) in &removed {
            let it = super::warnings::logged_issue(&k.entry, &k.subject);
            tracing::info!(
                target: "issues.cleared",
                issue = it.issue,
                group = it.group,
                subject = it.subject,
                commit = it.commit,
                held_secs = (now_ms - start).max(0) / 1000,
                "an issue cleared"
            );
        }
        if let Some(store) = &self.store {
            let removed: Vec<(String, String)> = removed.into_iter().map(|(k, _)| (k.entry, k.subject)).collect();
            let inserted: Vec<(String, String)> = inserted.into_iter().map(|k| (k.entry, k.subject)).collect();
            if let Err(e) = store.sync_fact_clock(&removed, &inserted, now_ms) {
                tracing::warn!(
                    target: "registry.fact_clock_not_persisted",
                    error = %e,
                    "could not persist the fact clock, so first-seen times restart from this moment"
                );
            }
        }
        active.iter().map(|k| (k.clone(), clock[k])).collect()
    }
}

/// `last_seen_ms` (epoch milliseconds, from a durable row) as an `Instant`, backdated by how long ago that was.
/// `snapshot`/`workers_in` read `last_seen.elapsed()`, so this is what lets a reopened registry report the same
/// "how long ago" a live one would have -- a worker that was heartbeating before the restart reads as recently
/// seen, not as never seen.
fn recovered_instant(last_seen_ms: i64, now_ms: i64) -> Instant {
    let ago_ms = (now_ms - last_seen_ms).max(0) as u64;
    Instant::now()
        .checked_sub(Duration::from_millis(ago_ms))
        .unwrap_or_else(Instant::now)
}

/// The labels that make a sample about a named object rather than about a thread: the three kinds a map draws and a
/// focus view charts. A sample carrying none of them has no subject to keep a series for.
const SUBJECT_LABELS: [&str; 3] = ["source", "destination", "pipeline"];

/// Metrics exposed by `GET /api/flow` keep one series per authenticated worker so the dashboard can focus its
/// charts on a worker. Warning inputs deliberately remain logical group/subject series: partition ownership can
/// move between workers, and splitting those series would leave stale or duplicate warnings behind.
const WORKER_SCOPED_METRICS: [&str; 5] = [
    "smeltr_rows_polled_total",
    "smeltr_bytes_polled_total",
    "smeltr_rows_sent_total",
    "smeltr_rows_delivered_total",
    "smeltr_bytes_delivered_total",
];

/// The series' aggregation key for one sample: `group` (the reporting worker's) plus every label but `thread`,
/// sorted, as canonical JSON. `None` for a sample naming no `source`, `destination` or `pipeline` -- a worker- or
/// thread-scoped metric, which is a health question `latest_state` already answers (design brief for this work)
/// rather than something E2/E6 chart over time.
///
/// `group` is part of the key, not just a label alongside it: a source or destination id is unique only within its
/// group plus `shared/` (same as a fact's own key, `warnings::scoped_subject`), so without it two groups running a
/// source of the same id would aggregate into one series -- their samples interleaving into a trend that belongs to
/// neither.
fn subject_key(group: &str, labels: &[(String, String)]) -> Option<String> {
    if !labels.iter().any(|(k, _)| SUBJECT_LABELS.contains(&k.as_str())) {
        return None;
    }
    let mut kept: Vec<(&str, &str)> = labels
        .iter()
        .filter(|(k, _)| k != "thread")
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    kept.push(("group", group));
    kept.sort();
    serde_json::to_string(&kept).ok()
}

/// Whether `report` is about the worker process rather than one of its threads: the report a worker sends with
/// nothing deployed, or its deploy-time gauges.
fn about_the_worker(report: &ThreadHeartbeat) -> bool {
    report.kind == Some(ThreadKind::Worker)
}

/// Write-through to the durable registry: every thread's `latest_state` row, and every subject-scoped sample into
/// the bucket `now_ms` falls in, in one transaction. Best-effort -- a heartbeat must not fail because its durable
/// mirror did, so a write that errors is reported and dropped rather than propagated. A stopping heartbeat's rows
/// are only those about the worker itself, as [`Registry::record_heartbeat`] keeps them; its samples are all of
/// them.
///
/// The encoding and the subject keys are assembled here because the wire types are this module's; `series` owns
/// the transaction and stores both opaquely.
fn persist_heartbeat(
    store: &RegistryStore,
    worker_id: &str,
    reports: &[ThreadHeartbeat],
    worker: &WorkerFacts,
    window: super::series::Window<'_>,
    now_ms: i64,
) -> Result<(), StoreError> {
    let threads: Vec<LatestRow<'_>> = reports
        .iter()
        .filter(|report| worker.stopped_at_ms.is_none() || about_the_worker(report))
        .filter_map(|report| match serde_json::to_string(report) {
            Ok(report_json) => Some(LatestRow {
                thread_id: &report.thread_id,
                report_json,
            }),
            // Its samples still ride along: a report that will not encode is one thread's state missing after a
            // restart, not a reason to lose the window's counts.
            Err(e) => {
                tracing::warn!(
                    target: "registry.heartbeat_not_encoded",
                    subject = %worker_id,
                    thread_id = %report.thread_id,
                    error = %e,
                    "could not encode a thread's report for storage, so it is missing after a restart"
                );
                None
            }
        })
        .collect();

    let points = series_points(worker_id, reports);

    let recorded = store.record_heartbeat(&HeartbeatRows {
        worker_id,
        threads: &threads,
        points: &points,
        window,
        stop_after_unreachable_secs: worker.stop_after_unreachable_secs,
        commit: worker.commit.as_deref(),
        smeltr_version: &worker.smeltr_version,
        release: worker.release.as_deref(),
        supervising_json: worker.supervising.as_ref().and_then(|s| serde_json::to_string(s).ok()),
        authority_expires_millis: worker.authority_expires_millis,
        stopped_at_ms: worker.stopped_at_ms,
        now_ms,
    });
    if let Err(e) = &recorded {
        // One transaction, so this is the whole heartbeat's mirror rather than a count of the rows that failed. An
        // example row rides along: a store that refuses the same point every beat is diagnosable only by what it
        // refused, and the counts alone cannot name it.
        tracing::warn!(
            target: "registry.heartbeat_not_persisted",
            subject = %worker_id,
            threads = threads.len(),
            points = points.len(),
            metric = points.first().map(|p| p.metric).unwrap_or_default(),
            subject_key = points.first().map(|p| p.subject_key.as_str()).unwrap_or_default(),
            error = %e,
            "a heartbeat's durable mirror could not be written, so the worker is refused and sends its window \
             again. `metric` and `subject_key` are the first point it carried, as an example of the shape"
        );
    }

    store.maybe_maintain(now_ms);
    recorded
}

/// Each sample `reports` carries that is a series point. A sample naming no subject, a fact carried as text, and a
/// kind from a newer build are not: `subject_key` says which names no subject.
fn series_points<'a>(worker_id: &str, reports: &'a [ThreadHeartbeat]) -> Vec<SeriesPoint<'a>> {
    reports
        .iter()
        .flat_map(|report| report.metrics.iter().map(move |s| (report, s)))
        .filter_map(|(report, sample)| {
            let (value, counter) = match &sample.value {
                MetricValueDto::Counter(v) => (*v as f64, true),
                MetricValueDto::FloatCounter(v) => (*v, true),
                MetricValueDto::Gauge(v) => (*v, false),
                MetricValueDto::Text(_) | MetricValueDto::Unknown(_) => return None,
            };
            let mut labels = sample.labels.clone();
            // Identity comes from authentication, never from a label supplied in the heartbeat. Only flow metrics
            // need it; warning metrics must continue across worker handoffs as one logical subject series.
            labels.retain(|(key, _)| key != "worker");
            if WORKER_SCOPED_METRICS.contains(&sample.metric.as_str()) {
                labels.push(("worker".to_string(), worker_id.to_string()));
            }
            Some(SeriesPoint {
                metric: &sample.metric,
                subject_key: subject_key(&report.group_id, &labels)?,
                counter,
                value,
            })
        })
        .collect()
}

/// Whether `next_ms` has come at `now_ms`, and if so, moves it `every` later. True for one caller per `every`.
fn due(next_ms: &std::sync::atomic::AtomicI64, now_ms: i64, every: Duration) -> bool {
    use std::sync::atomic::Ordering;
    let due = next_ms.load(Ordering::Relaxed);
    now_ms >= due
        && next_ms
            .compare_exchange(
                due,
                now_ms + every.as_millis() as i64,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::{MetricSampleDto, ThreadKind};
    use crate::leader::series;
    use crate::leader::series::Window;

    /// The question a rotation asks is about the fleet, not about whoever has spoken. A leader restarted
    /// mid-rotation knows its workers from the durable snapshot and has heard from none of them yet; cutting over
    /// on the first to check in would sign away from every worker still on the outgoing root.
    #[test]
    fn a_worker_expected_but_not_heard_from_holds_nothing() {
        let r = Registry::default();
        // Two workers the leader knows about, from threads the snapshot carries across a restart.
        r.record_heartbeat(
            "w1",
            vec![report("t1", "g")],
            WorkerFacts::default(),
            Window::default(),
            0,
        )
        .unwrap();
        r.record_heartbeat(
            "w2",
            vec![report("t2", "g")],
            WorkerFacts::default(),
            Window::default(),
            0,
        )
        .unwrap();
        // Only one of them has said what it holds this run.
        r.record_authority("w1", vec!["incoming".to_string()], true);

        let held = r.authorities_held(&r.known_workers());
        assert_eq!(
            held.len(),
            2,
            "a worker the leader expects was left out of the question"
        );
        assert!(
            held.iter().any(Vec::is_empty),
            "a worker that has not said what it holds was counted as holding the incoming root"
        );
    }

    /// The other half: a worker running nothing reports no thread at all, and is in nothing but this map.
    #[test]
    fn a_worker_with_nothing_deployed_is_still_expected() {
        let r = Registry::default();
        r.record_authority("idle", vec!["incoming".to_string()], true);
        assert_eq!(
            r.authorities_held(&r.known_workers()),
            vec![vec!["incoming".to_string()]]
        );
    }

    /// A worker verifying by its operator's authority can never hold what this leader hands out: it is named to
    /// the operator rather than waited for.
    #[test]
    fn a_worker_taking_no_authority_is_named_rather_than_waited_for() {
        let r = Registry::default();
        r.record_authority("follows", vec!["incoming".to_string()], true);
        r.record_authority("theirs", vec!["operators-root".to_string()], false);
        let expected = HashSet::from(["follows".to_string(), "theirs".to_string()]);
        assert_eq!(r.authorities_held(&expected), vec![vec!["incoming".to_string()]]);
        assert_eq!(r.not_taking_authority(), vec!["theirs".to_string()]);
    }

    /// A worker enrolled and never heard from is exactly the one a rotation must not sign away from, and it is in
    /// nothing this holds.
    #[test]
    fn a_worker_the_caller_expects_but_this_has_never_heard_from_holds_nothing() {
        let r = Registry::default();
        r.record_authority("spoke", vec!["incoming".to_string()], true);
        let expected = HashSet::from(["spoke".to_string(), "enrolled-and-silent".to_string()]);
        let held = r.authorities_held(&expected);
        assert_eq!(held.len(), 2);
        assert!(
            held.iter().any(Vec::is_empty),
            "a worker that has said nothing was counted as ready"
        );
    }

    /// A revoked worker is refused at the gate and can never answer, so waiting on it is waiting on nobody.
    #[test]
    fn a_worker_that_can_never_answer_is_not_waited_for() {
        let r = Registry::default();
        r.record_heartbeat(
            "gone",
            vec![report("t1", "g")],
            WorkerFacts::default(),
            Window::default(),
            0,
        )
        .unwrap();
        r.record_authority("gone", vec!["outgoing".to_string()], true);
        // Expected is the caller's answer, and a revoked worker is not in it.
        assert!(r.authorities_held(&HashSet::new()).is_empty());
    }

    /// Quiet is not gone. However long ago a worker said what it holds, it still holds it: how long a rotation
    /// waits on a quiet worker is the operator's deadline, and nothing here ages it out first.
    #[test]
    fn a_worker_that_has_merely_gone_quiet_is_still_waited_for() {
        let r = Registry::default();
        r.record_heartbeat(
            "quiet",
            vec![report("t1", "g")],
            WorkerFacts::default(),
            Window::default(),
            0,
        )
        .unwrap();
        r.record_authority("quiet", vec!["outgoing".to_string()], true);

        // Nothing injects a clock here, so the moment it was last heard from is set back instead: a day, or as far
        // as this machine's clock reaches.
        let long_ago = [86_400, 3_600, 60]
            .into_iter()
            .find_map(|secs| Instant::now().checked_sub(Duration::from_secs(secs)))
            .expect("a clock that reaches back a minute");
        for state in r.threads.lock().unwrap().values_mut() {
            state.last_seen = long_ago;
        }

        assert_eq!(
            r.authorities_held(&r.known_workers()),
            vec![vec!["outgoing".to_string()]]
        );
    }

    fn report(thread_id: &str, group_id: &str) -> ThreadHeartbeat {
        ThreadHeartbeat {
            thread_id: thread_id.to_string(),
            group_id: group_id.to_string(),
            kind: Some(ThreadKind::Reader),
            partitions_owned: vec![],
            metrics: vec![],
        }
    }

    #[test]
    fn snapshot_holds_one_entry_per_worker_thread_pair() {
        let r = Registry::default();
        r.record_heartbeat(
            "w1",
            vec![report("t0", "pool")],
            WorkerFacts::default(),
            Window::default(),
            0,
        )
        .unwrap();
        r.record_heartbeat(
            "w1",
            vec![report("t1", "pool")],
            WorkerFacts::default(),
            Window::default(),
            0,
        )
        .unwrap();
        r.record_heartbeat(
            "w2",
            vec![report("t0", "pool")],
            WorkerFacts::default(),
            Window::default(),
            0,
        )
        .unwrap();
        r.record_heartbeat(
            "w1",
            vec![report("t0", "prod")],
            WorkerFacts::default(),
            Window::default(),
            0,
        )
        .unwrap();
        let snapshot = r.snapshot();
        assert_eq!(snapshot.len(), 3);
        let w1t0 = snapshot
            .iter()
            .find(|s| s.worker_id == "w1" && s.report.thread_id == "t0")
            .unwrap();
        assert_eq!(w1t0.report.group_id, "prod", "the latest report wins");
    }

    #[test]
    fn health_keeps_the_report_s_own_field_names() {
        let r = Registry::default();
        r.record_heartbeat(
            "w1",
            vec![report("t0", "pool")],
            WorkerFacts {
                stop_after_unreachable_secs: Some(300),
                commit: Some("c0ffee".to_string()),
                ..Default::default()
            },
            Window::default(),
            0,
        )
        .unwrap();
        let json = serde_json::to_value(&r.snapshot()[0]).unwrap();
        // Flattened, so the report's fields sit beside the leader's rather than under a key of their own.
        for key in [
            "workerId",
            "lastSeenMillisAgo",
            "stopAfterUnreachableSecs",
            "commit",
            "authorityExpiresMillis",
            "threadId",
            "groupId",
            "kind",
            "partitionsOwned",
            "metrics",
        ] {
            assert!(json.get(key).is_some(), "{key} missing from {json}");
        }
        assert_eq!(json.as_object().unwrap().len(), 10, "no key beyond those ten");
        assert_eq!(json["stopAfterUnreachableSecs"], 300);
        assert_eq!(json["commit"], "c0ffee");
    }

    #[test]
    fn a_fact_seen_for_the_first_time_is_stamped_with_now() {
        let r = Registry::default();
        let key = FactKey::new("source_blocked", "7");
        let active = HashSet::from([key.clone()]);
        let starts = r.observe_facts(&active, 1_000);
        assert_eq!(starts[&key], 1_000);
    }

    #[test]
    fn a_fact_still_active_keeps_its_original_start() {
        let r = Registry::default();
        let key = FactKey::new("source_blocked", "7");
        let active = HashSet::from([key.clone()]);
        r.observe_facts(&active, 1_000);
        let starts = r.observe_facts(&active, 5_000);
        assert_eq!(
            starts[&key], 1_000,
            "the fact has been true the whole time, so its start does not move"
        );
    }

    #[test]
    fn a_fact_no_longer_active_is_forgotten_and_restarts_if_it_returns() {
        let r = Registry::default();
        let key = FactKey::new("source_blocked", "7");
        r.observe_facts(&HashSet::from([key.clone()]), 1_000);
        r.observe_facts(&HashSet::new(), 2_000);
        let starts = r.observe_facts(&HashSet::from([key.clone()]), 9_000);
        assert_eq!(
            starts[&key], 9_000,
            "gone once, so this is a new occurrence, not the old one continuing"
        );
    }

    #[test]
    fn an_issue_is_logged_when_it_begins_and_when_it_clears() {
        let r = Registry::default();
        let key = FactKey::new("source_blocked", r#"["prod","syslog"]"#);
        let began = crate::logs::said(|| {
            r.observe_facts(&HashSet::from([key.clone()]), 1_000);
            // Still active: nothing new to say.
            r.observe_facts(&HashSet::from([key.clone()]), 2_000);
        });
        assert_eq!(began.len(), 1, "{began:?}");
        let (level, site, fields) = &began[0];
        assert_eq!((*level, site.as_str()), (tracing::Level::WARN, "issues.began"));
        let field = |k: &str| fields.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(field("issue"), Some("sourceBlocked"));
        assert_eq!(field("group"), Some("prod"));
        assert_eq!(field("subject"), Some("syslog"));
        assert_eq!(field("message"), Some("an issue began"));

        let cleared = crate::logs::said(|| {
            r.observe_facts(&HashSet::new(), 61_000);
        });
        assert_eq!(cleared.len(), 1, "{cleared:?}");
        let (level, site, fields) = &cleared[0];
        assert_eq!((*level, site.as_str()), (tracing::Level::INFO, "issues.cleared"));
        let field = |k: &str| fields.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(field("held_secs"), Some("60"));
        assert_eq!(field("message"), Some("an issue cleared"));
    }

    #[test]
    fn distinct_subjects_of_the_same_entry_are_tracked_independently() {
        let r = Registry::default();
        let a = FactKey::new("source_blocked", "1");
        let b = FactKey::new("source_blocked", "2");
        r.observe_facts(&HashSet::from([a.clone()]), 1_000);
        let starts = r.observe_facts(&HashSet::from([a.clone(), b.clone()]), 2_000);
        assert_eq!(starts[&a], 1_000, "already active");
        assert_eq!(starts[&b], 2_000, "just appeared");
    }

    fn temp_registry_path(label: &str) -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("smeltr-registry-test-{label}-{}-{n}.db", std::process::id()));
        cleanup_registry_db(&path);
        path
    }

    fn cleanup_registry_db(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        for suffix in ["-wal", "-shm"] {
            let mut p = path.as_os_str().to_owned();
            p.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(p));
        }
    }

    /// A worker that said it was stopping has stopped, and says nothing more -- so the leader keeps that across its
    /// own restart. What it keeps is what the worker said about itself: the threads it ran are over, so their rows
    /// go, partitions and all, while what they counted last still reaches the series. The worker's next ordinary
    /// heartbeat is it back, and ends the stop.
    #[test]
    fn a_stop_on_notice_keeps_only_the_worker_and_survives_a_restart() {
        let path = temp_registry_path("stopped");
        let started_at = warnings::now_millis();
        let beat = |r: &Registry, threads: Vec<ThreadHeartbeat>, stopping: bool, at: i64| {
            r.record_heartbeat(
                "w1",
                threads,
                WorkerFacts {
                    stopped_at_ms: stopping.then_some(at),
                    ..Default::default()
                },
                Window::default(),
                at,
            )
            .unwrap();
        };
        let itself = ThreadHeartbeat {
            kind: Some(ThreadKind::Worker),
            ..report("w1-bootstrap", "pool")
        };
        let reading = ThreadHeartbeat {
            partitions_owned: vec![crate::heartbeat::PartitionRef {
                topic: "orders".to_string(),
                partition: 0,
            }],
            ..report("reader-0", "pool")
        };
        // What a reader counted before it ended, riding the notice under its own thread id, as a worker sends it.
        let counted_last = ThreadHeartbeat {
            kind: None,
            metrics: vec![MetricSampleDto {
                metric: "smeltr_rows_polled_total".to_string(),
                labels: vec![
                    ("source".to_string(), "orders-west".to_string()),
                    ("thread".to_string(), "reader-0".to_string()),
                ],
                value: MetricValueDto::Counter(5),
            }],
            ..report("reader-0", "pool")
        };
        let rows = |r: &Registry| -> Vec<(String, Option<i64>)> {
            let mut rows: Vec<_> = r
                .snapshot()
                .into_iter()
                .map(|t| (t.report.thread_id, t.stopped_at_millis))
                .collect();
            rows.sort();
            rows
        };
        let stopped = vec![("w1-bootstrap".to_string(), Some(started_at + 1))];
        {
            let r = Registry::open(&path).unwrap();
            beat(&r, vec![reading, itself.clone()], false, started_at);
            beat(&r, vec![itself.clone(), counted_last], true, started_at + 1);
            assert_eq!(rows(&r), stopped);
        }

        let r = Registry::open(&path).unwrap();
        assert_eq!(rows(&r), stopped);
        let key = subject_key(
            "pool",
            &[
                ("source".to_string(), "orders-west".to_string()),
                ("thread".to_string(), "reader-0".to_string()),
                ("worker".to_string(), "w1".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            r.series()
                .unwrap()
                .points("smeltr_rows_polled_total", &key, series::Tier::TenSeconds)
                .unwrap(),
            vec![(series::Tier::TenSeconds.align(started_at + 1), 5.0)]
        );

        beat(&r, vec![itself], false, started_at + 2);
        assert!(r.snapshot().iter().all(|t| t.stopped_at_millis.is_none()));
        drop(r);
        cleanup_registry_db(&path);
    }

    /// A beat the store will not take is an error, so the leader refuses it and the worker sends its window again;
    /// what it said of its threads still reaches the live view.
    #[test]
    fn a_beat_the_store_will_not_take_is_an_error_and_still_seen() {
        let path = temp_registry_path("not-taken");
        let r = Registry::open(&path).unwrap();
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TRIGGER refused BEFORE INSERT ON windows BEGIN SELECT RAISE(ABORT, 'refused'); END")
            .unwrap();
        let window = Window {
            boot_id: 7,
            seq: Some(1),
            streams: None,
        };
        assert!(
            r.record_heartbeat(
                "w1",
                vec![report("reader-0", "pool")],
                WorkerFacts::default(),
                window,
                0
            )
            .is_err(),
            "a beat that was not recorded was answered as recorded"
        );
        assert!(r.snapshot().iter().any(|t| t.report.thread_id == "reader-0"));
        drop(r);
        cleanup_registry_db(&path);
    }

    /// The two defects the durable registry closes: a restart used to reset a warning's start time, and could not
    /// tell a worker that had never heartbeated from one that was heartbeating right up until the leader restarted.
    #[test]
    fn latest_state_and_the_fact_clock_survive_a_reopen() {
        let path = temp_registry_path("reopen");
        let key = FactKey::new("source_blocked", "7");
        // Real epoch milliseconds, not small test literals: `recovered_instant` backdates against
        // `warnings::now_millis()` at the reopen, so the start time here has to be a real "a moment ago".
        let started_at = warnings::now_millis();
        {
            let r = Registry::open(&path).unwrap();
            r.record_heartbeat(
                "w1",
                vec![report("t0", "pool")],
                WorkerFacts::default(),
                Window::default(),
                started_at,
            )
            .unwrap();
            r.observe_facts(&HashSet::from([key.clone()]), started_at);
        }

        let r = Registry::open(&path).unwrap();
        let snapshot = r.snapshot();
        assert_eq!(snapshot.len(), 1, "the thread's last report survived the reopen");
        assert_eq!(snapshot[0].worker_id, "w1");
        assert!(
            snapshot[0].last_seen_millis_ago < 60_000,
            "a worker heartbeating right up to the restart must not read as never seen, got {}ms ago",
            snapshot[0].last_seen_millis_ago
        );

        let starts = r.observe_facts(&HashSet::from([key.clone()]), started_at + 50_000);
        assert_eq!(
            starts[&key], started_at,
            "the fact's start is when it was first observed, not when the registry reopened"
        );
        cleanup_registry_db(&path);
    }

    /// A worker behind its desired commit stays behind across a leader restart. The `NOT_DEPLOYING` fact ages
    /// from when the worker fell behind, and the fact clock keeps a start time only while the fact reads as true;
    /// a restored row that forgot its supervisor would read as caught up, drop the fact, and restart the clock on
    /// the next beat -- so repeated restarts would hold the warning off indefinitely.
    #[test]
    fn a_worker_behind_its_target_is_still_behind_after_a_restart() {
        let path = temp_registry_path("supervising-reopen");
        let started_at = warnings::now_millis();
        let supervising = crate::heartbeat::Supervising {
            desired_commit: Some("beef".to_string()),
            phase: Some("starting".to_string()),
            ..Default::default()
        };
        {
            let r = Registry::open(&path).unwrap();
            r.record_heartbeat(
                "w1",
                vec![report("t0", "pool")],
                WorkerFacts {
                    commit: Some("c0ffee".to_string()),
                    supervising: Some(supervising.clone()),
                    ..Default::default()
                },
                Window::default(),
                started_at,
            )
            .unwrap();
        }

        let r = Registry::open(&path).unwrap();
        let snapshot = r.snapshot();
        assert_eq!(
            snapshot[0]
                .supervising
                .as_ref()
                .and_then(|s| s.desired_commit.as_deref()),
            Some("beef"),
            "a restored worker forgot the commit it was told to deploy"
        );
        let active =
            warnings::currently_active_facts(&snapshot, started_at, Duration::from_millis(1500), &HashSet::new());
        assert!(
            active.contains(&warnings::not_deploying_key("w1", "beef")),
            "a worker behind its target read as caught up after the restart"
        );
        cleanup_registry_db(&path);
    }

    #[test]
    fn a_silent_worker_still_answers_what_it_does_when_the_leader_is_away() {
        let path = temp_registry_path("policy-reopen");
        let started_at = warnings::now_millis();
        {
            let r = Registry::open(&path).unwrap();
            r.record_heartbeat(
                "w1",
                vec![report("t0", "pool")],
                WorkerFacts {
                    stop_after_unreachable_secs: Some(300),
                    commit: Some("c0ffee".to_string()),
                    ..Default::default()
                },
                Window::default(),
                started_at,
            )
            .unwrap();
        }

        // The worker never heartbeats again: this is the restart during which it was already gone.
        let r = Registry::open(&path).unwrap();
        let snapshot = r.snapshot();
        assert_eq!(
            snapshot[0].stop_after_unreachable_secs,
            Some(300),
            "a worker that fell silent before the restart still answers what it would do about it"
        );
        assert_eq!(
            snapshot[0].commit.as_deref(),
            Some("c0ffee"),
            "and what it was last running, which nothing else can reconstruct"
        );
        cleanup_registry_db(&path);
    }

    /// A worker with nothing deployed heartbeats with no threads at all. What it says about itself still has to
    /// land, or its rows go on claiming the authority it held before it was given a new one.
    #[test]
    fn a_heartbeat_carrying_no_thread_still_says_what_the_worker_holds() {
        let path = temp_registry_path("idle-worker-facts");
        let started_at = warnings::now_millis();
        let r = Registry::open(&path).unwrap();
        r.record_heartbeat(
            "w1",
            vec![report("t0", "pool")],
            WorkerFacts {
                commit: Some("c0ffee".to_string()),
                authority_expires_millis: Some(started_at + 1000),
                ..Default::default()
            },
            Window::default(),
            started_at,
        )
        .unwrap();

        // Its deployment stopped, and it took a new authority. Its supervisor is meanwhile trying to deploy a
        // commit it has not got running -- which is the state the worker has no thread left to report through.
        let supervising = crate::heartbeat::Supervising {
            desired_commit: Some("beef".to_string()),
            phase: Some("starting".to_string()),
            ..Default::default()
        };
        r.record_heartbeat(
            "w1",
            vec![],
            WorkerFacts {
                supervising: Some(supervising),
                authority_expires_millis: Some(started_at + 9000),
                ..Default::default()
            },
            Window::default(),
            started_at + 1,
        )
        .unwrap();

        let snapshot = r.snapshot();
        assert_eq!(snapshot.len(), 1, "the thread's row is still there to carry it");
        assert_eq!(
            snapshot[0].authority_expires_millis,
            Some(started_at + 9000),
            "an idle worker could not correct what its rows claim it holds"
        );
        assert_eq!(snapshot[0].commit, None, "nor what it is running");
        assert_eq!(
            snapshot[0]
                .supervising
                .as_ref()
                .and_then(|s| s.desired_commit.as_deref()),
            Some("beef"),
            "nor what it is trying to deploy, which is what says it is behind rather than idle"
        );
        cleanup_registry_db(&path);
    }

    /// The worker this warns about is one whose expiring authority is about to stop it reaching the leader, so it
    /// will never be in a position to say this again. Lost at a restart, the warning naming it goes too.
    #[test]
    fn a_stranded_worker_s_authority_expiry_survives_a_restart() {
        let path = temp_registry_path("authority-reopen");
        let started_at = warnings::now_millis();
        let expires = started_at + 3 * 24 * 60 * 60 * 1000;
        {
            let r = Registry::open(&path).unwrap();
            r.record_heartbeat(
                "w1",
                vec![report("t0", "pool")],
                WorkerFacts {
                    authority_expires_millis: Some(expires),
                    ..Default::default()
                },
                Window::default(),
                started_at,
            )
            .unwrap();
        }

        let r = Registry::open(&path).unwrap();
        assert_eq!(
            r.snapshot()[0].authority_expires_millis,
            Some(expires),
            "a worker that cannot reach the leader again lost the expiry that says why"
        );
        cleanup_registry_db(&path);
    }

    /// A heartbeat is written whole: every thread it reported reaches `latest_state`, and every thread's
    /// subject-scoped samples reach the series -- while what is not a series point (a text fact, a sample naming
    /// no subject) is skipped without taking the rest of the heartbeat with it.
    #[test]
    fn one_heartbeat_persists_every_threads_state_and_only_its_subject_scoped_samples() {
        let path = temp_registry_path("beat-write-through");
        let r = Registry::open(&path).unwrap();

        let mut reader = report("reader-0", "pool");
        reader.metrics = vec![
            MetricSampleDto {
                metric: "smeltr_rows_polled_total".to_string(),
                labels: vec![("source".to_string(), "orders-west".to_string())],
                value: MetricValueDto::Counter(5),
            },
            // A fact, not a figure: no series point.
            MetricSampleDto {
                metric: "smeltr_source_blocked_reason".to_string(),
                labels: vec![("source".to_string(), "orders-west".to_string())],
                value: MetricValueDto::Text("held".to_string()),
            },
            // Worker-scoped: names no subject, so `subject_key` drops it.
            MetricSampleDto {
                metric: "smeltr_memory_peak_bytes".to_string(),
                labels: vec![],
                value: MetricValueDto::Gauge(900.0),
            },
        ];
        let mut egress = report("egress-1", "pool");
        egress.metrics = vec![MetricSampleDto {
            metric: "smeltr_rows_sent_total".to_string(),
            labels: vec![("destination".to_string(), "es-prod".to_string())],
            value: MetricValueDto::Counter(4),
        }];

        r.record_heartbeat(
            "w1",
            vec![reader, egress],
            WorkerFacts::default(),
            Window::default(),
            12_345,
        )
        .unwrap();

        assert_eq!(r.snapshot().len(), 2, "both threads are live state");
        let series = r.series().unwrap();
        let bucket = series::Tier::TenSeconds.align(12_345);
        let points = |metric: &str, labels: &[(&str, &str)]| {
            let mut owned: Vec<(String, String)> = labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();
            owned.push(("worker".to_string(), "w1".to_string()));
            series
                .points(metric, &subject_key("pool", &owned).unwrap(), series::Tier::TenSeconds)
                .unwrap()
        };
        assert_eq!(
            points("smeltr_rows_polled_total", &[("source", "orders-west")]),
            vec![(bucket, 5.0)],
            "the reader's counter"
        );
        assert_eq!(
            points("smeltr_rows_sent_total", &[("destination", "es-prod")]),
            vec![(bucket, 4.0)],
            "and the egress thread's, from the same call"
        );
        assert!(
            points("smeltr_source_blocked_reason", &[("source", "orders-west")]).is_empty(),
            "a text fact is no series point"
        );
        cleanup_registry_db(&path);
    }

    #[test]
    fn record_heartbeat_persists_a_subject_scoped_sample_into_the_series() {
        let path = temp_registry_path("series-write-through");
        let r = Registry::open(&path).unwrap();
        let mut with_metrics = report("reader-0", "pool");
        with_metrics.metrics = vec![crate::heartbeat::MetricSampleDto {
            metric: "smeltr_rows_polled_total".to_string(),
            labels: vec![
                ("source".to_string(), "orders-west".to_string()),
                ("thread".to_string(), "reader-0".to_string()),
                ("worker".to_string(), "spoofed-worker".to_string()),
            ],
            value: MetricValueDto::Counter(5),
        }];
        r.record_heartbeat(
            "w1",
            vec![with_metrics],
            WorkerFacts::default(),
            Window::default(),
            12_345,
        )
        .unwrap();

        let key = subject_key(
            "pool",
            &[
                ("source".to_string(), "orders-west".to_string()),
                ("thread".to_string(), "reader-0".to_string()),
                ("worker".to_string(), "w1".to_string()),
            ],
        )
        .unwrap();
        let points = r
            .series()
            .unwrap()
            .points("smeltr_rows_polled_total", &key, series::Tier::TenSeconds)
            .unwrap();
        assert_eq!(points, vec![(series::Tier::TenSeconds.align(12_345), 5.0)]);
        let spoofed_key = subject_key(
            "pool",
            &[
                ("source".to_string(), "orders-west".to_string()),
                ("worker".to_string(), "spoofed-worker".to_string()),
            ],
        )
        .unwrap();
        assert!(
            r.series()
                .unwrap()
                .points("smeltr_rows_polled_total", &spoofed_key, series::Tier::TenSeconds)
                .unwrap()
                .is_empty(),
            "a submitted worker label must not override the authenticated worker"
        );
        cleanup_registry_db(&path);
    }

    #[test]
    fn warning_series_are_logical_subjects_not_worker_scoped() {
        let path = temp_registry_path("logical-warning-series");
        let r = Registry::open(&path).unwrap();
        let mut with_lag = report("reader-0", "pool");
        with_lag.metrics = vec![MetricSampleDto {
            metric: "smeltr_source_lag".to_string(),
            labels: vec![
                ("source".to_string(), "orders-west".to_string()),
                ("topic".to_string(), "orders".to_string()),
                ("partition".to_string(), "0".to_string()),
                ("worker".to_string(), "spoofed-worker".to_string()),
            ],
            value: MetricValueDto::Gauge(12.0),
        }];
        r.record_heartbeat("w1", vec![with_lag], WorkerFacts::default(), Window::default(), 12_345)
            .unwrap();

        let key = subject_key(
            "pool",
            &[
                ("source".to_string(), "orders-west".to_string()),
                ("topic".to_string(), "orders".to_string()),
                ("partition".to_string(), "0".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            r.series()
                .unwrap()
                .points("smeltr_source_lag", &key, series::Tier::TenSeconds)
                .unwrap(),
            vec![(series::Tier::TenSeconds.align(12_345), 12.0)]
        );
        cleanup_registry_db(&path);
    }

    #[test]
    fn subject_key_is_none_for_a_worker_scoped_sample_naming_no_subject() {
        assert_eq!(
            subject_key("g", &[("thread".to_string(), "reader-0".to_string())]),
            None
        );
    }

    /// A pipeline is a subject too: its per-function counts are what a focus view charts, and before this they were
    /// dropped on the way into the store for naming neither a source nor a destination.
    #[test]
    fn subject_key_keeps_a_pipeline_scoped_sample() {
        let key = subject_key(
            "g",
            &[
                ("function".to_string(), "2".to_string()),
                ("pipeline".to_string(), "orders-normalize".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            key,
            r#"[["function","2"],["group","g"],["pipeline","orders-normalize"]]"#
        );
    }

    #[test]
    fn subject_key_drops_the_thread_label_but_keeps_the_rest_sorted_with_the_group() {
        let key = subject_key(
            "g",
            &[
                ("thread".to_string(), "reader-0".to_string()),
                ("partition".to_string(), "3".to_string()),
                ("source".to_string(), "orders".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(key, r#"[["group","g"],["partition","3"],["source","orders"]]"#);
    }

    /// The defect this closes: two groups running a source of the same id used to aggregate into one series.
    #[test]
    fn subject_key_differs_between_groups_for_the_same_labels() {
        let labels = [("source".to_string(), "7".to_string())];
        assert_ne!(
            subject_key("g1", &labels),
            subject_key("g2", &labels),
            "two groups' samples for the same source must not land in one series"
        );
    }
}
