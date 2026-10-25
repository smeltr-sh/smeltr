//! Capture sessions the leader is running: what was asked for, which workers have answered, and the records they
//! sent back.
//!
//! In memory only, like the registry beside it. A capture is something a person is watching happen; one interrupted
//! by a restart is one nobody is waiting on any more, and the sample it would have produced was never saved. A
//! restarted leader lists no capture to any worker, so every worker lets go of what it was running for the old one.
//!
//! The leader opens no connection of its own here. It hands every heartbeating worker the same invitation and merges
//! what comes back, so a source running on one worker and a source running on none are the same request.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::heartbeat::{CaptureBatch, CaptureEnd, CaptureInvitation, CaptureReport, CapturedRow};

/// What bounds a capture, and what a caller may ask for. A request past a ceiling is clamped rather than refused.
/// The total is at most what a sample holds ([`crate::config_file::MAX_SAMPLE_RECORDS`]), since a capture ends as
/// one; the byte and deadline ceilings protect the leader's memory and a worker's time.
pub const MAX_BYTES_CEILING: usize = 64 * 1024 * 1024;
pub const MAX_DEADLINE_SECS: u64 = 300;

/// What one capture may hold in bytes, however many workers answer it: a fleet's records sit in the leader, which
/// also serves the GUI and every heartbeat.
pub const SESSION_BYTES_CEILING: usize = MAX_BYTES_CEILING;

/// What one worker's report may weigh encoded, on the leader's endpoint and on the runtime pipe alike: the raw
/// ceiling with JSON escaping at its worst of six bytes per one, and room for what a record carries beside its
/// raw text.
pub const REPORT_BODY_LIMIT: usize = 6 * MAX_BYTES_CEILING + 8 * 1024 * 1024;

pub const DEFAULT_MAX_MATCHES: usize = 100;
pub const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_DEADLINE_SECS: u64 = 30;

/// What a caller asked for, before clamping. Exactly one of `source` and `destination` is set.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureRequest {
    /// The worker group to ask (`docs/user-stories.md` G1): a source in one may not exist in another, and a capture
    /// goes only to the workers of the group it was opened against.
    pub group: String,
    #[serde(default)]
    pub source: Option<crate::config_file::SourceDto>,
    /// A destination the group's deployment sends to, by name. What it reads saves as a sample that names no source:
    /// an outgoing row is what a pipeline finished, which no route takes in.
    #[serde(default)]
    pub destination: Option<String>,
    /// The identity that source names, where it names one. Sent as configuration like the source itself: a
    /// capture samples the author's branch, which nothing here reads.
    #[serde(default)]
    pub identity: Option<crate::config_file::IdentityDto>,
    #[serde(default)]
    pub filter: Option<String>,
    /// Where a Kafka or Event Hubs source's capture starts. Absent, it reads from the latest.
    #[serde(default)]
    pub start: Option<crate::config_file::StartPositionDto>,
    /// How many records the capture wants in all, at most what a sample holds. Each worker is handed the same
    /// number as its own bound, so any one of them can fill the capture alone, and the leader stops at that many
    /// whoever sent them.
    #[serde(default)]
    pub max_matches: Option<usize>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    pub deadline_secs: Option<u64>,
}

/// A capture as a caller watching it sees it.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureStatus {
    pub session: String,
    /// The destination a capture there reads, by name. Absent on a capture of a source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,
    pub done: bool,
    /// Ended by whoever was watching, before its deadline or its total. What it holds is what it will ever hold.
    pub stopped: bool,
    /// How many records the capture asked for in all.
    pub max_matches: usize,
    /// Every worker the capture knows of -- expected when it opened, or handed the invitation since -- in name
    /// order. No worker leaves the list for the capture's life.
    pub workers: Vec<CaptureWorker>,
    /// What every worker sent, folded: one batch per distinct column list, each row naming its worker.
    pub batches: Vec<CaptureBatch>,
    /// Why each worker stopped, so a short sample says which bound it hit.
    pub ended: Vec<(String, CaptureEnd)>,
    pub seconds_left: u64,
    /// Workers that were expected to answer and have not yet. Empty on a finished capture that heard from everyone.
    /// Once a capture is over, only a worker named here that had stopped before it ended may still say why; the
    /// rest never answer.
    pub waiting_on: Vec<String>,
    /// Whether the byte ceiling dropped records a worker sent. A sample saved from it holds only what the ceiling
    /// kept, and the sample file carries no mark of the loss.
    pub truncated: bool,
    /// What Run found out about an SQL source's query: its columns, and what the database plans for a poll.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<crate::heartbeat::SqlRun>,
    /// The fields the newest Avro writer's schema a worker read declares, in its order. Empty from a source that
    /// reads no Avro.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub avro: Vec<crate::heartbeat::AvroField>,
}

/// One worker in a capture, and where it stands.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureWorker {
    pub worker: String,
    pub state: WorkerState,
}

/// `Waiting` has sent no report the capture folded in. `Filling` has, and has not yet said why it stopped; the
/// capture may hold none of its records. `Answered` has said why.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    Waiting,
    Filling,
    Answered,
}

struct Session {
    invitation: CaptureInvitation,
    started: Instant,
    /// When it opened, in microseconds since the epoch, and the commit the fleet ran then: what a sample saved from
    /// it records ([`Captures::capture_fields`]).
    opened_micros: i64,
    config: Option<String>,
    /// Set by [`Captures::stop`] on a capture still running. Nothing more is folded in and nobody more is invited.
    stopped: bool,
    /// The workers this capture is waiting for: those in its group the leader had heard from when it opened.
    ///
    /// Fixed at that moment rather than grown as workers heartbeat. A set that grows cannot say whether it is
    /// complete, so a capture keyed on it finishes as soon as the first worker to be invited answers -- and every
    /// worker that had not heartbeated yet is left out of a sample nothing marks as partial. A one-worker capture
    /// waits for the one it invited instead.
    expected: HashSet<String>,
    /// The workers of `expected` that held rows for this capture when it opened (`warnings::holding_for_capture`). Its
    /// total does not end it before each has reported, since what they hold is what the capture is opened to see.
    holding: HashSet<String>,
    /// Every worker handed this invitation, and every one that has sent a report saying why it stopped. A worker
    /// still filling has reported -- [`Session::per_worker`] may already hold what it caught -- without being
    /// answered: that is what a progress report is.
    invited: HashSet<String>,
    answered: HashSet<String>,
    /// Each worker's most recent report, replaced whole on every one that arrives rather than appended to: a
    /// report always carries everything that worker has caught so far, progress or final, so the newest one is
    /// the only one worth keeping.
    ///
    /// Trimmed by [`Session::recompute`] to what it actually folded in, so a worker's entry never outgrows its
    /// own share of the session's ceilings -- unbounded here would let a capture's fleet, not its ceilings, set
    /// how much the leader holds: a hundred workers each up to a byte ceiling meant for the whole session.
    per_worker: HashMap<String, Vec<CaptureBatch>>,
    /// `batches`, `rows` and `bytes`, folded from `per_worker` against the session's own ceilings. Kept rather than
    /// computed on every [`Session::status`], which the GUI's poll loop calls once a second for as long as a
    /// capture runs.
    batches: Vec<CaptureBatch>,
    rows: usize,
    bytes: usize,
    truncated: bool,
    ended: Vec<(String, CaptureEnd)>,
    /// Whether one worker reads this capture's source: a single-worker kind is read by one worker at a time, and Run
    /// asks its database once.
    one_worker: bool,
    sql: Option<crate::heartbeat::SqlRun>,
    avro: Vec<crate::heartbeat::AvroField>,
}

impl Session {
    /// Refold `batches`, `rows`, `bytes` and `truncated` from `per_worker`, in worker-id order so the same inputs always
    /// fold the same way. Called every time `per_worker` changes -- sessions are small, bounded by their total and
    /// [`SESSION_BYTES_CEILING`] however many workers or reports contribute to them, so refolding from scratch each
    /// time costs nothing a capture would notice.
    ///
    /// Also trims `per_worker` itself to what it kept: what a worker's own entry loses here is what the fold
    /// discarded from the merged view too, so total memory stays bounded by the session's own ceilings rather
    /// than growing with the fleet. Harmless to a later, fuller report from the same worker -- `Captures::record`
    /// always replaces a worker's entry outright, never appends to what recompute left behind.
    ///
    /// `truncated` only ever turns on, never off, because that trim is the reason a later fold can no longer
    /// see what an earlier one dropped: a worker sorted after the one that filled the ceiling is trimmed to
    /// nothing, and a later report replacing some *other* worker's entry would otherwise refold from a `per_worker`
    /// that no longer remembers the loss, and report the sample as whole when it is not.
    fn recompute(&mut self) {
        let mut workers: Vec<String> = self.per_worker.keys().cloned().collect();
        workers.sort();

        let mut batches: Vec<CaptureBatch> = Vec::new();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut truncated = false;
        let mut kept: HashMap<String, Vec<CaptureBatch>> = HashMap::new();
        // Rows a block holds first, from every worker, then the rest: they are what a capture of something blocked is
        // opened to see, and the total would otherwise go to whichever workers sort first.
        for held in [true, false] {
            for worker in &workers {
                let kept = kept.entry(worker.clone()).or_default();
                for sent in self.per_worker[worker].iter().filter(|b| is_held(b) == held) {
                    if truncated || rows >= self.invitation.max_matches {
                        break;
                    }
                    let mut mine = CaptureBatch::new(sent.columns.clone());
                    for row in &sent.rows {
                        if rows >= self.invitation.max_matches {
                            break;
                        }
                        let held = batches.iter().position(|b| b.columns == sent.columns);
                        // Each worker's batch keeps its own header, so each is charged once, however many share it.
                        let cost = row.size() + if mine.rows.is_empty() { mine.header_size() } else { 0 };
                        if bytes + cost > SESSION_BYTES_CEILING {
                            truncated = true;
                            break;
                        }
                        bytes += cost;
                        let held = match held {
                            Some(i) => &mut batches[i],
                            None => {
                                batches.push(CaptureBatch::new(sent.columns.clone()));
                                batches.last_mut().expect("just pushed")
                            }
                        };
                        mine.rows.push(row.clone());
                        held.rows.push(CapturedRow {
                            worker: Some(worker.clone()),
                            ..row.clone()
                        });
                        rows += 1;
                    }
                    if !mine.rows.is_empty() {
                        kept.push(mine);
                    }
                }
            }
        }
        self.per_worker = kept;
        self.batches = batches;
        self.rows = rows;
        self.bytes = bytes;
        // Sticky: this pass may no longer have the evidence an earlier one did, once that evidence is the very
        // thing the trim above discarded.
        self.truncated |= truncated;
    }

    /// A capture is over when it holds what it asked for and has heard from every worker in `holding`, when its
    /// deadline passes, when every worker it was
    /// waiting for has answered, or when whoever was watching stopped it.
    ///
    /// Each is needed. Without the deadline a fleet with one silent worker never finishes; without the answers a
    /// capture that filled instantly still makes someone wait out the clock; without the total a fleet that has
    /// sent more than was wanted keeps everyone waiting on the slowest. A capture whose group held no worker the
    /// leader had heard from ends only on the deadline, its total, or a stop -- there is nobody whose silence would
    /// end it sooner.
    ///
    /// Once over it stays over. Every clause but the total moves one way, and [`Captures::record`] folds nothing
    /// into a capture that is over, so what it holds -- and with it the total -- never changes after. The heartbeat
    /// relies on this: a worker lets go of a capture the leader stops listing, and one listed again would start over
    /// on every worker.
    fn done(&self) -> bool {
        self.stopped
            || (self.rows >= self.invitation.max_matches
                && self.holding.iter().all(|w| self.per_worker.contains_key(w)))
            || self.elapsed() >= Duration::from_secs(self.invitation.deadline_secs)
            || (!self.expected.is_empty() && self.expected.is_subset(&self.answered))
    }

    /// Who this capture is still waiting for.
    fn waiting_on(&self) -> Vec<String> {
        let mut out: Vec<String> = self.expected.difference(&self.answered).cloned().collect();
        out.sort();
        out
    }

    /// Every worker this capture expected or invited, in name order, with where each stands.
    fn workers(&self) -> Vec<CaptureWorker> {
        let mut names: Vec<&String> = self.expected.union(&self.invited).collect();
        names.sort();
        names
            .into_iter()
            .map(|worker| CaptureWorker {
                worker: worker.clone(),
                state: if self.answered.contains(worker) {
                    WorkerState::Answered
                } else if self.per_worker.contains_key(worker) {
                    WorkerState::Filling
                } else {
                    WorkerState::Waiting
                },
            })
            .collect()
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn seconds_left(&self) -> u64 {
        Duration::from_secs(self.invitation.deadline_secs)
            .saturating_sub(self.elapsed())
            .as_secs()
    }

    fn status(&self, session: &str) -> CaptureStatus {
        CaptureStatus {
            session: session.to_string(),
            destination: self.invitation.destination.clone(),
            done: self.done(),
            stopped: self.stopped,
            max_matches: self.invitation.max_matches,
            workers: self.workers(),
            batches: self.batches.clone(),
            ended: self.ended.clone(),
            seconds_left: self.seconds_left(),
            waiting_on: self.waiting_on(),
            truncated: self.truncated,
            sql: self.sql.clone(),
            avro: self.avro.clone(),
        }
    }
}

/// Whether a worker's batch holds rows a block keeps: [`crate::worker::capture::EXCEPTION_MESSAGE`] leads them.
fn is_held(batch: &CaptureBatch) -> bool {
    batch
        .columns
        .first()
        .is_some_and(|c| c.name == crate::worker::capture::EXCEPTION_MESSAGE)
}

/// How many rows `batches` hold in all.
fn rows_in(batches: &[CaptureBatch]) -> usize {
    batches.iter().map(|b| b.rows.len()).sum()
}

/// Every capture the leader is running.
#[derive(Default)]
pub struct Captures(Mutex<HashMap<String, Session>>);

impl Captures {
    /// Open a session under `id` and answer the invitation workers will be handed.
    ///
    /// `expected` is the workers in this capture's group the leader has heard from: who it waits for before calling
    /// itself finished. `holding` is those of them holding rows a block keeps, which its total waits for. `secrets`
    /// is what this source's own credentials resolved to, which the caller reads out of the store once here rather
    /// than on every heartbeat that hands the invitation on. `config` is the commit the fleet ran when it opened, for
    /// a sample saved from it to record.
    pub fn open(
        &self,
        id: String,
        request: CaptureRequest,
        expected: HashSet<String>,
        holding: HashSet<String>,
        secrets: Vec<crate::heartbeat::SecretDelivery>,
        config: Option<String>,
    ) -> CaptureInvitation {
        let one_worker = request.source.as_ref().is_some_and(|s| s.kind.single_worker());
        let invitation = CaptureInvitation {
            session: id.clone(),
            group: request.group,
            source: request.source,
            destination: request.destination,
            identity: request.identity,
            filter: request.filter,
            // The latest is where a capture reads from without one, so it is no start of its own.
            start: request
                .start
                .filter(|s| *s != crate::config_file::StartPositionDto::Latest),
            // A capture ends as a sample, so it asks for no more than one holds.
            max_matches: request
                .max_matches
                .unwrap_or(DEFAULT_MAX_MATCHES)
                .clamp(1, crate::config_file::MAX_SAMPLE_RECORDS),
            max_bytes: request
                .max_bytes
                .unwrap_or(DEFAULT_MAX_BYTES)
                .clamp(1, MAX_BYTES_CEILING),
            deadline_secs: request
                .deadline_secs
                .unwrap_or(DEFAULT_DEADLINE_SECS)
                .clamp(1, MAX_DEADLINE_SECS),
            secrets,
        };
        self.0.lock().unwrap().insert(
            id,
            Session {
                stopped: false,
                invitation: invitation.clone(),
                started: Instant::now(),
                opened_micros: crate::core::ctx::wall_clock_micros(),
                config,
                expected,
                holding,
                invited: HashSet::new(),
                answered: HashSet::new(),
                per_worker: HashMap::new(),
                batches: Vec::new(),
                rows: 0,
                bytes: 0,
                truncated: false,
                ended: Vec::new(),
                one_worker,
                sql: None,
                avro: Vec::new(),
            },
        );
        invitation
    }

    /// What `worker_id`, reporting `group`, should run: every capture the leader still wants from it, whole on every
    /// answer. A worker lets go of a session still filling that this leaves out, so a session leaves it only once
    /// the leader wants nothing more from this worker for it -- the capture is over, this worker has answered, or it
    /// was saved, discarded or swept -- and never comes back: `done` never falls, a worker once answered stays
    /// answered, a closed session is gone for good, and a session id is never reused.
    ///
    /// Only captures opened against that group -- minimization, as with secrets (design doc, Secrets boundary): a
    /// source's configuration goes to the workers of the group that runs it.
    ///
    /// A worker that has already answered is not invited again: the session is finished with it, and re-inviting
    /// would start a second capture on the same worker for the same session.
    ///
    /// A single-worker source is captured by one worker: the one `holder` names as holding it in the group, whose
    /// deployment reads it, or the first to ask where none does.
    pub fn invitations_for(
        &self,
        worker_id: &str,
        group: &str,
        holder: impl Fn(&str) -> Option<String>,
    ) -> Vec<CaptureInvitation> {
        let mut sessions = self.0.lock().unwrap();
        let mut out = Vec::new();
        for session in sessions.values_mut() {
            if session.invitation.group != group || session.done() || session.answered.contains(worker_id) {
                continue;
            }
            if session.one_worker && !session.invited.contains(worker_id) {
                let source = session.invitation.source.as_ref().map_or("", |s| s.id.as_str());
                if !session.invited.is_empty() || holder(source).is_some_and(|h| h != worker_id) {
                    continue;
                }
                session.expected = HashSet::from([worker_id.to_string()]);
            }
            session.invited.insert(worker_id.to_string());
            out.push(session.invitation.clone());
        }
        out
    }

    /// Merge one worker's report, progress or final, while the capture runs. Once it is over what it holds is final,
    /// and a report only notes why its worker stopped.
    ///
    /// Unknown, un-invited, or already-answered sessions are dropped: a report for a capture nobody is watching any
    /// more is not an error, it is late, and a worker that has already answered has nothing left to say -- a report
    /// crossing that one in flight changes nothing.
    ///
    /// A worker that was never invited is refused. It learns a session id only by being handed one, so a report
    /// from anyone else is not a late answer -- it is records the leader never asked for, from a credential that
    /// has no part in this capture.
    pub fn record(&self, report: CaptureReport) {
        let mut sessions = self.0.lock().unwrap();
        let Some(session) = sessions.get_mut(&report.session) else {
            return;
        };
        if !session.invited.contains(&report.worker_id) || session.answered.contains(&report.worker_id) {
            return;
        }
        // Checked before this report's own answer is noted, so the final that completes the capture is folded.
        // Once over, what it holds is what whoever watched it was last shown, and what a save commits.
        if !session.done() {
            // Replaces this worker's last report rather than adding to it: `report.batches` is always everything
            // that worker has caught so far, so the newest report is the only one worth folding in -- except when
            // it is smaller than what this worker already gave. A runtime releases its session once it writes a
            // report to its own pipe, not once the leader has it (`docs/status.md`), so a crash between the two can
            // see the leader re-invite a worker that already answered in part; the replacement starts filling from
            // nothing, and its first reports must not erase what the leader already holds from the attempt before
            // it.
            let grows = session
                .per_worker
                .get(&report.worker_id)
                .is_none_or(|held| rows_in(&report.batches) >= rows_in(held));
            if grows {
                session.per_worker.insert(report.worker_id.clone(), report.batches);
            }
            if report.sql.is_some() {
                session.sql = report.sql;
            }
            if !report.avro.is_empty() {
                session.avro = report.avro;
            }
            session.recompute();
        }
        if let Some(reason) = report.finished {
            session.answered.insert(report.worker_id.clone());
            session.ended.push((report.worker_id, reason));
        }
    }

    pub fn status(&self, session: &str) -> Option<CaptureStatus> {
        self.0.lock().unwrap().get(session).map(|s| s.status(session))
    }

    /// How `session` was taken, as the `__capture` fields a sample saved from it carries on every record: when it
    /// opened, what it read, in which group, under which filter, on which commit, and each worker that failed with
    /// why. A field with nothing to say is left out.
    pub fn capture_fields(&self, session: &str) -> Vec<(crate::heartbeat::CaptureColumn, serde_json::Value)> {
        let sessions = self.0.lock().unwrap();
        let Some(s) = sessions.get(session) else {
            return Vec::new();
        };
        let mut timestamp = Vec::new();
        crate::ops::datetime::write_iso8601_micros_bare(s.opened_micros, &mut timestamp);
        let timestamp = String::from_utf8(timestamp).expect("an instant is written as ASCII");
        let failed: Vec<serde_json::Value> = s
            .ended
            .iter()
            .filter_map(|(worker, end)| match end {
                CaptureEnd::Failed(reason) => Some(serde_json::json!({"worker": worker, "reason": reason})),
                _ => None,
            })
            .collect();
        use crate::core::column::DataType;
        let text = |v: &str| serde_json::Value::String(v.to_string());
        let failure = DataType::Struct(
            vec![
                ("worker".to_string(), DataType::Str),
                ("reason".to_string(), DataType::Str),
            ],
            false,
        );
        [
            ("timestamp", DataType::DateTime, Some(text(&timestamp))),
            (
                "source",
                DataType::Str,
                s.invitation.source.as_ref().map(|source| text(&source.id)),
            ),
            (
                "destination",
                DataType::Str,
                s.invitation.destination.as_deref().map(text),
            ),
            ("group", DataType::Str, Some(text(&s.invitation.group))),
            ("filter", DataType::Str, s.invitation.filter.as_deref().map(text)),
            ("config", DataType::Str, s.config.as_deref().map(text)),
            (
                "failed",
                DataType::Array(Box::new(failure)),
                (!failed.is_empty()).then_some(serde_json::Value::Array(failed)),
            ),
        ]
        .into_iter()
        .filter_map(|(name, ty, value)| {
            let column = crate::heartbeat::CaptureColumn {
                name: format!("__capture.{name}"),
                ty: ty.spell(),
            };
            value.map(|v| (column, v))
        })
        .collect()
    }

    /// The group `session` was opened against, which is the area whoever follows it must edit.
    pub fn group(&self, session: &str) -> Option<String> {
        self.0.lock().unwrap().get(session).map(|s| s.invitation.group.clone())
    }

    /// End a capture still running where it stands. What it holds is what it will ever hold: records that land
    /// after this are dropped, a worker not yet invited never is, and every worker still running it lets go once
    /// its next heartbeat's answer no longer lists it -- a supervisor's own capture on that beat, a runtime's on the
    /// deployer's next turn. A capture already over is answered as it ended.
    pub fn stop(&self, session: &str) -> Option<CaptureStatus> {
        let mut sessions = self.0.lock().unwrap();
        let s = sessions.get_mut(session)?;
        if !s.done() {
            s.stopped = true;
        }
        Some(s.status(session))
    }

    /// Forget a session. Called when its records have been saved as a sample, or when whoever started it gave up.
    pub fn close(&self, session: &str) -> Option<CaptureStatus> {
        let mut sessions = self.0.lock().unwrap();
        sessions.remove(session).map(|s| s.status(session))
    }

    /// Drop every session that finished more than `keep` ago. A capture nobody collected must not hold its records
    /// forever: this is the only thing that bounds the leader's memory across sessions.
    pub fn sweep(&self, keep: Duration) {
        let mut sessions = self.0.lock().unwrap();
        sessions.retain(|_, s| !s.done() || s.elapsed() < Duration::from_secs(s.invitation.deadline_secs) + keep);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::{SourceDto, SourceKindDto};

    fn expect(workers: &[&str]) -> HashSet<String> {
        workers.iter().map(|w| w.to_string()).collect()
    }

    fn request(deadline_secs: Option<u64>) -> CaptureRequest {
        CaptureRequest {
            group: "prod-west".to_string(),
            source: Some(SourceDto {
                id: "orders".to_string(),
                kind: SourceKindDto::Kafka {
                    schema_registry: None,
                    read: Default::default(),
                    brokers: "b1:9092".to_string(),
                    topics: vec!["orders".to_string()],
                    group_id: "production".to_string(),
                    identity: None,
                    start: Default::default(),
                    stop: None,
                    fetch_bytes: Default::default(),
                    encoding: Default::default(),
                    props: Default::default(),
                },
            }),
            destination: None,
            identity: None,
            filter: None,
            start: None,
            max_matches: None,
            max_bytes: None,
            deadline_secs,
        }
    }

    fn report(session: &str, worker: &str, raws: &[&str]) -> CaptureReport {
        CaptureReport {
            finished: Some(CaptureEnd::Matches),
            ..progress_report(session, worker, raws)
        }
    }

    /// A worker still filling, reporting what it has caught so far -- `finished: None`.
    fn progress_report(session: &str, worker: &str, raws: &[&str]) -> CaptureReport {
        CaptureReport {
            session: session.to_string(),
            worker_id: worker.to_string(),
            batches: vec![CaptureBatch {
                columns: vec![crate::heartbeat::CaptureColumn {
                    name: "_raw".to_string(),
                    ty: "str".to_string(),
                }],
                rows: raws
                    .iter()
                    .map(|raw| CapturedRow {
                        values: vec![serde_json::Value::from(*raw)],
                        ..CapturedRow::default()
                    })
                    .collect(),
            }],
            finished: None,
            sql: None,
            avro: Vec::new(),
        }
    }

    /// Each worker a status names, with where it stands.
    fn workers(status: &CaptureStatus) -> Vec<(&str, WorkerState)> {
        status.workers.iter().map(|w| (w.worker.as_str(), w.state)).collect()
    }

    /// Each record a status holds, as the worker that sent it and its text.
    fn held(status: &CaptureStatus) -> Vec<(&str, &str)> {
        status
            .batches
            .iter()
            .flat_map(|b| &b.rows)
            .map(|r| (r.worker.as_deref().unwrap(), r.values[0].as_str().unwrap()))
            .collect()
    }

    /// How many records a status holds.
    fn count(status: &CaptureStatus) -> usize {
        rows_in(&status.batches)
    }

    /// A sample saved from a capture records how it was taken: what it read, in which group, under which filter, on
    /// which commit, and each worker that failed with why.
    #[test]
    fn a_capture_says_how_it_was_taken() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                filter: Some("_raw.includes(\"x\")".to_string()),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            Some("54bf84a1".to_string()),
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);
        captures.record(report("s1", "w1", &["a"]));
        captures.record(CaptureReport {
            finished: Some(CaptureEnd::Failed("the broker refused the credential".to_string())),
            ..progress_report("s1", "w2", &[])
        });

        let taken = captures.capture_fields("s1");
        let field = |name: &str| {
            taken
                .iter()
                .find(|(c, _)| c.name == format!("__capture.{name}"))
                .map(|(_, v)| v.clone())
        };
        let timestamp = field("timestamp").unwrap();
        let timestamp = timestamp.as_str().unwrap();
        assert!(timestamp.ends_with('Z') && timestamp.contains('T'), "{timestamp}");
        assert_eq!(field("source"), Some(serde_json::json!("orders")));
        assert_eq!(field("destination"), None);
        assert_eq!(field("group"), Some(serde_json::json!("prod-west")));
        assert_eq!(field("filter"), Some(serde_json::json!("_raw.includes(\"x\")")));
        assert_eq!(field("config"), Some(serde_json::json!("54bf84a1")));
        assert_eq!(
            field("failed"),
            Some(serde_json::json!([{"worker": "w2", "reason": "the broker refused the credential"}])),
            "a worker that finished is not a failure"
        );
        let columns: Vec<_> = taken.into_iter().map(|(c, _)| c).collect();
        let meta =
            crate::plan::topology::SourceMeta::of_columns(&columns).expect("a sample reads its `__capture` columns");
        assert!(
            meta.record()
                .iter()
                .any(|(name, _)| name.as_ref() == "__capture.config"),
            "a pipeline reads them"
        );
    }

    /// A capture asks for one total. Each worker is handed that number as its own bound, and the leader stops
    /// folding at it whoever sent what: the fleet may send more, and what it holds is what was asked for.
    #[test]
    fn a_capture_holds_what_it_asked_for_and_is_done() {
        let captures = Captures::default();
        let invitation = captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(3),
                ..request(Some(60))
            },
            expect(&["w1", "w2", "w3"]),
            Default::default(),
            Vec::new(),
            None,
        );
        assert_eq!(invitation.max_matches, 3, "each worker may fill it alone");
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);

        captures.record(progress_report("s1", "w1", &["a", "b"]));
        let status = captures.status("s1").unwrap();
        assert!(!status.done, "two of three, and nobody has answered");

        captures.record(progress_report("s1", "w2", &["c", "d"]));
        let status = captures.status("s1").unwrap();
        assert_eq!(count(&status), 3, "what was asked for, not what was sent");
        assert!(status.done, "holding the total finishes it, with w3 never heard from");
        assert!(!status.truncated, "the fourth was never wanted, so nothing was lost");
        assert_eq!(
            held(&status).iter().map(|(w, _)| *w).collect::<Vec<_>>(),
            vec!["w1", "w1", "w2"],
            "each record says who sent it"
        );
        assert!(
            captures.invitations_for("w3", "prod-west", |_| None).is_empty(),
            "a finished capture invites nobody else"
        );
    }

    /// The fields of the Avro schema a worker read with are the capture's to show, and a report without them keeps
    /// those already shown.
    #[test]
    fn a_capture_shows_the_fields_of_the_avro_schema_read() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);
        let fields = vec![crate::heartbeat::AvroField {
            name: "id".to_string(),
            avro_type: "long".to_string(),
            suggested: "i64".to_string(),
        }];
        captures.record(CaptureReport {
            avro: fields.clone(),
            ..progress_report("s1", "w1", &["a"])
        });
        captures.record(progress_report("s1", "w2", &["b"]));
        assert_eq!(captures.status("s1").unwrap().avro, fields);
    }

    /// A source one worker reads at a time is captured by one worker too: the first to ask, which is then the only
    /// one the capture waits for. What Run found out about its query is the capture's to show.
    #[test]
    fn a_single_worker_source_is_captured_by_the_first_worker_to_ask() {
        let captures = Captures::default();
        let source: SourceDto = serde_json::from_value(serde_json::json!({
            "id": "pg", "type": "postgres", "host": "db", "database": "auth", "user": "r",
            "query": "SELECT id FROM t", "mode": {"type": "everyPoll"}, "fields": []
        }))
        .unwrap();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                source: Some(source),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        assert_eq!(captures.invitations_for("w2", "prod-west", |_| None).len(), 1);
        assert!(
            captures.invitations_for("w1", "prod-west", |_| None).is_empty(),
            "w2 reads it"
        );
        assert_eq!(captures.status("s1").unwrap().waiting_on, ["w2"]);

        let run = crate::heartbeat::SqlRun {
            columns: vec![crate::heartbeat::SqlColumn {
                name: "id".to_string(),
                database_type: "int8".to_string(),
                suggested: "i64".to_string(),
                table: Some("public.t".to_string()),
                unique: true,
            }],
            verdict: None,
        };
        captures.record(CaptureReport {
            sql: Some(run.clone()),
            ..report("s1", "w2", &["1"])
        });
        let status = captures.status("s1").unwrap();
        assert!(status.done, "the one worker it waits for has answered");
        assert_eq!(status.sql, Some(run));
    }

    /// A deployed single-worker source is read only by the worker that holds it, so that worker captures it, whoever
    /// asks first.
    #[test]
    fn a_held_source_is_captured_by_its_holder() {
        let captures = Captures::default();
        let source: SourceDto = serde_json::from_value(serde_json::json!({
            "id": "pg", "type": "postgres", "host": "db", "database": "auth", "user": "r",
            "query": "SELECT id FROM t", "mode": {"type": "everyPoll"}, "fields": []
        }))
        .unwrap();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                source: Some(source),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        let held_by_w1 = |source: &str| (source == "pg").then(|| "w1".to_string());
        assert!(
            captures.invitations_for("w2", "prod-west", held_by_w1).is_empty(),
            "w2 does not hold it"
        );
        assert_eq!(captures.invitations_for("w1", "prod-west", held_by_w1).len(), 1);
        assert_eq!(captures.status("s1").unwrap().waiting_on, ["w1"]);
    }

    /// Stopped is over where it stands. Records that land after it are dropped, a late final still says why its
    /// worker stopped, and a worker that had not been invited yet never is.
    #[test]
    fn a_stopped_capture_keeps_what_it_had_and_drops_what_comes_after() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.record(progress_report("s1", "w1", &["a"]));
        assert!(!captures.status("s1").unwrap().done);

        let stopped = captures.stop("s1").expect("open");
        assert!(stopped.done && stopped.stopped);
        assert_eq!(count(&stopped), 1);
        assert_eq!(
            stopped.waiting_on,
            vec!["w1".to_string(), "w2".to_string()],
            "still says who never answered: w1 was filling, w2 was never heard from"
        );

        captures.record(report("s1", "w1", &["a", "b", "c"]));
        let status = captures.status("s1").unwrap();
        assert_eq!(count(&status), 1, "late records are dropped");
        assert_eq!(
            status.ended,
            vec![("w1".to_string(), CaptureEnd::Matches)],
            "the late final still says why w1 stopped"
        );
        assert_eq!(status.waiting_on, vec!["w2".to_string()], "and w1 is no longer owed");
        assert!(
            captures.invitations_for("w2", "prod-west", |_| None).is_empty(),
            "and nobody new is asked"
        );
        assert!(captures.stop("nope").is_none());
    }

    /// A capture that holds its total is over, and what it holds is final: a later report folds nothing, even one
    /// from a worker that sorts first, and a final only says why its worker stopped.
    #[test]
    fn a_capture_that_holds_its_total_takes_nothing_more() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(2),
                ..request(Some(60))
            },
            expect(&["w1", "w2", "w3"]),
            Default::default(),
            Vec::new(),
            None,
        );
        for worker in ["w1", "w2", "w3"] {
            captures.invitations_for(worker, "prod-west", |_| None);
        }

        captures.record(progress_report("s1", "w2", &["c", "d"]));
        assert!(captures.status("s1").unwrap().done, "it holds its total");

        captures.record(progress_report("s1", "w1", &["a"]));
        let status = captures.status("s1").unwrap();
        assert_eq!(
            held(&status),
            vec![("w2", "c"), ("w2", "d")],
            "what the capture showed when it finished is what it holds"
        );

        captures.record(report("s1", "w3", &["z"]));
        let status = captures.status("s1").unwrap();
        assert_eq!(
            held(&status),
            vec![("w2", "c"), ("w2", "d")],
            "a final folds nothing either"
        );
        assert_eq!(status.ended, vec![("w3".to_string(), CaptureEnd::Matches)]);
        assert_eq!(status.waiting_on, vec!["w1".to_string(), "w2".to_string()]);
    }

    /// A worker's report of the records a block holds: each under `__exception.message`, `why`.
    fn held_report(session: &str, worker: &str, why: &str) -> CaptureReport {
        let mut report = progress_report(session, worker, &["blocked"]);
        let batch = &mut report.batches[0];
        batch.columns.insert(
            0,
            crate::heartbeat::CaptureColumn {
                name: crate::worker::capture::EXCEPTION_MESSAGE.to_string(),
                ty: "str".to_string(),
            },
        );
        batch.rows[0].values.insert(0, serde_json::Value::from(why));
        report
    }

    /// Rows a block holds take the total before any other worker's, whatever the workers' names: they are what a
    /// capture of something blocked is opened to see.
    #[test]
    fn held_rows_take_the_total_first() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(2),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            expect(&["w2"]),
            Vec::new(),
            None,
        );
        for worker in ["w1", "w2"] {
            captures.invitations_for(worker, "prod-west", |_| None);
        }

        captures.record(held_report("s1", "w2", "refused"));
        captures.record(progress_report("s1", "w1", &["a", "b"]));
        let status = captures.status("s1").unwrap();
        assert!(status.done);
        assert_eq!(held(&status), vec![("w2", "refused"), ("w1", "a")]);
    }

    /// A capture's total does not end it before every worker holding rows for it has reported: whichever worker's
    /// heartbeat lands first would otherwise fill it.
    #[test]
    fn a_capture_waits_for_the_workers_holding_rows_before_its_total_ends_it() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(2),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            expect(&["w2"]),
            Vec::new(),
            None,
        );
        for worker in ["w1", "w2"] {
            captures.invitations_for(worker, "prod-west", |_| None);
        }

        captures.record(progress_report("s1", "w1", &["a", "b"]));
        assert!(
            !captures.status("s1").unwrap().done,
            "w2 holds rows and has not reported"
        );

        captures.record(held_report("s1", "w2", "refused"));
        let status = captures.status("s1").unwrap();
        assert!(status.done);
        assert_eq!(held(&status), vec![("w2", "refused"), ("w1", "a")]);
    }

    /// Once over, a capture stays over. A report that would push the fold past the byte ceiling and leave it short
    /// of its total is not folded, so the capture never reads as running again and invites nobody.
    #[test]
    fn a_finished_capture_stays_finished_when_a_later_report_would_cross_the_byte_ceiling() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(2),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);

        captures.record(progress_report("s1", "w2", &["x", "y"]));
        assert!(captures.status("s1").unwrap().done, "it holds its total");

        let whole_ceiling = "a".repeat(SESSION_BYTES_CEILING);
        captures.record(progress_report("s1", "w1", &[whole_ceiling.as_str()]));
        let status = captures.status("s1").unwrap();
        assert!(status.done, "still over");
        assert_eq!(held(&status), vec![("w2", "x"), ("w2", "y")]);
        assert!(!status.truncated, "nothing it holds was dropped");
        assert!(
            captures.invitations_for("w3", "prod-west", |_| None).is_empty(),
            "a finished capture invites nobody"
        );
    }

    /// Stop ends a capture still running. One already over is answered as it ended, and a final that lands after
    /// still says why its worker stopped.
    #[test]
    fn stopping_a_finished_capture_leaves_it_finished() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(1),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);
        captures.record(progress_report("s1", "w1", &["a"]));

        let status = captures.stop("s1").expect("open");
        assert!(status.done && !status.stopped, "it finished on its total, and says so");

        captures.record(report("s1", "w1", &["a"]));
        assert_eq!(
            captures.status("s1").unwrap().ended,
            vec![("w1".to_string(), CaptureEnd::Matches)]
        );
    }

    /// Every worker a capture expected or invited is named, with where it stands. A worker whose records the fold
    /// pushed out is still filling, and a worker invited after the capture opened is named too.
    #[test]
    fn every_worker_in_a_capture_is_named_with_where_it_stands() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(2),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        for worker in ["w2", "w1", "w3"] {
            captures.invitations_for(worker, "prod-west", |_| None);
        }

        captures.record(progress_report("s1", "w2", &["x"]));
        captures.record(progress_report("s1", "w1", &["a", "b"]));
        let status = captures.status("s1").unwrap();
        assert!(status.done, "it holds its total");
        assert_eq!(
            workers(&status),
            vec![
                ("w1", WorkerState::Filling),
                ("w2", WorkerState::Filling),
                ("w3", WorkerState::Waiting),
            ]
        );
        assert!(
            held(&status).iter().all(|(w, _)| *w == "w1"),
            "w2 is filling with nothing held: the fold kept w1's"
        );

        captures.record(report("s1", "w3", &[]));
        let status = captures.status("s1").unwrap();
        assert_eq!(
            workers(&status),
            vec![
                ("w1", WorkerState::Filling),
                ("w2", WorkerState::Filling),
                ("w3", WorkerState::Answered),
            ]
        );
    }

    /// A capture that is over, or gone, is listed to nobody. Its absence from the heartbeat's answer is what tells a
    /// worker to let go.
    #[test]
    fn a_capture_that_is_over_is_listed_to_nobody() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        assert_eq!(captures.invitations_for("w1", "prod-west", |_| None).len(), 1);
        captures.stop("s1");
        assert!(
            captures.invitations_for("w1", "prod-west", |_| None).is_empty(),
            "the worker running it is not asked again"
        );
        assert!(
            captures.invitations_for("w9", "prod-west", |_| None).is_empty(),
            "and nobody new is asked"
        );

        captures.open(
            "s2".to_string(),
            request(Some(60)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        let listed: Vec<String> = captures
            .invitations_for("w1", "prod-west", |_| None)
            .into_iter()
            .map(|i| i.session)
            .collect();
        assert_eq!(listed, vec!["s2".to_string()]);
        captures.close("s2");
        assert!(captures.invitations_for("w1", "prod-west", |_| None).is_empty());
        assert!(captures.invitations_for("w9", "prod-west", |_| None).is_empty());
    }

    /// Every worker answers, and the leader merges them into one set of records.
    #[test]
    fn a_capture_merges_every_workers_answer() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );

        assert_eq!(captures.invitations_for("w1", "prod-west", |_| None).len(), 1);
        assert_eq!(captures.invitations_for("w2", "prod-west", |_| None).len(), 1);
        captures.record(report("s1", "w1", &["a", "b"]));
        assert!(!captures.status("s1").unwrap().done, "w2 has not answered");

        captures.record(report("s1", "w2", &["c"]));
        let status = captures.status("s1").unwrap();
        assert!(status.done, "every invited worker answered");
        assert_eq!(count(&status), 3);
        assert_eq!(
            workers(&status),
            vec![("w1", WorkerState::Answered), ("w2", WorkerState::Answered)]
        );
    }

    /// A worker that has answered is not invited again: it is finished with this session, and a second invitation
    /// would start a second capture under the same id.
    #[test]
    fn a_worker_that_answered_is_not_invited_again() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.record(report("s1", "w1", &["a"]));
        assert!(captures.invitations_for("w1", "prod-west", |_| None).is_empty());
    }

    /// A second report from one worker is dropped rather than counted twice: an invitation repeats on every
    /// heartbeat, so a report crossing one in flight is ordinary.
    #[test]
    fn a_repeated_report_is_not_merged_twice() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.record(report("s1", "w1", &["a"]));
        captures.record(report("s1", "w1", &["a"]));
        let status = captures.status("s1").unwrap();
        assert_eq!(count(&status), 1);
        assert_eq!(status.ended.len(), 1, "the repeat is not a second answer");
    }

    /// A worker still filling shows up in the sample as it goes: a progress report merges the same way a final
    /// one does, and does not answer the capture.
    #[test]
    fn a_progress_report_grows_the_sample_without_finishing_it() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);

        captures.record(progress_report("s1", "w1", &["a"]));
        let status = captures.status("s1").unwrap();
        assert_eq!(count(&status), 1);
        assert!(!status.done, "a progress report is not an answer");
        assert_eq!(workers(&status), vec![("w1", WorkerState::Filling)]);
    }

    /// A worker's later report -- progress or final -- carries everything it has caught, not just what is new, so
    /// the leader replaces rather than appends: two reports of a growing list must not double what it already had.
    #[test]
    fn a_later_report_replaces_a_workers_earlier_one_rather_than_adding_to_it() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);

        captures.record(progress_report("s1", "w1", &["a"]));
        captures.record(progress_report("s1", "w2", &["x"]));
        captures.record(progress_report("s1", "w1", &["a", "b", "c"]));
        assert_eq!(
            count(&captures.status("s1").unwrap()),
            4,
            "w1's three plus w2's one, not w1's four counted twice"
        );

        captures.record(report("s1", "w1", &["a", "b", "c"]));
        let status = captures.status("s1").unwrap();
        assert_eq!(count(&status), 4, "the final report named the same records again");
        assert!(!status.done, "w2 has not answered yet");
        assert_eq!(status.waiting_on, vec!["w2".to_string()]);
    }

    /// A worker that has answered is done talking: a progress report arriving after its final one -- offered late,
    /// or crossing it in flight -- changes nothing.
    #[test]
    fn a_progress_report_after_a_worker_has_answered_is_ignored() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);

        captures.record(report("s1", "w1", &["a", "b"]));
        captures.record(progress_report("s1", "w1", &["a", "b", "c"]));

        let status = captures.status("s1").unwrap();
        assert_eq!(
            count(&status),
            2,
            "the late progress report did not reopen what had already answered"
        );
        assert!(status.done);
    }

    /// A restarted runtime re-admits its session fresh and starts filling from nothing: its own early reports
    /// must not erase what an earlier attempt already gave the leader before it crashed.
    #[test]
    fn a_smaller_report_never_erases_what_a_worker_already_gave() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);

        captures.record(progress_report("s1", "w1", &["a", "b", "c"]));
        assert_eq!(count(&captures.status("s1").unwrap()), 3);

        captures.record(progress_report("s1", "w1", &["x"]));
        assert_eq!(
            count(&captures.status("s1").unwrap()),
            3,
            "the smaller report from the restarted worker did not erase the earlier attempt's records"
        );
    }

    /// `per_worker` holds only what `recompute` kept, so a fleet's reports cost the leader one session's bound
    /// however many workers send them.
    #[test]
    fn a_workers_stored_copy_is_trimmed_to_what_the_fold_kept() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(3),
                ..request(Some(60))
            },
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);

        captures.record(progress_report("s1", "w1", &["a", "b"]));
        captures.record(progress_report("s1", "w2", &["c", "d"]));
        assert!(captures.status("s1").unwrap().done, "it holds its total");

        let sessions = captures.0.lock().unwrap();
        let session = sessions.get("s1").unwrap();
        assert_eq!(rows_in(&session.per_worker["w1"]), 2, "all of w1's report fit");
        assert_eq!(
            rows_in(&session.per_worker["w2"]),
            1,
            "w2 keeps only the record that reached the total, and what was past it is gone from memory too"
        );
    }

    /// `truncated` must not clear once a later report's own recompute no longer has the evidence: the worker
    /// that got cut was already trimmed to nothing, so a later fold over it alone finds nothing to trip the
    /// ceiling and would otherwise conclude, wrongly, that nothing was ever dropped.
    #[test]
    fn truncated_stays_true_after_a_later_report_from_an_unrelated_worker() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);

        // Records sized so that w1's report fills the byte ceiling exactly: nothing of w1's is past it, and w2's
        // one record is.
        // The batch's one column costs its name and type once; the last record takes up what that leaves.
        let big = "x".repeat(SESSION_BYTES_CEILING / 16);
        let last = "x".repeat(SESSION_BYTES_CEILING / 16 - "_raw".len() - "str".len());
        let refs: Vec<&str> = (0..15).map(|_| big.as_str()).chain([last.as_str()]).collect();
        captures.record(progress_report("s1", "w1", &refs));
        assert!(
            !captures.status("s1").unwrap().truncated,
            "w1's records fit the ceiling exactly"
        );
        captures.record(report("s1", "w2", &["z"]));
        assert!(captures.status("s1").unwrap().truncated, "w2's record did not fit");

        // Sixteen held of a hundred, and w1 still owed: the capture runs. w1 answers with what it already had --
        // nothing new, and it fits whole. w2's own entry is already empty from the earlier trim, so this fold finds
        // nothing past the ceiling.
        captures.record(report("s1", "w1", &refs));
        assert!(
            captures.status("s1").unwrap().truncated,
            "the earlier loss must stay reported even though this fold alone would not rediscover it"
        );
    }

    /// Every worker's batch keeps its own copy of the columns, so a wide header two workers share counts twice
    /// against the ceiling, not once.
    #[test]
    fn each_workers_copy_of_a_shared_header_counts_against_the_ceiling() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);
        let wide = |worker: &str| {
            let mut report = report("s1", worker, &["a"]);
            report.batches[0].columns[0].name = "x".repeat(SESSION_BYTES_CEILING / 2);
            report
        };
        captures.record(wide("w1"));
        captures.record(wide("w2"));
        let status = captures.status("s1").unwrap();
        assert_eq!(held(&status), [("w1", "a")]);
        assert!(status.truncated);
    }

    /// A fleet where some worker never answers still finishes, on the deadline.
    #[test]
    fn a_capture_finishes_on_its_deadline_with_a_silent_worker() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(1)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.invitations_for("w2", "prod-west", |_| None);
        captures.record(report("s1", "w1", &["a"]));

        let status = captures.status("s1").unwrap();
        assert!(!status.done, "w2 is still owed, and the deadline has not passed");

        std::thread::sleep(Duration::from_millis(1100));
        let status = captures.status("s1").unwrap();
        assert!(status.done, "the deadline finished it");
        assert_eq!(count(&status), 1, "with what did arrive");
    }

    /// The bug this replaced: `done` keyed on the set of workers that happened to have heartbeated, so the first
    /// worker to be invited and answer finished the capture -- and `invitations_for` then skipped it forever, so a
    /// worker that had not heartbeated yet was never asked at all. The sample was silently one worker's.
    #[test]
    fn a_capture_is_not_finished_by_the_first_worker_to_answer() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1", "w2"]),
            Default::default(),
            Vec::new(),
            None,
        );

        // Only w1 has heartbeated so far, and it answers immediately.
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.record(report("s1", "w1", &["a"]));

        let status = captures.status("s1").unwrap();
        assert!(!status.done, "w2 was always expected and has not answered");
        assert_eq!(status.waiting_on, vec!["w2".to_string()]);

        // w2 heartbeats late and is still handed the invitation.
        assert_eq!(
            captures.invitations_for("w2", "prod-west", |_| None).len(),
            1,
            "a capture that is not finished still invites"
        );
        captures.record(report("s1", "w2", &["b"]));
        let status = captures.status("s1").unwrap();
        assert!(status.done);
        assert_eq!(count(&status), 2);
        assert!(status.waiting_on.is_empty());
    }

    /// A worker is handed only the captures opened against its own group.
    #[test]
    fn a_worker_is_not_invited_to_another_groups_capture() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );

        assert!(
            captures
                .invitations_for("w-elsewhere", "prod-east", |_| None)
                .is_empty(),
            "another group's worker is not invited"
        );
        assert_eq!(captures.invitations_for("w1", "prod-west", |_| None).len(), 1);
    }

    /// A capture keeps the group it was opened against for its whole life, and a closed one names none.
    #[test]
    fn a_capture_keeps_its_group() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        assert_eq!(captures.group("s1").as_deref(), Some("prod-west"));
        captures.stop("s1");
        assert_eq!(captures.group("s1").as_deref(), Some("prod-west"));
        captures.close("s1");
        assert_eq!(captures.group("s1"), None);
        assert_eq!(captures.group("unknown"), None);
    }

    /// A worker learns a session id only by being handed one, so a report from a worker nobody invited is not a late
    /// answer -- it is records the leader never asked for.
    #[test]
    fn a_report_from_a_worker_that_was_never_invited_is_refused() {
        let captures = Captures::default();
        captures.open(
            "s1".to_string(),
            request(Some(60)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.record(report("s1", "impostor", &["forged", "forged"]));

        let status = captures.status("s1").unwrap();
        assert!(count(&status) == 0, "nothing uninvited was merged");
        assert_eq!(
            workers(&status),
            vec![("w1", WorkerState::Waiting)],
            "and the impostor is nobody the capture names"
        );
    }

    /// A request past a ceiling is clamped, and the total's ceiling is what a sample holds.
    #[test]
    fn a_request_past_a_ceiling_is_clamped() {
        let captures = Captures::default();
        let invitation = captures.open(
            "s1".to_string(),
            CaptureRequest {
                max_matches: Some(usize::MAX),
                max_bytes: Some(usize::MAX),
                deadline_secs: Some(u64::MAX),
                ..request(None)
            },
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        assert_eq!(invitation.max_matches, crate::config_file::MAX_SAMPLE_RECORDS);
        assert_eq!(invitation.max_bytes, MAX_BYTES_CEILING);
        assert_eq!(invitation.deadline_secs, MAX_DEADLINE_SECS);
    }

    /// The latest is where a capture reads from with no start, so asking for it is asking for no start: the capture
    /// taps a deployment that reads the source rather than reading it again.
    #[test]
    fn a_start_at_the_latest_is_no_start() {
        use crate::config_file::StartPositionDto;
        let captures = Captures::default();
        let open = |id: &str, start| {
            captures.open(
                id.to_string(),
                CaptureRequest { start, ..request(None) },
                expect(&["w1"]),
                Default::default(),
                Vec::new(),
                None,
            )
        };
        assert_eq!(open("s1", Some(StartPositionDto::Latest)).start, None);
        assert_eq!(
            open("s2", Some(StartPositionDto::Earliest)).start,
            Some(StartPositionDto::Earliest)
        );
    }

    /// Nothing collects a capture nobody came back for, so the sweep is what stops finished sessions accumulating.
    #[test]
    fn the_sweep_drops_a_finished_session_nobody_collected() {
        let captures = Captures::default();
        // One expected worker, which answers: the session is finished and nobody ever collects it.
        captures.open(
            "s1".to_string(),
            request(Some(1)),
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        captures.invitations_for("w1", "prod-west", |_| None);
        captures.record(report("s1", "w1", &["a"]));
        assert!(captures.status("s1").unwrap().done);

        captures.sweep(Duration::from_secs(3600));
        assert!(captures.status("s1").is_some(), "still inside the keep window");

        std::thread::sleep(Duration::from_millis(1100));
        captures.sweep(Duration::from_secs(0));
        assert!(captures.status("s1").is_none(), "swept");
    }

    /// A capture at a destination says so in its status, which is how a save knows to refuse it: what a destination
    /// sends is not what a pipeline takes in.
    #[test]
    fn a_destination_capture_names_its_destination() {
        let captures = Captures::default();
        let invitation = captures.open(
            "s1".to_string(),
            CaptureRequest {
                source: None,
                destination: Some("es".to_string()),
                ..request(Some(60))
            },
            expect(&["w1"]),
            Default::default(),
            Vec::new(),
            None,
        );
        assert_eq!(invitation.destination.as_deref(), Some("es"));
        assert!(invitation.source.is_none());
        assert_eq!(captures.status("s1").unwrap().destination.as_deref(), Some("es"));
        assert!(captures.status("s1").is_some_and(|s| !s.done));
    }
}
