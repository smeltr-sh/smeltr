//! The registry's own database (`RegistryStore`): one file, one connection, separate from `leader.db`
//! (`sqlite_store::SqliteStore`) because a per-heartbeat write must never queue behind an operator's config save.
//! Two tables with opposite write patterns share this file without sharing a table (design brief for this work):
//! `latest_state`, one row per (worker, thread) upserted every beat and bounded by fleet size; and `series_points`,
//! append-only counters and gauges aggregated on arrival into fixed buckets, keyed by subject rather than by the
//! thread that reported them (E2/E6 ask about a source or a destination, not a thread -- per-thread detail is a
//! health question `latest_state` already answers). A point names its series by id; `series` holds each series'
//! metric and subject once, and loses a series when its last point expires. A third table, `fact_clock`, is the
//! same shape as `latest_state` and rides along for the same reason: `registry::Registry::observe_facts` needs its
//! start times to survive a restart too.
//!
//! **What one heartbeat costs.** One transaction ([`RegistryStore::record_heartbeat`]), whatever it carries: a
//! commit is what costs, and the statements inside it are microseconds each, so the fleet's write load follows its
//! heartbeat rate rather than its sample rate. The connection runs at synchronous NORMAL, which gives up only the
//! most recent commits after a power loss -- this file is the leader's operational view, and what belongs
//! somewhere durable leaves as data through a smeltr logs or smeltr metrics source.
//!
//! **Steps.** [`STEPS`] is the file's ordered list of forward steps, run at open (`steps`).
//!
//! **Retention.** [`Tier`] names the four windows E2 asks for: 10s buckets for 60 minutes, 1m for 24 hours, 5m for
//! 8 days, 1h for 30 days. A bucket is written only at the finest tier ([`write_point`], whether one sample seeds
//! it or a heartbeat carries hundreds); [`RegistryStore::maintain`] is what moves data outward -- a fine bucket
//! about to age out of its tier's window is summed (counter) or maxed (gauge) into the coarse bucket that covers
//! it, then deleted, so it is counted exactly once on its way through. The last tier has nowhere to roll to and
//! only expires. A bucket nothing landed in is a bucket with no row: there is nothing to promote and nothing to
//! delete, so an outage reads as absent rather than as a zero.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use rusqlite::{Connection, OptionalExtension, params};

use super::store::StoreError;

/// One fixed retention window. Ordered finest first; [`Tier::next`] is where a bucket goes once it ages out of
/// this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    TenSeconds,
    OneMinute,
    FiveMinutes,
    OneHour,
}

impl Tier {
    /// Every tier, finest first.
    pub const ALL: [Tier; 4] = [Tier::TenSeconds, Tier::OneMinute, Tier::FiveMinutes, Tier::OneHour];

    /// This tier and every finer one, finest first: a bucket this tier's width stays in the finer ones until
    /// [`RegistryStore::maintain`] moves it up, so these are where a window of this tier's buckets lies.
    pub fn and_finer(self) -> &'static [Tier] {
        &Self::ALL[..=self.code() as usize]
    }

    /// The `series_points.tier` value. A small fixed integer rather than the bucket duration itself, so a row's
    /// tier is unambiguous even though `FiveMinutes`' bucket and a hypothetical difference-shaped tier could
    /// otherwise collide.
    fn code(self) -> i64 {
        match self {
            Tier::TenSeconds => 0,
            Tier::OneMinute => 1,
            Tier::FiveMinutes => 2,
            Tier::OneHour => 3,
        }
    }

    /// This tier's bucket width -- also what a caller of [`Tier::for_window`] reports as the resolution it served
    /// (`leader::FlowResponse::bucket_millis`).
    pub fn bucket_ms(self) -> i64 {
        match self {
            Tier::TenSeconds => 10_000,
            Tier::OneMinute => 60_000,
            Tier::FiveMinutes => 5 * 60_000,
            Tier::OneHour => 60 * 60_000,
        }
    }

    /// How long a bucket stays in this tier before [`RegistryStore::maintain`] moves it on.
    fn retention_ms(self) -> i64 {
        match self {
            Tier::TenSeconds => 60 * 60_000,           // 60 minutes
            Tier::OneMinute => 24 * 60 * 60_000,       // 24 hours
            Tier::FiveMinutes => 8 * 24 * 60 * 60_000, // 8 days: `throughput` reads a week back, and an hour past it
            Tier::OneHour => 30 * 24 * 60 * 60_000,    // 30 days
        }
    }

    /// The tier a bucket promotes into once it leaves this one's window. `None` for the last tier: past 30 days a
    /// point has nowhere left to go, so it only expires.
    fn next(self) -> Option<Tier> {
        match self {
            Tier::TenSeconds => Some(Tier::OneMinute),
            Tier::OneMinute => Some(Tier::FiveMinutes),
            Tier::FiveMinutes => Some(Tier::OneHour),
            Tier::OneHour => None,
        }
    }

    /// `ms` rounded down to this tier's bucket boundary -- both where a fresh sample lands, and where an aging
    /// bucket's timestamp maps to in the next tier.
    pub fn align(self, ms: i64) -> i64 {
        ms.div_euclid(self.bucket_ms()) * self.bucket_ms()
    }

    /// The finest tier whose retention covers `window` -- the buckets E2 serves a chosen window in. The four retentions are E2's own boundaries (sixty minutes, a day, a
    /// week and the day `throughput` reads past it, thirty days), so this is a lookup, not an estimate; past the last one there is still only `OneHour` to
    /// serve from.
    pub fn for_window(window: std::time::Duration) -> Tier {
        let window_ms = window.as_millis() as i64;
        Tier::ALL
            .into_iter()
            .find(|tier| tier.retention_ms() >= window_ms)
            .unwrap_or(Tier::OneHour)
    }
}

/// One subject's points, oldest first: bucket start (epoch milliseconds) and value. What
/// [`RegistryStore::series_since`] groups every subject's rows into.
pub type SubjectPoints = Vec<(i64, f64)>;

/// One thread's `latest_state` row. `report_json` is the caller's encoding of what that thread reported
/// (`registry::Registry` uses `serde_json` over the wire `ThreadHeartbeat`); this file stores it opaquely, so a
/// field added to the wire type needs no step here.
pub struct LatestRow<'a> {
    pub thread_id: &'a str,
    /// Owned: the caller encodes it for this write and nothing else holds it, so borrowing would only cost the
    /// caller a second pass to keep the encodings alive.
    pub report_json: String,
}

/// One sample to fold into the fine bucket its heartbeat landed in. `subject_key` is the caller's
/// (`registry::subject_key`); a counter sums within the bucket and a gauge keeps its maximum.
pub struct SeriesPoint<'a> {
    pub metric: &'a str,
    /// Owned, for the reason [`LatestRow::report_json`] is.
    pub subject_key: String,
    pub counter: bool,
    pub value: f64,
}

/// What one heartbeat writes, assembled by the caller: it owns the wire types, the encoding and the subject keys,
/// and this file owns the transaction.
pub struct HeartbeatRows<'a> {
    pub worker_id: &'a str,
    pub threads: &'a [LatestRow<'a>],
    pub points: &'a [SeriesPoint<'a>],
    /// The worker process's own settings, which ride beside each report rather than inside it.
    pub stop_after_unreachable_secs: Option<u64>,
    pub commit: Option<&'a str>,
    /// The worker process's own build. Empty from a worker too old to report one.
    pub smeltr_version: &'a str,
    /// The release that build is, where it is one.
    pub release: Option<&'a str>,
    /// Owned, for the reason [`LatestRow::report_json`] is.
    pub supervising_json: Option<String>,
    /// When the authority that worker verifies the leader against expires. Mirrored with the rest, because the
    /// worker this warns about is one that can no longer reach the leader to say it again.
    pub authority_expires_millis: Option<i64>,
    /// When this heartbeat said the worker was stopping, if it did. Mirrored with the rest, because the worker it
    /// describes has stopped and will not say it again. Its `threads` are then every row the worker keeps.
    pub stopped_at_ms: Option<i64>,
    /// One "now" for the whole heartbeat: `last_seen_ms`, and the bucket every point lands in.
    pub now_ms: i64,
}

const COUNTER: &str = "counter";
const GAUGE: &str = "gauge";

/// Every step this file has taken, oldest first (`steps`). Append; never edit one that has shipped.
pub(crate) const STEPS: &[&str] = &[
    // 1 -- the schema of the first release.
    "CREATE TABLE latest_state (
         worker_id    TEXT NOT NULL,
         thread_id    TEXT NOT NULL,
         report_json  TEXT NOT NULL,
         last_seen_ms INTEGER NOT NULL,
         stop_after_unreachable_secs INTEGER,
         commit_sha   TEXT,
         -- When the worker's authority stops being valid. The warning this feeds is about a worker that cannot
         -- reach the leader, so waiting for it to say this again is waiting forever. Null from a worker on plain
         -- HTTP.
         authority_expires_ms INTEGER,
         -- The worker's supervisor state, so a leader restart does not read every worker as no longer behind its
         -- target -- which would delete the `fact_clock` start times that make such a warning age.
         supervising_json TEXT,
         -- The worker's own build, so a leader restart does not forget a mixed-version fleet.
         smeltr_version TEXT,
         -- When the worker said it was stopping, so a leader restart does not read a worker that stopped on
         -- purpose as one that failed. Null on every row of a worker that has not said it.
         stopped_at_ms INTEGER,
         PRIMARY KEY (worker_id, thread_id)
     );
     CREATE TABLE fact_clock (
         entry      TEXT NOT NULL,
         subject    TEXT NOT NULL,
         started_ms INTEGER NOT NULL,
         PRIMARY KEY (entry, subject)
     );
     -- The subject in `labels_json` carries the reporting group (`registry::subject_key`): a source or destination
     -- id is unique only within its group.
     CREATE TABLE series_points (
         tier        INTEGER NOT NULL,
         metric      TEXT NOT NULL,
         labels_json TEXT NOT NULL,
         bucket_ms   INTEGER NOT NULL,
         kind        TEXT NOT NULL,
         value       REAL NOT NULL,
         PRIMARY KEY (tier, metric, labels_json, bucket_ms)
     );
     CREATE INDEX series_points_tier_bucket ON series_points (tier, bucket_ms);
     -- `thread_name`, not `thread_id`: this holds what the OS thread is called (`smeltr-3`, `egress-1`, `main`),
     -- which is not the `reader-0` that `latest_state.thread_id` holds. One name per thing.
     --
     -- `(worker_id, boot_id, seq)` is unique because `HeartbeatClient::send` retries once, and unlike the metrics
     -- beside them these rows accumulate rather than overwrite -- which that retry's own doc comment says needs a
     -- key first.
     --
     -- `group_id` is which group said it, stored rather than joined: the worker states its group on the heartbeat
     -- that carries these, and that is a fact about the delivery. Joining `latest_state` instead would re-file
     -- every old row under a worker's current group, and answer nothing at all for a worker that never deployed.
     CREATE TABLE logs (
         id          INTEGER PRIMARY KEY AUTOINCREMENT,
         at_ms       INTEGER NOT NULL,
         worker_id   TEXT    NOT NULL,
         boot_id     INTEGER NOT NULL,
         seq         INTEGER NOT NULL,
         thread_name TEXT,
         level       TEXT    NOT NULL,
         site        TEXT    NOT NULL,
         message     TEXT    NOT NULL,
         fields_json TEXT    NOT NULL,
         group_id    TEXT
     );
     CREATE INDEX logs_at ON logs (at_ms);
     CREATE UNIQUE INDEX logs_once ON logs (worker_id, boot_id, seq);",
    // 2 -- a series' metric and labels are kept once, in `series`, and each point names its series by id. Keyed by
    // text, every point carried both twice over (once in the row, once in its key), ten times what it now costs.
    // The points the old layout held are dropped rather than carried across: this file is the leader's operational
    // view, and its charts refill as the fleet reports.
    "DROP TABLE series_points;
     CREATE TABLE series (
         id          INTEGER PRIMARY KEY,
         metric      TEXT NOT NULL,
         labels_json TEXT NOT NULL,
         kind        TEXT NOT NULL,
         UNIQUE (metric, labels_json)
     );
     CREATE TABLE series_points (
         series_id INTEGER NOT NULL REFERENCES series (id),
         tier      INTEGER NOT NULL,
         bucket_ms INTEGER NOT NULL,
         value     REAL NOT NULL,
         PRIMARY KEY (series_id, tier, bucket_ms)
     ) WITHOUT ROWID;
     CREATE INDEX series_points_tier_bucket ON series_points (tier, bucket_ms);",
    // 3 -- the release the worker's build is, beside the build itself.
    "ALTER TABLE latest_state ADD COLUMN release TEXT;",
    // 4 -- what the fleet sees of its streams (design doc, Observability: Streams), summed from every heartbeat: the
    // fields each Parser produced on one source's route, and the keys of the rows each source received that no route
    // took. Each row keeps when it was first and last seen, which is what tells a field that stopped arriving from
    // one that never did. Additive: nothing older reads it.
    "CREATE TABLE stream_parsers (
         group_id       TEXT NOT NULL,
         source         TEXT NOT NULL,
         route          TEXT NOT NULL,
         pipeline       TEXT NOT NULL,
         step           TEXT NOT NULL,
         rows           INTEGER NOT NULL,
         -- Rows of fields no row here names: past the cap, or with a name too long for the wire.
         fields_dropped INTEGER NOT NULL,
         first_seen_ms  INTEGER NOT NULL,
         last_seen_ms   INTEGER NOT NULL,
         PRIMARY KEY (group_id, source, route, pipeline, step)
     ) WITHOUT ROWID;
     CREATE TABLE stream_fields (
         group_id      TEXT NOT NULL,
         source        TEXT NOT NULL,
         route         TEXT NOT NULL,
         pipeline      TEXT NOT NULL,
         step          TEXT NOT NULL,
         field         TEXT NOT NULL,
         declared      INTEGER NOT NULL,
         suggested     TEXT NOT NULL,
         present       INTEGER NOT NULL,
         bools         INTEGER NOT NULL,
         ints          INTEGER NOT NULL,
         floats        INTEGER NOT NULL,
         strings       INTEGER NOT NULL,
         arrays        INTEGER NOT NULL,
         objects       INTEGER NOT NULL,
         failed        INTEGER NOT NULL,
         first_seen_ms INTEGER NOT NULL,
         last_seen_ms  INTEGER NOT NULL,
         PRIMARY KEY (group_id, source, route, pipeline, step, field)
     ) WITHOUT ROWID;
     CREATE TABLE stream_unmatched (
         group_id      TEXT NOT NULL,
         source        TEXT NOT NULL,
         rows          INTEGER NOT NULL,
         keys_dropped  INTEGER NOT NULL,
         first_seen_ms INTEGER NOT NULL,
         last_seen_ms  INTEGER NOT NULL,
         PRIMARY KEY (group_id, source)
     ) WITHOUT ROWID;
     CREATE TABLE stream_keys (
         group_id      TEXT NOT NULL,
         source        TEXT NOT NULL,
         key           TEXT NOT NULL,
         rows          INTEGER NOT NULL,
         first_seen_ms INTEGER NOT NULL,
         last_seen_ms  INTEGER NOT NULL,
         PRIMARY KEY (group_id, source, key)
     ) WITHOUT ROWID;
     -- The last window recorded from each worker, in the life of the process that sent it: a beat sent again
     -- carries a window this already holds, and is recorded once.
     CREATE TABLE stream_windows (
         worker_id TEXT NOT NULL PRIMARY KEY,
         boot_id   INTEGER NOT NULL,
         seq       INTEGER NOT NULL
     ) WITHOUT ROWID;",
    // 5 -- how many of a field's values read as a time, which is what proposes a field production saw as a
    // `datetime`. Zero on every field summed before it: those windows did not count.
    "ALTER TABLE stream_fields ADD COLUMN times INTEGER NOT NULL DEFAULT 0;",
    // 6 -- when a field was last present and last failed, and when its latest run of failures began, and when its
    // step last read a row: what tells a declared field that stopped arriving, or stopped coercing, from one that
    // once did (design doc, Observability: Streams). A declared field is reported on every tallied batch, present
    // or not, and a step under a guard on every batch, read or not, so `last_seen_ms` cannot. NULL on every row
    // summed before it: those windows did not say.
    "ALTER TABLE stream_parsers ADD COLUMN read_last_ms INTEGER;
     ALTER TABLE stream_fields ADD COLUMN present_last_ms INTEGER;
     ALTER TABLE stream_fields ADD COLUMN failed_last_ms INTEGER;
     ALTER TABLE stream_fields ADD COLUMN failing_since_ms INTEGER;",
    // 7 -- when a source's latest run of rows no route took began: when its unrouted-stream warning started. A run
    // ends when an hour passes with none. NULL on every row summed before it: those windows did not say.
    "ALTER TABLE stream_unmatched ADD COLUMN unrouted_since_ms INTEGER;",
    // 8 -- every metric named by the Prometheus standard: a counter's name ends in _total, and a time is in seconds,
    // named _seconds. A time's points are rescaled to seconds, so its charts keep their history.
    "UPDATE series_points SET value = value / 1000.0
         WHERE series_id IN (SELECT id FROM series WHERE metric = 'smeltr_destination_rtt_ms');
     UPDATE series_points SET value = value / 1000.0
         WHERE series_id IN (SELECT id FROM series WHERE metric = 'smeltr_destination_throttled_ms');
     UPDATE series_points SET value = value / 1000000.0
         WHERE series_id IN (SELECT id FROM series WHERE metric = 'smeltr_function_micros');
     UPDATE series_points SET value = value / 1000000.0
         WHERE series_id IN (SELECT id FROM series WHERE metric = 'smeltr_memory_abandoned_micros');
     UPDATE series_points SET value = value / 1000000.0
         WHERE series_id IN (SELECT id FROM series WHERE metric = 'smeltr_tick_flush_micros');
     UPDATE series_points SET value = value / 1000000.0
         WHERE series_id IN (SELECT id FROM series WHERE metric = 'smeltr_tick_poll_micros');
     UPDATE series_points SET value = value / 1000000.0
         WHERE series_id IN (SELECT id FROM series WHERE metric = 'smeltr_tick_process_micros');
     UPDATE series SET metric = 'smeltr_batches_dlq_total' WHERE metric = 'smeltr_batches_dlq';
     UPDATE series SET metric = 'smeltr_batches_poisoned_total' WHERE metric = 'smeltr_batches_poisoned';
     UPDATE series SET metric = 'smeltr_batches_waiting_for_room_total' WHERE metric = 'smeltr_batches_waiting_for_room';
     UPDATE series SET metric = 'smeltr_bytes_delivered_total' WHERE metric = 'smeltr_bytes_delivered';
     UPDATE series SET metric = 'smeltr_bytes_polled_total' WHERE metric = 'smeltr_bytes_polled';
     UPDATE series SET metric = 'smeltr_capture_records_total' WHERE metric = 'smeltr_capture_records';
     UPDATE series SET metric = 'smeltr_commits_total' WHERE metric = 'smeltr_commits';
     UPDATE series SET metric = 'smeltr_commits_dropped_total' WHERE metric = 'smeltr_commits_dropped';
     UPDATE series SET metric = 'smeltr_destination_rtt_seconds' WHERE metric = 'smeltr_destination_rtt_ms';
     UPDATE series SET metric = 'smeltr_destination_stalls_total' WHERE metric = 'smeltr_destination_stalls';
     UPDATE series SET metric = 'smeltr_destination_throttled_seconds_total' WHERE metric = 'smeltr_destination_throttled_ms';
     UPDATE series SET metric = 'smeltr_destination_unwritten_total' WHERE metric = 'smeltr_destination_unwritten';
     UPDATE series SET metric = 'smeltr_flushes_total' WHERE metric = 'smeltr_flushes';
     UPDATE series SET metric = 'smeltr_function_calls_total' WHERE metric = 'smeltr_function_calls';
     UPDATE series SET metric = 'smeltr_function_seconds_total' WHERE metric = 'smeltr_function_micros';
     UPDATE series SET metric = 'smeltr_logs_dropped_total' WHERE metric = 'smeltr_logs_dropped';
     UPDATE series SET metric = 'smeltr_logs_undelivered_total' WHERE metric = 'smeltr_logs_undelivered';
     UPDATE series SET metric = 'smeltr_logs_unread_total' WHERE metric = 'smeltr_logs_unread';
     UPDATE series SET metric = 'smeltr_logs_unwritable_total' WHERE metric = 'smeltr_logs_unwritable';
     UPDATE series SET metric = 'smeltr_logs_written_total' WHERE metric = 'smeltr_logs_written';
     UPDATE series SET metric = 'smeltr_memory_abandoned_seconds_total' WHERE metric = 'smeltr_memory_abandoned_micros';
     UPDATE series SET metric = 'smeltr_memory_splits_total' WHERE metric = 'smeltr_memory_splits';
     UPDATE series SET metric = 'smeltr_route_matched_total' WHERE metric = 'smeltr_route_matched';
     UPDATE series SET metric = 'smeltr_rows_delivered_total' WHERE metric = 'smeltr_rows_delivered';
     UPDATE series SET metric = 'smeltr_rows_dlq_over_budget_total' WHERE metric = 'smeltr_rows_dlq_over_budget';
     UPDATE series SET metric = 'smeltr_rows_dropped_total' WHERE metric = 'smeltr_rows_dropped';
     UPDATE series SET metric = 'smeltr_rows_errored_total' WHERE metric = 'smeltr_rows_errored';
     UPDATE series SET metric = 'smeltr_rows_in_total' WHERE metric = 'smeltr_rows_in';
     UPDATE series SET metric = 'smeltr_rows_lost_total' WHERE metric = 'smeltr_rows_lost';
     UPDATE series SET metric = 'smeltr_rows_out_total' WHERE metric = 'smeltr_rows_out';
     UPDATE series SET metric = 'smeltr_rows_polled_total' WHERE metric = 'smeltr_rows_polled';
     UPDATE series SET metric = 'smeltr_rows_rejected_total' WHERE metric = 'smeltr_rows_rejected';
     UPDATE series SET metric = 'smeltr_rows_salvaged_total' WHERE metric = 'smeltr_rows_salvaged';
     UPDATE series SET metric = 'smeltr_rows_send_failed_total' WHERE metric = 'smeltr_rows_send_failed';
     UPDATE series SET metric = 'smeltr_rows_sent_total' WHERE metric = 'smeltr_rows_sent';
     UPDATE series SET metric = 'smeltr_rows_unmatched_total' WHERE metric = 'smeltr_rows_unmatched';
     UPDATE series SET metric = 'smeltr_rows_unrouted_total' WHERE metric = 'smeltr_rows_unrouted';
     UPDATE series SET metric = 'smeltr_rows_without_doc_id_total' WHERE metric = 'smeltr_rows_without_doc_id';
     UPDATE series SET metric = 'smeltr_source_dropped_total' WHERE metric = 'smeltr_source_dropped';
     UPDATE series SET metric = 'smeltr_source_full_total' WHERE metric = 'smeltr_source_full';
     UPDATE series SET metric = 'smeltr_source_messages_finished_total' WHERE metric = 'smeltr_source_messages_finished';
     UPDATE series SET metric = 'smeltr_source_poll_failures_total' WHERE metric = 'smeltr_source_poll_failures';
     UPDATE series SET metric = 'smeltr_source_refused_total' WHERE metric = 'smeltr_source_refused';
     UPDATE series SET metric = 'smeltr_tick_batches_total' WHERE metric = 'smeltr_tick_batches';
     UPDATE series SET metric = 'smeltr_tick_flush_seconds_total' WHERE metric = 'smeltr_tick_flush_micros';
     UPDATE series SET metric = 'smeltr_tick_idle_polls_total' WHERE metric = 'smeltr_tick_idle_polls';
     UPDATE series SET metric = 'smeltr_tick_poll_seconds_total' WHERE metric = 'smeltr_tick_poll_micros';
     UPDATE series SET metric = 'smeltr_tick_process_seconds_total' WHERE metric = 'smeltr_tick_process_micros';",
    // 9 -- a warning's facts once they end, so a chart can mark when one held. `fact_clock` holds those still active.
    "CREATE TABLE fact_history (
         entry      TEXT NOT NULL,
         subject    TEXT NOT NULL,
         started_ms INTEGER NOT NULL,
         ended_ms   INTEGER NOT NULL,
         PRIMARY KEY (entry, subject, started_ms)
     );
     CREATE INDEX fact_history_subject ON fact_history (subject, ended_ms);
     CREATE INDEX fact_history_ended ON fact_history (ended_ms);",
];

/// [`RegistryStore::series_since`] on a connection the caller holds.
fn series_since_on(
    conn: &Connection,
    metric: &str,
    tier: Tier,
    since_ms: i64,
) -> Result<Vec<(String, SubjectPoints)>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT s.labels_json, p.bucket_ms, p.value FROM series s JOIN series_points p ON p.series_id = s.id \
         WHERE p.tier = ?1 AND s.metric = ?2 AND p.bucket_ms >= ?3 ORDER BY s.labels_json, p.bucket_ms",
    )?;
    let rows: Vec<(String, i64, f64)> = stmt
        .query_map(params![tier.code(), metric, since_ms], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    // Grouping by a linear scan rather than a map: the query is already ordered by `labels_json`, so each
    // subject's rows arrive together.
    let mut out: Vec<(String, SubjectPoints)> = Vec::new();
    for (subject_key, bucket_ms, value) in rows {
        match out.last_mut() {
            Some((key, points)) if *key == subject_key => points.push((bucket_ms, value)),
            _ => out.push((subject_key, vec![(bucket_ms, value)])),
        }
    }
    Ok(out)
}

/// Logs the leader keeps for the fleet. A row cap rather than an age: a quiet fleet should still have its
/// last words, and a loud one must not grow the store without bound. `id` rises with arrival, so the newest rows
/// are the ones past the cap from the other end.
pub const LOGS_KEPT: i64 = 100_000;

/// `%` and `_` are wildcards to `LIKE`, so a search for them must say so. Paired with `ESCAPE '\\'` at every
/// call, and `\\` itself is escaped first or it would escape the escape.
fn like_escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

const UPSERT_LATEST: &str = "INSERT INTO latest_state
     (worker_id, thread_id, report_json, stop_after_unreachable_secs, commit_sha, smeltr_version,
      supervising_json, authority_expires_ms, stopped_at_ms, last_seen_ms, release)
 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
 ON CONFLICT(worker_id, thread_id) DO UPDATE SET
     report_json = excluded.report_json,
     stop_after_unreachable_secs = excluded.stop_after_unreachable_secs,
     commit_sha = excluded.commit_sha,
     smeltr_version = excluded.smeltr_version,
     release = excluded.release,
     supervising_json = excluded.supervising_json,
     authority_expires_ms = excluded.authority_expires_ms,
     stopped_at_ms = excluded.stopped_at_ms,
     last_seen_ms = excluded.last_seen_ms";

/// Every row one worker has.
const FORGET_WORKER_ROWS: &str = "DELETE FROM latest_state WHERE worker_id = ?1";

/// What a heartbeat says about the worker rather than the thread, onto every row that worker already has.
const REFRESH_WORKER_FACTS: &str = "UPDATE latest_state
    SET stop_after_unreachable_secs = ?2, commit_sha = ?3, smeltr_version = ?4, authority_expires_ms = ?5,
        supervising_json = ?6, stopped_at_ms = ?7, release = ?8
  WHERE worker_id = ?1";

/// Fold a counter's delta into whatever the bucket already holds: two samples landing in one bucket sum.
const UPSERT_COUNTER: &str = "INSERT INTO series_points (series_id, tier, bucket_ms, value) VALUES (?1,?2,?3,?4)
     ON CONFLICT(series_id, tier, bucket_ms) DO UPDATE SET value = value + excluded.value";

/// Keep a gauge's maximum within the bucket: a level, not an amount, so a spike survives the window.
const UPSERT_GAUGE: &str = "INSERT INTO series_points (series_id, tier, bucket_ms, value) VALUES (?1,?2,?3,?4)
     ON CONFLICT(series_id, tier, bucket_ms) DO UPDATE SET value = MAX(value, excluded.value)";

fn upsert_sql(kind: &str) -> &'static str {
    if kind == COUNTER { UPSERT_COUNTER } else { UPSERT_GAUGE }
}

/// The id of the series `metric` and `subject_key` name, created on first use with the kind its first point has.
fn series_id(conn: &Connection, metric: &str, subject_key: &str, counter: bool) -> rusqlite::Result<i64> {
    let found = conn
        .prepare_cached("SELECT id FROM series WHERE metric = ?1 AND labels_json = ?2")?
        .query_row(params![metric, subject_key], |r| r.get(0))
        .optional()?;
    if let Some(id) = found {
        return Ok(id);
    }
    conn.prepare_cached("INSERT INTO series (metric, labels_json, kind) VALUES (?1, ?2, ?3)")?
        .execute(params![metric, subject_key, if counter { COUNTER } else { GAUGE }])?;
    Ok(conn.last_insert_rowid())
}

/// Write one point into the fine bucket covering `now_ms`, through [`UPSERT_COUNTER`] or [`UPSERT_GAUGE`],
/// whichever the point's kind names. The one place a sample's tier, bucket and kind are decided, so a heartbeat's
/// hundreds and a single seeded point cannot drift apart.
fn write_point(conn: &Connection, point: &SeriesPoint<'_>, now_ms: i64) -> rusqlite::Result<()> {
    let id = series_id(conn, point.metric, &point.subject_key, point.counter)?;
    conn.prepare_cached(if point.counter { UPSERT_COUNTER } else { UPSERT_GAUGE })?
        .execute(params![
            id,
            Tier::TenSeconds.code(),
            Tier::TenSeconds.align(now_ms),
            point.value
        ])?;
    Ok(())
}

/// One row of `latest_state`: what a thread last reported, and what the leader knew about the worker that sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestState {
    pub worker_id: String,
    pub thread_id: String,
    /// The caller's encoding of the thread's report, stored opaquely (see [`LatestRow`]).
    pub report_json: String,
    pub stop_after_unreachable_secs: Option<u64>,
    /// The commit whose bundle the worker was running when it sent this.
    pub commit: Option<String>,
    /// The worker's own build when it sent this. Empty from a worker too old to report one, and from a row
    /// written before this column.
    pub smeltr_version: String,
    /// The release that build is. `None` on any other build, and from a row written before this column.
    pub release: Option<String>,
    /// The caller's encoding of what the worker's supervisor was doing, stored opaquely (see [`LatestRow`]).
    pub supervising_json: Option<String>,
    /// When the authority the worker verifies the leader against expires, as of this report.
    pub authority_expires_millis: Option<i64>,
    /// When the worker said it was stopping, where that is the last thing it said.
    pub stopped_at_ms: Option<i64>,
    pub last_seen_ms: i64,
}

/// The registry's durable half: `latest_state`, `fact_clock`, and `series_points`, behind one connection guarded
/// by one `Mutex` -- the same shape `sqlite_store::SqliteStore` uses for `leader.db`, in a file of its own.
pub struct RegistryStore {
    /// Taken with `unwrap_or_else(|e| e.into_inner())`, so poisoning is recovered from rather than propagated --
    /// the one place this store differs from `sqlite_store::SqliteStore`, which holds the configuration and is
    /// right to fail loudly.
    ///
    /// Every method here answers a `Result`, and this lock is the only way any of them could panic instead; a
    /// caller already has the error path. A panic under the lock is reported where it happened, and poisoning is
    /// only its aftermath: refusing to work afterward would turn one bug into a leader that has permanently
    /// stopped answering `/api/health`, `/api/warnings`, `/api/flow` and `/api/areas/:area/stats`, none of which it
    /// touched.
    /// Recovery is sound because a `Transaction` rolls back as it drops, so what comes back is consistent.
    conn: Mutex<Connection>,
    /// When [`RegistryStore::maybe_maintain`] should next actually run `maintain` -- epoch milliseconds. Rollup and
    /// expiry cost a table scan per promoting tier; throttling them to once per fine bucket, the same cadence data
    /// arrives at, keeps that cost off every single heartbeat.
    next_maintenance_due_ms: AtomicI64,
    /// Logs kept, as [`LOGS_KEPT`] except where a test lowers it. A field rather than a constant so
    /// that `maintain` is what a test exercises, rather than a copy of its `DELETE`.
    logs_kept: i64,
}

/// One stored log, as the leader holds it. Every string here already passed through the heartbeat's
/// bounds (`heartbeat::MAX_TEXT_LEN` and friends), so what is here is what the fleet delivered rather
/// than what its call site said.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredLog {
    pub id: i64,
    pub at_ms: i64,
    pub worker_id: String,
    pub group_id: Option<String>,
    pub thread_name: Option<String>,
    pub level: String,
    pub site: String,
    pub message: String,
    /// The field list as it was delivered, still JSON: nothing here reads it, and whatever renders it wants
    /// the pairs rather than a shape this layer invented.
    pub fields_json: String,
}

/// What one worker last logged at one site about one subject, as [`RegistryStore::latest_log_by_site`] reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct LoggedSite {
    pub worker_id: String,
    pub site: String,
    /// The group the latest row was delivered under.
    pub group_id: Option<String>,
    /// The latest row's level, message and fields.
    pub level: String,
    pub message: String,
    /// As [`StoredLog::fields_json`] holds it.
    pub fields_json: String,
    /// When the latest row was logged.
    pub at_ms: i64,
    /// When the earliest row read at this site about this subject was logged.
    pub first_ms: i64,
}

/// What to narrow a read of the logs table by. Every field is optional and absent means "any", so the
/// default asks for the newest of everything.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogFilter<'a> {
    /// Exactly this severity. Levels do not nest here: asking for `warn` is asking for `warn`, and a caller
    /// wanting warnings and errors together asks for both.
    pub levels: &'a [String],
    pub worker_id: Option<&'a str>,
    pub group_id: Option<&'a str>,
    /// Matched anywhere in the site, the same as `message_contains` is against the message.
    pub site_contains: Option<&'a str>,
    /// Matched anywhere in the message. Truncated at delivery, so this cannot find what was cut.
    pub message_contains: Option<&'a str>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    /// Rows below this id -- the "older" page. At most one of `before_id`/`after_id` is set; both together
    /// read as a range.
    pub before_id: Option<i64>,
    /// Rows above this id -- the "newer" page, walking back toward the current head.
    pub after_id: Option<i64>,
}

/// Renames a damaged registry file and its sidecars to `<path>.damaged-<unix seconds>`, `-wal` and `-shm` after
/// that, so SQLite still pairs them when someone opens the kept file to look. Returns the kept file's path.
fn set_aside(path: &Path) -> Result<std::path::PathBuf, StoreError> {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let with = |suffix: &str| {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        std::path::PathBuf::from(p)
    };
    let kept = with(&format!(".damaged-{secs}"));
    for sidecar in ["", "-wal", "-shm"] {
        let (from, to) = (with(sidecar), with(&format!(".damaged-{secs}{sidecar}")));
        match std::fs::rename(&from, &to) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !sidecar.is_empty() => {}
            Err(e) => return Err(StoreError(format!("setting aside {}: {e}", from.display()))),
        }
    }
    Ok(kept)
}

impl RegistryStore {
    /// A damaged file is set aside and a fresh one started: what it held comes back with the next heartbeats.
    pub fn open(path: &Path) -> Result<RegistryStore, StoreError> {
        let mut conn = Connection::open(path)?;
        if let Some(found) = super::sqlite_store::damage(&conn, super::sqlite_store::Check::Quick)? {
            // Closed first: the sidecars are renamed with the file, and a fresh connection must not find them.
            drop(conn);
            let kept = set_aside(path)?;
            tracing::warn!(
                target: "registry.rebuilt",
                path = %path.display(),
                kept = %kept.display(),
                damage = %found,
                "registry.db was damaged: set it aside and started a fresh one"
            );
            conn = Connection::open(path)?;
        }
        // Synchronous NORMAL rather than the default: in WAL that is safe from corruption and from a process
        // crash, and gives up only that the most recent commits may roll back after a power loss or a hard reset.
        // This file is the leader's operational view, not anyone's system of record -- what belongs somewhere
        // durable goes there as data, through a smeltr logs or smeltr metrics source routed to a real destination.
        // `leader.db` holds the configuration, on a different connection at the default.
        // `auto_vacuum` first: it is fixed once page 1 is written, and the switch to WAL writes it. Asked for
        // after the switch, it is refused without a word and `incremental_vacuum` reclaims nothing.
        conn.execute_batch("PRAGMA auto_vacuum=INCREMENTAL; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        super::steps::upgrade_sqlite(&mut conn, "registry.db", STEPS).map_err(StoreError)?;
        Ok(RegistryStore {
            conn: Mutex::new(conn),
            next_maintenance_due_ms: AtomicI64::new(0),
            logs_kept: LOGS_KEPT,
        })
    }

    // -- latest state --------------------------------------------------------

    /// Every thread's last report, unordered.
    pub fn load_latest_state(&self) -> Result<Vec<LatestState>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT worker_id, thread_id, report_json, stop_after_unreachable_secs, commit_sha, smeltr_version,
                    supervising_json, authority_expires_ms, stopped_at_ms, last_seen_ms, release
             FROM latest_state",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(LatestState {
                    worker_id: r.get(0)?,
                    thread_id: r.get(1)?,
                    report_json: r.get(2)?,
                    stop_after_unreachable_secs: r.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                    commit: r.get(4)?,
                    smeltr_version: r.get::<_, Option<String>>(5)?.unwrap_or_default(),
                    supervising_json: r.get(6)?,
                    authority_expires_millis: r.get(7)?,
                    stopped_at_ms: r.get(8)?,
                    last_seen_ms: r.get(9)?,
                    release: r.get(10)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Drops every row a worker has. What the registry mirrors when the leader forgets one.
    pub fn forget_worker(&self, worker_id: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(FORGET_WORKER_ROWS, params![worker_id])?;
        // Its last window, which only that worker's next beat would be checked against.
        conn.execute("DELETE FROM stream_windows WHERE worker_id = ?1", params![worker_id])?;
        Ok(())
    }

    // -- fact clock -----------------------------------------------------------

    /// Mirrors one call to `registry::Registry::observe_facts`: `removed` facts are no longer active, so they move
    /// to `fact_history` as having ended at `now_ms`; `inserted` are newly active and are stamped `now_ms` -- but
    /// only if nothing is there yet, so a fact this call already knows the true start of (loaded at open, or
    /// inserted on an earlier call) keeps it.
    pub fn sync_fact_clock(
        &self,
        removed: &[(String, String)],
        inserted: &[(String, String)],
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if removed.is_empty() && inserted.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        for (entry, subject) in removed {
            conn.execute(
                "INSERT OR IGNORE INTO fact_history (entry, subject, started_ms, ended_ms) \
                 SELECT entry, subject, started_ms, ?3 FROM fact_clock WHERE entry = ?1 AND subject = ?2",
                params![entry, subject, now_ms],
            )?;
            conn.execute(
                "DELETE FROM fact_clock WHERE entry = ?1 AND subject = ?2",
                params![entry, subject],
            )?;
        }
        for (entry, subject) in inserted {
            conn.execute(
                "INSERT OR IGNORE INTO fact_clock (entry, subject, started_ms) VALUES (?1, ?2, ?3)",
                params![entry, subject, now_ms],
            )?;
        }
        Ok(())
    }

    /// Every currently-active fact's true start: (entry, subject, started).
    pub fn load_fact_clock(&self) -> Result<Vec<(String, String, i64)>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare("SELECT entry, subject, started_ms FROM fact_clock")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// The subjects of every one of `entries` that ended at or after `since_ms`.
    pub fn facts_ended_since(&self, entries: &[&str], since_ms: i64) -> Result<Vec<String>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt =
            conn.prepare_cached("SELECT entry, subject FROM fact_history WHERE ended_ms >= ?1 ORDER BY subject")?;
        let rows = stmt
            .query_map(params![since_ms], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut out: Vec<String> = rows
            .into_iter()
            .filter(|(entry, _)| entries.contains(&entry.as_str()))
            .map(|(_, subject)| subject)
            .collect();
        out.dedup();
        Ok(out)
    }

    /// When each of `entries` held for `subject` and overlapped the time since `since_ms`, oldest first:
    /// (entry, started, ended), with no end for one still active.
    pub fn fact_spans(
        &self,
        entries: &[&str],
        subject: &str,
        since_ms: i64,
    ) -> Result<Vec<(String, i64, Option<i64>)>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();
        let mut ended = conn.prepare_cached(
            "SELECT entry, started_ms, ended_ms FROM fact_history WHERE subject = ?1 AND ended_ms >= ?2",
        )?;
        for row in ended.query_map(params![subject, since_ms], |r| {
            Ok((r.get::<_, String>(0)?, r.get(1)?, Some(r.get(2)?)))
        })? {
            out.push(row?);
        }
        let mut active = conn.prepare_cached("SELECT entry, started_ms FROM fact_clock WHERE subject = ?1")?;
        for row in active.query_map(params![subject], |r| Ok((r.get::<_, String>(0)?, r.get(1)?, None)))? {
            out.push(row?);
        }
        out.retain(|(entry, _, _)| entries.contains(&entry.as_str()));
        out.sort_by_key(|&(_, started, _)| started);
        Ok(out)
    }

    // -- series -----------------------------------------------------------------

    /// Add a counter's delta into the fine bucket covering `bucket_ms`. Deltas already: the heartbeat's cursor
    /// (`heartbeat::peek_for_heartbeat`) subtracts what a subscriber last committed, so two samples landing in one
    /// bucket are summed here, not replaced.
    pub fn record_delta(&self, metric: &str, subject_key: &str, bucket_ms: i64, delta: f64) -> Result<(), StoreError> {
        self.record_point(metric, subject_key, bucket_ms, true, delta)
    }

    /// Keep a gauge's maximum within the fine bucket covering `bucket_ms`. A gauge is a level, not an amount, so a
    /// bucket does not sum it; the maximum is what a reader watching for lag climbing (E6) needs to still see a
    /// spike that a last-value or an average would have smoothed away, and it composes without re-deriving
    /// anything: the maximum of a set of maxima is the maximum of the whole set, so a rollup can fold it forward
    /// tier by tier and never has to look back at raw samples.
    pub fn record_gauge_max(
        &self,
        metric: &str,
        subject_key: &str,
        bucket_ms: i64,
        value: f64,
    ) -> Result<(), StoreError> {
        self.record_point(metric, subject_key, bucket_ms, false, value)
    }

    /// One point, through the statement its kind names. What seeds a series outside a heartbeat; a heartbeat's own
    /// points take the same path in bulk ([`RegistryStore::record_heartbeat`]).
    fn record_point(
        &self,
        metric: &str,
        subject_key: &str,
        bucket_ms: i64,
        counter: bool,
        value: f64,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        write_point(
            &conn,
            &SeriesPoint {
                metric,
                subject_key: subject_key.to_string(),
                counter,
                value,
            },
            bucket_ms,
        )?;
        Ok(())
    }

    /// One heartbeat's durable mirror, in one transaction: each thread's latest state and every point its samples
    /// fold into.
    ///
    /// One transaction rather than one per row. A commit is what costs -- the statements inside it are a few
    /// microseconds each -- so a fleet's write load is its heartbeat rate, not its sample rate. It is also the
    /// truthful unit: a half-written heartbeat is a claim about a window that never held.
    pub fn record_heartbeat(&self, beat: &HeartbeatRows<'_>) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.transaction()?;
        {
            // Cached on the connection, not compiled per beat: the same three statements run for every heartbeat
            // this leader will ever take.
            if beat.stopped_at_ms.is_some() {
                // A stopping heartbeat's rows are every row its worker keeps, so the rest go.
                tx.prepare_cached(FORGET_WORKER_ROWS)?
                    .execute(params![beat.worker_id])?;
            } else {
                // Onto every row this worker already has, before its own are written: a heartbeat carrying no
                // thread still said what the process behind them holds, and these are the only rows that can say
                // it.
                tx.prepare_cached(REFRESH_WORKER_FACTS)?.execute(params![
                    beat.worker_id,
                    beat.stop_after_unreachable_secs.map(|v| v as i64),
                    beat.commit,
                    beat.smeltr_version,
                    beat.authority_expires_millis,
                    beat.supervising_json,
                    beat.stopped_at_ms,
                    beat.release
                ])?;
            }
            let mut latest = tx.prepare_cached(UPSERT_LATEST)?;
            for thread in beat.threads {
                latest.execute(params![
                    beat.worker_id,
                    thread.thread_id,
                    thread.report_json,
                    beat.stop_after_unreachable_secs.map(|v| v as i64),
                    beat.commit,
                    beat.smeltr_version,
                    beat.supervising_json,
                    beat.authority_expires_millis,
                    beat.stopped_at_ms,
                    beat.now_ms,
                    beat.release
                ])?;
            }
            for point in beat.points {
                write_point(&tx, point, beat.now_ms)?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Every point of one series at one tier, oldest first. A bucket nothing landed in has no row and so no entry
    /// here -- absence, not a zero.
    pub fn points(&self, metric: &str, subject_key: &str, tier: Tier) -> Result<Vec<(i64, f64)>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(
            "SELECT p.bucket_ms, p.value FROM series s JOIN series_points p ON p.series_id = s.id \
             WHERE p.tier = ?1 AND s.metric = ?2 AND s.labels_json = ?3 ORDER BY p.bucket_ms",
        )?;
        let rows = stmt
            .query_map(params![tier.code(), metric, subject_key], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Every subject of `metric` at `tier` with a point at or after `since_ms`, oldest first within each subject --
    /// what E2 (`leader::flow_snapshot`) and E6's lag-climbing check both want: every source or destination's
    /// series at once, not one subject looked up at a time. A subject with nothing in range is missing from the
    /// result outright, the same absence [`RegistryStore::points`] already gives one bucket.
    pub fn series_since(
        &self,
        metric: &str,
        tier: Tier,
        since_ms: i64,
    ) -> Result<Vec<(String, SubjectPoints)>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        series_since_on(&conn, metric, tier, since_ms)
    }

    /// [`RegistryStore::series_since`] at each of `tiers`, read together: [`RegistryStore::maintain`] cannot move a
    /// bucket from one tier to the next between them, which would count it in both.
    pub fn series_since_at(
        &self,
        metric: &str,
        tiers: &[Tier],
        since_ms: i64,
    ) -> Result<Vec<Vec<(String, SubjectPoints)>>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        tiers
            .iter()
            .map(|tier| series_since_on(&conn, metric, *tier, since_ms))
            .collect()
    }

    /// Record what one worker said about itself. Best-effort like the series: a heartbeat must not fail because
    /// the leader could not write down what it carried.
    pub fn record_logs(
        &self,
        worker_id: &str,
        boot_id: u64,
        group_id: Option<&str>,
        events: &[crate::heartbeat::LogDto],
    ) -> Result<(), StoreError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.transaction()?;
        {
            // `OR IGNORE` against the unique key: a heartbeat whose answer was lost is sent again with the same
            // events, and the second arrival must leave the table as the first did.
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO logs \
                 (at_ms, worker_id, boot_id, seq, thread_name, level, site, message, fields_json, group_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )?;
            for event in events {
                let fields = serde_json::to_string(&event.fields).unwrap_or_else(|_| "[]".to_string());
                stmt.execute(params![
                    event.timestamp_ms,
                    worker_id,
                    // A log from the worker's runtime counts its sequence in that process's life, not this
                    // request's, so it dedupes against the right one.
                    event.boot_id.unwrap_or(boot_id) as i64,
                    event.seq as i64,
                    event.thread,
                    event.level,
                    event.site,
                    event.message,
                    fields,
                    group_id,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Newest first, at most `limit`, from a keyset position: [`LogFilter::before_id`] and
    /// [`LogFilter::after_id`] bound `id`, so a result stays correct against the row cap `maintain`
    /// enforces -- pruning deletes only the oldest rows, which a bounding id never revisits, where an
    /// offset would silently skip or repeat rows as the table shifts under it.
    ///
    /// `after_id` walks toward the newest and is fetched ascending so `LIMIT` bounds the right end of the
    /// range, then reversed; every other case is already newest-first.
    ///
    /// No index covers `level`, `site` or `message` (only `at_ms` and the delivery key). At the cap of
    /// [`LOGS_KEPT`] the worst case -- a substring that matches nothing, so every row is read -- measures
    /// ~13 ms, and an ordinary filtered page ~1 ms. The cap is doing the work an index would, so there is none.
    pub fn logs(&self, filter: &LogFilter, limit: usize) -> Result<Vec<StoredLog>, StoreError> {
        let mut sql = String::from(
            "SELECT id, at_ms, worker_id, group_id, thread_name, level, site, message, fields_json \
             FROM logs WHERE 1 = 1",
        );
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if !filter.levels.is_empty() {
            let holes = std::iter::repeat_n("?", filter.levels.len())
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" AND level IN ({holes})"));
            for level in filter.levels {
                args.push(Box::new(level.clone()));
            }
        }
        for (column, value) in [("worker_id", filter.worker_id), ("group_id", filter.group_id)] {
            if let Some(value) = value {
                sql.push_str(&format!(" AND {column} = ?"));
                args.push(Box::new(value.to_string()));
            }
        }
        // `escape`d, so a site or message containing `%` or `_` searches for those characters rather than for
        // every row.
        for (column, value) in [("site", filter.site_contains), ("message", filter.message_contains)] {
            if let Some(text) = value {
                sql.push_str(&format!(" AND {column} LIKE ? ESCAPE '\\'"));
                args.push(Box::new(format!("%{}%", like_escape(text))));
            }
        }
        for (op, value) in [(">=", filter.since_ms), ("<=", filter.until_ms)] {
            if let Some(value) = value {
                sql.push_str(&format!(" AND at_ms {op} ?"));
                args.push(Box::new(value));
            }
        }
        if let Some(id) = filter.after_id {
            sql.push_str(" AND id > ?");
            args.push(Box::new(id));
        }
        if let Some(id) = filter.before_id {
            sql.push_str(" AND id < ?");
            args.push(Box::new(id));
        }
        // Ascending only for `after_id`, so `LIMIT` keeps the rows nearest it rather than the oldest ones.
        let ascending = filter.after_id.is_some();
        sql.push_str(if ascending {
            " ORDER BY id ASC LIMIT ?"
        } else {
            " ORDER BY id DESC LIMIT ?"
        });
        args.push(Box::new(limit as i64));

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(&sql)?;
        let mut rows: Vec<StoredLog> = stmt
            .query_map(rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())), |r| {
                Ok(StoredLog {
                    id: r.get(0)?,
                    at_ms: r.get(1)?,
                    worker_id: r.get(2)?,
                    group_id: r.get(3)?,
                    thread_name: r.get(4)?,
                    level: r.get(5)?,
                    site: r.get(6)?,
                    message: r.get(7)?,
                    fields_json: r.get(8)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        if ascending {
            rows.reverse();
        }
        Ok(rows)
    }

    /// The latest row each worker logged at each site about each subject (its `subject` field, or none) at one of
    /// `levels`, within `[since_ms, until_ms]` -- one entry per (worker, site, subject), not one per row. Per subject
    /// because one worker can log one site about two destinations, and each is told on its own warning.
    /// `worker_logged_warnings` wants one warning per worker and site,
    /// so it does the grouping here rather than paging through every qualifying row itself: a page walk needs a
    /// cap on how many pages to read, and any cap short of the whole table can let a noisy worker's rows fill it
    /// before the scan reaches an older, quieter one's own inside the same window. A single query has no such cap
    /// to pick -- it reads the window once, whatever the table holds, and groups as it goes.
    ///
    /// Window functions rather than a bare column beside `MAX(at_ms)`: an entry wants both the latest row and the
    /// earliest time, and SQLite says which row a bare column comes from only when one `MIN` or `MAX` stands alone.
    /// Two rows at the same `at_ms` go to the one that arrived last.
    pub fn latest_log_by_site(
        &self,
        levels: &[String],
        since_ms: i64,
        until_ms: i64,
    ) -> Result<Vec<LoggedSite>, StoreError> {
        if levels.is_empty() {
            return Ok(Vec::new());
        }
        let holes = std::iter::repeat_n("?", levels.len()).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT worker_id, site, group_id, level, message, fields_json, at_ms, first_ms FROM ( \
                 SELECT worker_id, site, group_id, level, message, fields_json, at_ms, \
                     MIN(at_ms) OVER (PARTITION BY worker_id, site, subject) AS first_ms, \
                     ROW_NUMBER() OVER (PARTITION BY worker_id, site, subject ORDER BY at_ms DESC, id DESC) AS nth \
                 FROM ( \
                     SELECT *, (SELECT CAST(json_extract(f.value, '$[1].value') AS TEXT) \
                                FROM json_each(logs.fields_json) AS f \
                                WHERE json_extract(f.value, '$[0]') = 'subject' LIMIT 1) AS subject \
                     FROM logs WHERE level IN ({holes}) AND at_ms >= ? AND at_ms <= ? \
                 ) \
             ) WHERE nth = 1"
        );
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = levels
            .iter()
            .map(|level| Box::new(level.clone()) as Box<dyn rusqlite::ToSql>)
            .collect();
        args.push(Box::new(since_ms));
        args.push(Box::new(until_ms));

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())), |r| {
                Ok(LoggedSite {
                    worker_id: r.get(0)?,
                    site: r.get(1)?,
                    group_id: r.get(2)?,
                    level: r.get(3)?,
                    message: r.get(4)?,
                    fields_json: r.get(5)?,
                    at_ms: r.get(6)?,
                    first_ms: r.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    /// Roll every bucket that has aged out of its tier into the next one, then expire the last tier outright.
    /// One transaction: a fine bucket is read, folded into its coarse bucket, and deleted together, so a bucket is
    /// promoted exactly once no matter how many times `maintain` runs -- a later call finds nothing left to
    /// re-process until more buckets age past the (moving) cutoff.
    ///
    /// Also trims the logs table to its row cap, since that is the other thing here that grows.
    pub fn maintain(&self, now_ms: i64) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.transaction()?;

        for tier in [Tier::TenSeconds, Tier::OneMinute, Tier::FiveMinutes] {
            let next = tier.next().expect("every promoting tier has a next");
            let cutoff = now_ms - tier.retention_ms();
            let aged: Vec<(i64, i64, String, f64)> = {
                let mut stmt = tx.prepare(
                    "SELECT p.series_id, p.bucket_ms, s.kind, p.value \
                     FROM series_points p JOIN series s ON s.id = p.series_id \
                     WHERE p.tier = ?1 AND p.bucket_ms < ?2",
                )?;
                stmt.query_map(params![tier.code(), cutoff], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                })?
                .collect::<rusqlite::Result<_>>()?
            };
            for (series_id, bucket_ms, kind, value) in &aged {
                tx.prepare_cached(upsert_sql(kind))?.execute(params![
                    series_id,
                    next.code(),
                    next.align(*bucket_ms),
                    value
                ])?;
            }
            tx.execute(
                "DELETE FROM series_points WHERE tier = ?1 AND bucket_ms < ?2",
                params![tier.code(), cutoff],
            )?;
        }

        // Past the cap, oldest first. `AUTOINCREMENT` makes `id` monotonic even across deletes, so this cannot
        // start keeping the wrong end.
        tx.execute(
            "DELETE FROM logs WHERE id <= (SELECT MAX(id) FROM logs) - ?1",
            params![self.logs_kept],
        )?;

        // The last tier has nowhere to roll to: past its window a point is simply gone.
        let cutoff = now_ms - Tier::OneHour.retention_ms();
        tx.execute(
            "DELETE FROM series_points WHERE tier = ?1 AND bucket_ms < ?2",
            params![Tier::OneHour.code(), cutoff],
        )?;
        // A warning past the last tier's reach is past any chart's.
        tx.execute("DELETE FROM fact_history WHERE ended_ms < ?1", params![cutoff])?;
        // What the fleet's streams have not shown for as long as the last tier keeps: a stream that stopped, or
        // a field it stopped carrying, so a cap's room goes to what arrives now. A live step's declared fields are
        // reported on every tallied batch and so never age.
        for table in ["stream_fields", "stream_parsers", "stream_keys", "stream_unmatched"] {
            tx.execute(&format!("DELETE FROM {table} WHERE last_seen_ms < ?1"), params![cutoff])?;
        }
        // A series whose last point just expired: a subject no longer reported, which would otherwise keep its
        // row for good as workers and subjects come and go.
        tx.execute(
            "DELETE FROM series WHERE NOT EXISTS (SELECT 1 FROM series_points WHERE series_id = series.id)",
            [],
        )?;

        tx.commit()?;
        // Best-effort: reclaiming the pages `DELETE` freed is bookkeeping, not correctness, and must not fail a
        // heartbeat over it.
        let _ = conn.execute_batch("PRAGMA incremental_vacuum;");
        Ok(())
    }

    /// `maintain`, throttled to once per fine bucket -- the cadence data actually arrives at, so rollup never falls
    /// behind ingestion but also never runs more than data justifies. Failure is reported and dropped: a heartbeat
    /// must not fail because housekeeping did.
    pub fn maybe_maintain(&self, now_ms: i64) {
        let due = self.next_maintenance_due_ms.load(Ordering::Relaxed);
        if now_ms < due {
            return;
        }
        self.next_maintenance_due_ms
            .store(now_ms + Tier::TenSeconds.bucket_ms(), Ordering::Relaxed);
        if let Err(e) = self.maintain(now_ms) {
            tracing::warn!(
                target: "series.maintenance_failed",
                error = %e,
                "registry series maintenance failed, so old points are not being rolled up or dropped"
            );
        }
    }
}

// -- streams ------------------------------------------------------------------

/// One Parser's shape as the leader has accumulated it (`heartbeat::ParserShape`, summed over every heartbeat of
/// every worker of the group), with when it was first and last reported.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredParserShape {
    pub group_id: String,
    pub source: String,
    pub route: String,
    pub pipeline: String,
    pub step: String,
    pub rows: i64,
    pub fields_dropped: i64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub fields: Vec<StoredFieldShape>,
}

/// One field of a [`StoredParserShape`] (`heartbeat::FieldShape`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredFieldShape {
    pub name: String,
    pub declared: bool,
    pub suggested: String,
    pub present: i64,
    pub bools: i64,
    pub ints: i64,
    pub floats: i64,
    pub strings: i64,
    pub arrays: i64,
    pub objects: i64,
    pub failed: i64,
    /// Values that read as a time (`heartbeat::FieldShape::times`).
    pub times: i64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
}

/// The rows of one source no route took, by stream, as the leader has accumulated them
/// (`heartbeat::UnmatchedStreams`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredUnmatched {
    pub group_id: String,
    pub source: String,
    pub rows: i64,
    pub keys_dropped: i64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub keys: Vec<StoredStreamKey>,
}

/// One stream of a [`StoredUnmatched`] (`heartbeat::StreamKey`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredStreamKey {
    pub key: String,
    pub rows: i64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    /// The route condition that takes this stream (`SourceMeta::stream_condition`). Not stored: `GET /api/streams`
    /// fills it from the source's kind in the group's deployed bundle, and leaves it out where that bundle has none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    /// Whether rows came under it within [`DRIFT_WINDOW`], which is when a `StreamUnrouted` warning names it. Not
    /// stored: `GET /api/streams` sets it.
    pub recent: bool,
}

/// Which window of one worker process's life a stream report is ([`crate::heartbeat::HeartbeatRequest`]'s
/// `streams_window`), so a window sent twice is recorded once.
#[derive(Debug, Clone, Copy)]
pub struct StreamWindow<'a> {
    pub worker_id: &'a str,
    pub boot_id: u64,
    pub seq: u64,
}

/// Everything the leader has accumulated of the fleet's streams.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredStreams {
    pub parsers: Vec<StoredParserShape>,
    pub unmatched: Vec<StoredUnmatched>,
}

impl RegistryStore {
    /// Add one heartbeat's `report`, seen at `now_ms`, to what `group_id` has accumulated, unless it is a `window`
    /// already recorded: every count summed, the first and last seen times widened to take it in, and a field's type
    /// taken from the newest report. Beats of one group commit in whatever order they reach the lock, so an older one can follow a newer. The wire's caps hold here too: a field or key the table already holds the cap of is counted as
    /// dropped rather than added, so the tables grow with the configuration and never with the traffic.
    ///
    /// `deployed` is whether the report comes from the commit its group is deployed at. A step whose declarations
    /// differ from those held starts its counts over from such a report, and is left out of any other: during a
    /// rollout the workers still on the old commit neither restart the new counts nor add to them.
    pub fn record_streams(
        &self,
        group_id: &str,
        report: &crate::heartbeat::StreamReport,
        window: Option<StreamWindow>,
        deployed: bool,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        if report.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.transaction()?;
        if let Some(w) = window {
            let last: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT boot_id, seq FROM stream_windows WHERE worker_id = ?1",
                    params![w.worker_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if last.is_some_and(|(boot, seq)| boot == w.boot_id as i64 && seq >= w.seq as i64) {
                return Ok(());
            }
            tx.execute(
                "INSERT INTO stream_windows (worker_id, boot_id, seq) VALUES (?1, ?2, ?3) \
                 ON CONFLICT (worker_id) DO UPDATE SET boot_id = excluded.boot_id, seq = excluded.seq",
                params![w.worker_id, w.boot_id as i64, w.seq as i64],
            )?;
        }
        for shape in &report.parsers {
            let at = params![group_id, shape.source, shape.route, shape.pipeline, shape.step];
            // A step reports every field it declares, present or not, with its type. One whose declarations differ
            // from what is held runs another pipeline than the one counted.
            let held_declared: std::collections::BTreeSet<(String, String)> = tx
                .prepare(
                    "SELECT field, suggested FROM stream_fields WHERE group_id = ?1 AND source = ?2 AND route = ?3 \
                         AND pipeline = ?4 AND step = ?5 AND declared = 1",
                )?
                .query_map(at, |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?;
            let declared: std::collections::BTreeSet<(String, String)> = shape
                .fields
                .iter()
                .filter(|f| f.declared)
                .map(|f| (f.name.clone(), f.suggested.clone()))
                .collect();
            if held_declared != declared {
                let held: bool = tx.query_row(
                    "SELECT EXISTS (SELECT 1 FROM stream_parsers WHERE group_id = ?1 AND source = ?2 AND route = ?3 \
                         AND pipeline = ?4 AND step = ?5)",
                    at,
                    |row| row.get(0),
                )?;
                if held && !deployed {
                    continue;
                }
                tx.execute(
                    "DELETE FROM stream_fields WHERE group_id = ?1 AND source = ?2 AND route = ?3 \
                         AND pipeline = ?4 AND step = ?5",
                    at,
                )?;
                tx.execute(
                    "DELETE FROM stream_parsers WHERE group_id = ?1 AND source = ?2 AND route = ?3 \
                         AND pipeline = ?4 AND step = ?5",
                    at,
                )?;
            }
            let held: i64 = tx.query_row(
                "SELECT COUNT(*) FROM stream_fields \
                 WHERE group_id = ?1 AND source = ?2 AND route = ?3 AND pipeline = ?4 AND step = ?5",
                at,
                |row| row.get(0),
            )?;
            let mut room = crate::heartbeat::STREAM_FIELDS_PER_PARSER.saturating_sub(held as usize);
            let mut dropped = shape.fields_dropped as i64;
            for field in &shape.fields {
                let known: bool = tx.query_row(
                    "SELECT COUNT(*) FROM stream_fields \
                     WHERE group_id = ?1 AND source = ?2 AND route = ?3 AND pipeline = ?4 AND step = ?5 AND field = ?6",
                    params![
                        group_id,
                        shape.source,
                        shape.route,
                        shape.pipeline,
                        shape.step,
                        field.name
                    ],
                    |row| row.get::<_, i64>(0).map(|n| n > 0),
                )?;
                // A declaration is bounded by the config, so it takes a row past the cap: drift reads it.
                if !known && !field.declared {
                    if room == 0 {
                        dropped += field.present as i64;
                        continue;
                    }
                    room -= 1;
                }
                tx.execute(
                    "INSERT INTO stream_fields (group_id, source, route, pipeline, step, field, declared, suggested, \
                         present, bools, ints, floats, strings, arrays, objects, failed, times, first_seen_ms, last_seen_ms, \
                         present_last_ms, failed_last_ms, failing_since_ms) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?18, ?17, ?17, \
                         CASE WHEN ?9 > 0 THEN ?17 END, CASE WHEN ?16 > 0 THEN ?17 END, CASE WHEN ?16 > 0 THEN ?17 END) \
                     ON CONFLICT (group_id, source, route, pipeline, step, field) DO UPDATE SET \
                         declared = CASE WHEN excluded.last_seen_ms >= last_seen_ms THEN excluded.declared ELSE declared END, \
                         suggested = CASE WHEN excluded.last_seen_ms >= last_seen_ms THEN excluded.suggested ELSE suggested END, \
                         present = present + excluded.present, bools = bools + excluded.bools, \
                         ints = ints + excluded.ints, floats = floats + excluded.floats, \
                         strings = strings + excluded.strings, arrays = arrays + excluded.arrays, \
                         objects = objects + excluded.objects, failed = failed + excluded.failed, \
                         times = times + excluded.times, \
                         present_last_ms = CASE WHEN excluded.present > 0 THEN MAX(COALESCE(present_last_ms, 0), ?17) \
                             ELSE present_last_ms END, \
                         failing_since_ms = CASE WHEN excluded.failed = 0 THEN failing_since_ms \
                             WHEN failed_last_ms IS NULL OR ?17 - failed_last_ms > ?19 THEN ?17 \
                             ELSE failing_since_ms END, \
                         failed_last_ms = CASE WHEN excluded.failed > 0 THEN MAX(COALESCE(failed_last_ms, 0), ?17) \
                             ELSE failed_last_ms END, \
                         first_seen_ms = MIN(first_seen_ms, excluded.first_seen_ms), \
                     last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
                    params![
                        group_id,
                        shape.source,
                        shape.route,
                        shape.pipeline,
                        shape.step,
                        field.name,
                        field.declared,
                        field.suggested,
                        field.present as i64,
                        field.bools as i64,
                        field.ints as i64,
                        field.floats as i64,
                        field.strings as i64,
                        field.arrays as i64,
                        field.objects as i64,
                        field.failed as i64,
                        now_ms,
                        field.times as i64,
                        DRIFT_WINDOW.as_millis() as i64,
                    ],
                )?;
            }
            tx.execute(
                "INSERT INTO stream_parsers (group_id, source, route, pipeline, step, rows, fields_dropped, \
                     first_seen_ms, last_seen_ms, read_last_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, CASE WHEN ?6 > 0 THEN ?8 END) \
                 ON CONFLICT (group_id, source, route, pipeline, step) DO UPDATE SET \
                     rows = rows + excluded.rows, fields_dropped = fields_dropped + excluded.fields_dropped, \
                     read_last_ms = CASE WHEN excluded.rows > 0 THEN MAX(COALESCE(read_last_ms, 0), ?8) \
                         ELSE read_last_ms END, \
                     first_seen_ms = MIN(first_seen_ms, excluded.first_seen_ms), \
                     last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
                params![
                    group_id,
                    shape.source,
                    shape.route,
                    shape.pipeline,
                    shape.step,
                    shape.rows as i64,
                    dropped,
                    now_ms
                ],
            )?;
        }
        for streams in &report.unmatched {
            let held: i64 = tx.query_row(
                "SELECT COUNT(*) FROM stream_keys WHERE group_id = ?1 AND source = ?2",
                params![group_id, streams.source],
                |row| row.get(0),
            )?;
            let mut room = crate::heartbeat::STREAM_KEYS_PER_SOURCE.saturating_sub(held as usize);
            let mut dropped = streams.keys_dropped as i64;
            for key in &streams.keys {
                let known: bool = tx.query_row(
                    "SELECT COUNT(*) FROM stream_keys WHERE group_id = ?1 AND source = ?2 AND key = ?3",
                    params![group_id, streams.source, key.key],
                    |row| row.get::<_, i64>(0).map(|n| n > 0),
                )?;
                if !known {
                    if room == 0 {
                        dropped += key.rows as i64;
                        continue;
                    }
                    room -= 1;
                }
                tx.execute(
                    "INSERT INTO stream_keys (group_id, source, key, rows, first_seen_ms, last_seen_ms) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?5) \
                     ON CONFLICT (group_id, source, key) DO UPDATE SET \
                         rows = rows + excluded.rows, first_seen_ms = MIN(first_seen_ms, excluded.first_seen_ms), \
                     last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
                    params![group_id, streams.source, key.key, key.rows as i64, now_ms],
                )?;
            }
            tx.execute(
                "INSERT INTO stream_unmatched (group_id, source, rows, keys_dropped, first_seen_ms, last_seen_ms, \
                     unrouted_since_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?5) \
                 ON CONFLICT (group_id, source) DO UPDATE SET \
                     rows = rows + excluded.rows, keys_dropped = keys_dropped + excluded.keys_dropped, \
                     unrouted_since_ms = CASE WHEN unrouted_since_ms IS NULL OR ?5 - last_seen_ms > ?6 THEN ?5 \
                         ELSE unrouted_since_ms END, \
                     first_seen_ms = MIN(first_seen_ms, excluded.first_seen_ms), \
                     last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
                params![
                    group_id,
                    streams.source,
                    streams.rows as i64,
                    dropped,
                    now_ms,
                    DRIFT_WINDOW.as_millis() as i64
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Everything accumulated of every group's streams, each shape with its fields and each source with its keys,
    /// in key order.
    pub fn streams(&self) -> Result<StoredStreams, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut parsers: Vec<StoredParserShape> = conn
            .prepare(
                "SELECT group_id, source, route, pipeline, step, rows, fields_dropped, first_seen_ms, last_seen_ms \
                 FROM stream_parsers ORDER BY group_id, source, route, pipeline, step",
            )?
            .query_map([], |row| {
                Ok(StoredParserShape {
                    group_id: row.get(0)?,
                    source: row.get(1)?,
                    route: row.get(2)?,
                    pipeline: row.get(3)?,
                    step: row.get(4)?,
                    rows: row.get(5)?,
                    fields_dropped: row.get(6)?,
                    first_seen_ms: row.get(7)?,
                    last_seen_ms: row.get(8)?,
                    fields: Vec::new(),
                })
            })?
            .collect::<Result<_, _>>()?;
        let mut fields = conn.prepare(
            "SELECT field, declared, suggested, present, bools, ints, floats, strings, arrays, objects, failed, \
                 first_seen_ms, last_seen_ms, times \
             FROM stream_fields \
             WHERE group_id = ?1 AND source = ?2 AND route = ?3 AND pipeline = ?4 AND step = ?5 \
             ORDER BY first_seen_ms, field",
        )?;
        for shape in &mut parsers {
            shape.fields = fields
                .query_map(
                    params![shape.group_id, shape.source, shape.route, shape.pipeline, shape.step],
                    |row| {
                        Ok(StoredFieldShape {
                            name: row.get(0)?,
                            declared: row.get(1)?,
                            suggested: row.get(2)?,
                            present: row.get(3)?,
                            bools: row.get(4)?,
                            ints: row.get(5)?,
                            floats: row.get(6)?,
                            strings: row.get(7)?,
                            arrays: row.get(8)?,
                            objects: row.get(9)?,
                            failed: row.get(10)?,
                            first_seen_ms: row.get(11)?,
                            last_seen_ms: row.get(12)?,
                            times: row.get(13)?,
                        })
                    },
                )?
                .collect::<Result<_, _>>()?;
        }
        let mut unmatched: Vec<StoredUnmatched> = conn
            .prepare(
                "SELECT group_id, source, rows, keys_dropped, first_seen_ms, last_seen_ms \
                 FROM stream_unmatched ORDER BY group_id, source",
            )?
            .query_map([], |row| {
                Ok(StoredUnmatched {
                    group_id: row.get(0)?,
                    source: row.get(1)?,
                    rows: row.get(2)?,
                    keys_dropped: row.get(3)?,
                    first_seen_ms: row.get(4)?,
                    last_seen_ms: row.get(5)?,
                    keys: Vec::new(),
                })
            })?
            .collect::<Result<_, _>>()?;
        let mut keys = conn.prepare(
            "SELECT key, rows, first_seen_ms, last_seen_ms FROM stream_keys \
             WHERE group_id = ?1 AND source = ?2 ORDER BY first_seen_ms, key",
        )?;
        for streams in &mut unmatched {
            streams.keys = keys
                .query_map(params![streams.group_id, streams.source], |row| {
                    Ok(StoredStreamKey {
                        key: row.get(0)?,
                        rows: row.get(1)?,
                        first_seen_ms: row.get(2)?,
                        last_seen_ms: row.get(3)?,
                        condition: None,
                        recent: false,
                    })
                })?
                .collect::<Result<_, _>>()?;
        }
        Ok(StoredStreams { parsers, unmatched })
    }

    /// Forgets what every source outside `kept`, by `(group, source)`, showed of its streams.
    pub fn forget_streams_except(&self, kept: &std::collections::HashSet<(String, String)>) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.transaction()?;
        let held: Vec<(String, String)> = tx
            .prepare(
                "SELECT group_id, source FROM stream_parsers UNION SELECT group_id, source FROM stream_fields \
                 UNION SELECT group_id, source FROM stream_unmatched UNION SELECT group_id, source FROM stream_keys",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        for (group, source) in held.iter().filter(|g| !kept.contains(*g)) {
            for table in ["stream_parsers", "stream_fields", "stream_unmatched", "stream_keys"] {
                tx.execute(
                    &format!("DELETE FROM {table} WHERE group_id = ?1 AND source = ?2"),
                    params![group, source],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Each source that received rows no route took at or after `since_ms`, naming the streams it received them under
    /// since then.
    pub fn unrouted_since(&self, since_ms: i64) -> Result<Vec<UnroutedSource>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut sources: Vec<UnroutedSource> = conn
            .prepare(
                "SELECT group_id, source, unrouted_since_ms FROM stream_unmatched WHERE last_seen_ms >= ?1 \
                 ORDER BY group_id, source",
            )?
            .query_map(params![since_ms], |row| {
                Ok(UnroutedSource {
                    group_id: row.get(0)?,
                    source: row.get(1)?,
                    since_ms: row.get(2)?,
                    keys: Vec::new(),
                })
            })?
            .collect::<Result<_, _>>()?;
        let mut keys = conn.prepare(
            "SELECT key FROM stream_keys WHERE group_id = ?1 AND source = ?2 AND last_seen_ms >= ?3 \
             ORDER BY first_seen_ms, key",
        )?;
        for s in &mut sources {
            s.keys = keys
                .query_map(params![s.group_id, s.source, since_ms], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
        }
        Ok(sources)
    }

    /// Every declared field of every Parser shape, with when its step last read rows, it was last present and it
    /// last failed: what a drift warning reads.
    pub fn declared_fields(&self) -> Result<Vec<DeclaredField>, StoreError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let fields = conn
            .prepare(
                "SELECT f.group_id, f.pipeline, f.step, f.field, f.suggested, p.read_last_ms, f.present_last_ms, \
                     f.failed_last_ms, f.failing_since_ms \
                 FROM stream_fields f JOIN stream_parsers p \
                     USING (group_id, source, route, pipeline, step) \
                 WHERE f.declared = 1 \
                 ORDER BY f.group_id, f.pipeline, f.step, f.first_seen_ms, f.field",
            )?
            .query_map([], |row| {
                Ok(DeclaredField {
                    group_id: row.get(0)?,
                    pipeline: row.get(1)?,
                    step: row.get(2)?,
                    field: row.get(3)?,
                    declared: row.get(4)?,
                    read_last_ms: row.get(5)?,
                    present_last_ms: row.get(6)?,
                    failed_last_ms: row.get(7)?,
                    failing_since_ms: row.get(8)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(fields)
    }
}

/// One source of one group that received rows no route took (`RegistryStore::unrouted_since`): the streams they
/// came under, in the order each was first seen. An empty key is a row its source files under no stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnroutedSource {
    pub group_id: String,
    pub source: String,
    /// When its latest run of rows no route took began; `None` for a run recorded before the store kept it.
    pub since_ms: Option<i64>,
    pub keys: Vec<String>,
}

/// How long a declared field may go without a row that holds it, while its step reads rows, before it is gone; and
/// how far apart two failures may fall and still be one run of them. An hour of tallied batches is enough that a
/// field most rows hold is not missed by chance.
pub const DRIFT_WINDOW: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// One declared field of one Parser shape (`RegistryStore::declared_fields`). `None` where no window since the
/// store began keeping it said so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredField {
    pub group_id: String,
    pub pipeline: String,
    pub step: String,
    pub field: String,
    /// Its declared type.
    pub declared: String,
    /// When its step last read a row on this shape.
    pub read_last_ms: Option<i64>,
    pub present_last_ms: Option<i64>,
    pub failed_last_ms: Option<i64>,
    /// When the run of failures that ends at `failed_last_ms` began: no two of its failures fell further apart than
    /// [`DRIFT_WINDOW`].
    pub failing_since_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn stream_report(fields: &[&str], keys: &[&str]) -> crate::heartbeat::StreamReport {
        use crate::heartbeat::*;
        StreamReport {
            parsers: vec![ParserShape {
                source: "orders".into(),
                route: "to-es".into(),
                pipeline: "normalize".into(),
                step: "parse".into(),
                rows: 10,
                fields: fields
                    .iter()
                    .map(|name| FieldShape {
                        name: name.to_string(),
                        declared: false,
                        suggested: "str".into(),
                        present: 10,
                        strings: 10,
                        times: 4,
                        ..Default::default()
                    })
                    .collect(),
                fields_dropped: 0,
            }],
            unmatched: vec![UnmatchedStreams {
                source: "hec".into(),
                rows: keys.len() as u64,
                keys: keys
                    .iter()
                    .map(|key| StreamKey {
                        key: key.to_string(),
                        rows: 1,
                    })
                    .collect(),
                keys_dropped: 0,
            }],
        }
    }

    /// A declared field `ts`, present in `present` of a window's rows, `failed` of which would not coerce.
    fn declared_window(present: u64, failed: u64) -> crate::heartbeat::StreamReport {
        let mut report = stream_report(&["ts"], &[]);
        report.unmatched.clear();
        let field = &mut report.parsers[0].fields[0];
        (field.declared, field.suggested, field.present, field.failed) = (true, "datetime".into(), present, failed);
        report
    }

    /// A declared field keeps when it was last present and last failed, apart from when it was last reported, and
    /// a failure further than the drift window from the one before starts a new run.
    #[test]
    fn a_declared_field_keeps_when_it_was_last_present_and_when_its_failures_began() {
        let store = memory_store();
        let hour = DRIFT_WINDOW.as_millis() as i64;
        let windows = [
            (10, 0, 1_000),
            (10, 2, 2_000),
            (10, 1, 3_000),
            (0, 0, 4_000),
            (10, 3, 5_000 + hour),
        ];
        let mut seen = Vec::new();
        for (present, failed, at) in windows {
            store
                .record_streams("prod", &declared_window(present, failed), None, true, at)
                .unwrap();
            let f = store.declared_fields().unwrap().remove(0);
            seen.push((f.read_last_ms, f.present_last_ms, f.failed_last_ms, f.failing_since_ms));
        }
        assert_eq!(
            seen,
            [
                (Some(1_000), Some(1_000), None, None),
                (Some(2_000), Some(2_000), Some(2_000), Some(2_000)),
                (Some(3_000), Some(3_000), Some(3_000), Some(2_000)),
                (Some(4_000), Some(3_000), Some(3_000), Some(2_000)),
                (
                    Some(5_000 + hour),
                    Some(5_000 + hour),
                    Some(5_000 + hour),
                    Some(5_000 + hour)
                ),
            ]
        );
    }

    /// A step a guard kept from every row read nothing, and a declaration a report leaves out is no longer one.
    #[test]
    fn a_step_reads_only_the_rows_it_parsed_and_declares_only_what_it_reports() {
        let store = memory_store();
        store
            .record_streams("prod", &declared_window(10, 0), None, true, 1_000)
            .unwrap();
        let mut guarded = declared_window(0, 0);
        guarded.parsers[0].rows = 0;
        store.record_streams("prod", &guarded, None, true, 2_000).unwrap();
        assert_eq!(store.declared_fields().unwrap()[0].read_last_ms, Some(1_000));
        let mut undeclared = declared_window(0, 0);
        undeclared.parsers[0].fields.clear();
        store.record_streams("prod", &undeclared, None, true, 3_000).unwrap();
        assert!(store.declared_fields().unwrap().is_empty());
    }

    /// A step whose declarations change starts its counts over; one that keeps them adds to them.
    #[test]
    fn a_step_counts_afresh_once_its_declarations_change() {
        let store = memory_store();
        let rows = |store: &RegistryStore| store.streams().unwrap().parsers[0].rows;
        store
            .record_streams("prod", &declared_window(10, 0), None, true, 1_000)
            .unwrap();
        store
            .record_streams("prod", &declared_window(10, 0), None, true, 2_000)
            .unwrap();
        assert_eq!(rows(&store), 20);
        let mut retyped = declared_window(10, 0);
        retyped.parsers[0].fields[0].suggested = "str".into();
        store.record_streams("prod", &retyped, None, true, 3_000).unwrap();
        assert_eq!(rows(&store), 10);
        assert_eq!(store.streams().unwrap().parsers[0].first_seen_ms, 3_000);
    }

    /// During a rollout a step's report from a commit other than the deployed one, with other declarations, is
    /// left out; a first report is taken from any commit.
    #[test]
    fn a_rollout_neither_restarts_nor_mixes_a_steps_counts() {
        let store = memory_store();
        let rows = |store: &RegistryStore| store.streams().unwrap().parsers[0].rows;
        let mut older = declared_window(10, 0);
        older.parsers[0].fields[0].suggested = "str".into();
        store.record_streams("prod", &older, None, false, 1_000).unwrap();
        assert_eq!(rows(&store), 10);
        store
            .record_streams("prod", &declared_window(10, 0), None, true, 2_000)
            .unwrap();
        store.record_streams("prod", &older, None, false, 3_000).unwrap();
        store
            .record_streams("prod", &declared_window(10, 0), None, false, 4_000)
            .unwrap();
        assert_eq!(rows(&store), 20);
    }

    /// What a stream has not shown for the last tier's window goes; what it still shows stays.
    #[test]
    fn a_stream_unseen_for_the_last_window_is_forgotten() {
        let store = memory_store();
        store
            .record_streams("prod", &stream_report(&["old"], &["k1"]), None, true, 1_000)
            .unwrap();
        let later = 2_000 + Tier::OneHour.retention_ms();
        let mut newer = stream_report(&["new"], &[]);
        newer.unmatched.clear();
        store.record_streams("prod", &newer, None, true, later).unwrap();
        store.maintain(later).unwrap();
        let streams = store.streams().unwrap();
        let fields: Vec<_> = streams.parsers[0].fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(fields, ["new"]);
        assert!(streams.unmatched.is_empty(), "{:?}", streams.unmatched);
    }

    /// A source no kept bundle reads takes its streams with it; one still read keeps them.
    #[test]
    fn a_removed_source_takes_its_streams_with_it() {
        let store = memory_store();
        store
            .record_streams("prod", &stream_report(&["a"], &["k1"]), None, true, 1_000)
            .unwrap();
        let kept = [("prod".to_string(), "orders".to_string())].into();
        store.forget_streams_except(&kept).unwrap();
        let streams = store.streams().unwrap();
        assert_eq!(
            (streams.parsers.len(), streams.unmatched.len()),
            (1, 0),
            "hec is not read"
        );
        store.forget_streams_except(&Default::default()).unwrap();
        assert!(store.streams().unwrap().parsers.is_empty());
    }

    /// Two heartbeats of one shape are one shape with both windows' counts, first seen at the first and last at the
    /// second.
    #[test]
    fn streams_accumulate_across_heartbeats() {
        let store = memory_store();
        store
            .record_streams("prod", &stream_report(&["a", "b"], &["k1"]), None, true, 1_000)
            .unwrap();
        store
            .record_streams("prod", &stream_report(&["b", "c"], &["k1", "k2"]), None, true, 2_000)
            .unwrap();
        store
            .record_streams("staging", &stream_report(&["a"], &[]), None, true, 3_000)
            .unwrap();

        let streams = store.streams().unwrap();
        assert_eq!(streams.parsers.len(), 2, "one shape per group");
        let prod = &streams.parsers[0];
        assert_eq!((prod.group_id.as_str(), prod.rows), ("prod", 20));
        assert_eq!((prod.first_seen_ms, prod.last_seen_ms), (1_000, 2_000));
        let by_name: Vec<(&str, i64, i64, i64)> = prod
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.present, f.first_seen_ms, f.last_seen_ms))
            .collect();
        assert_eq!(
            by_name,
            vec![
                ("a", 10, 1_000, 1_000),
                ("b", 20, 1_000, 2_000),
                ("c", 10, 2_000, 2_000)
            ]
        );
        assert_eq!(prod.fields[1].times, 8, "the times are summed with the rest");
        let hec = &streams.unmatched[0];
        assert_eq!((hec.rows, hec.keys.len()), (3, 2));
        assert_eq!(hec.keys[0].rows, 2);
    }

    /// A beat taken at an earlier time can commit after a later one; the times and types stay those of the newest.
    #[test]
    fn an_older_beat_committed_late_moves_nothing_back() {
        let store = memory_store();
        let mut newer = stream_report(&["a"], &["k1"]);
        newer.parsers[0].fields[0].suggested = "i64".into();
        store.record_streams("prod", &newer, None, true, 2_000).unwrap();
        store
            .record_streams("prod", &stream_report(&["a"], &["k1"]), None, true, 1_000)
            .unwrap();

        let streams = store.streams().unwrap();
        let prod = &streams.parsers[0];
        assert_eq!((prod.first_seen_ms, prod.last_seen_ms), (1_000, 2_000));
        let a = &prod.fields[0];
        assert_eq!((a.first_seen_ms, a.last_seen_ms, a.present), (1_000, 2_000, 20));
        assert_eq!(a.suggested, "i64", "the newer beat's type stands");
        let hec = &streams.unmatched[0];
        assert_eq!((hec.first_seen_ms, hec.last_seen_ms), (1_000, 2_000));
        assert_eq!((hec.keys[0].first_seen_ms, hec.keys[0].last_seen_ms), (1_000, 2_000));
    }

    /// A window sent again, because its beat's answer was lost, is counted once; the next one, or the first of a
    /// new life, is counted.
    #[test]
    fn a_window_is_recorded_once() {
        let store = memory_store();
        let window = |boot_id, seq| {
            Some(StreamWindow {
                worker_id: "w1",
                boot_id,
                seq,
            })
        };
        let report = stream_report(&["a"], &["k1"]);
        store
            .record_streams("prod", &report, window(7, 1), true, 1_000)
            .unwrap();
        store
            .record_streams("prod", &report, window(7, 1), true, 1_000)
            .unwrap();
        assert_eq!(store.streams().unwrap().parsers[0].rows, 10, "the repeat was recorded");
        store
            .record_streams("prod", &report, window(7, 2), true, 2_000)
            .unwrap();
        store
            .record_streams("prod", &report, window(8, 1), true, 3_000)
            .unwrap();
        assert_eq!(store.streams().unwrap().parsers[0].rows, 30);
    }

    /// The tables grow with the configuration, never the traffic: a name past the cap is counted, not kept.
    #[test]
    fn a_stream_table_holds_the_caps_worth_of_names_and_counts_the_rest() {
        let store = memory_store();
        let many: Vec<String> = (0..crate::heartbeat::STREAM_FIELDS_PER_PARSER)
            .map(|i| format!("f{i}"))
            .collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        store
            .record_streams("prod", &stream_report(&many, &[]), None, true, 1)
            .unwrap();
        store
            .record_streams("prod", &stream_report(&["one-more", "f0"], &[]), None, true, 2)
            .unwrap();

        let streams = store.streams().unwrap();
        let shape = &streams.parsers[0];
        assert_eq!(shape.fields.len(), crate::heartbeat::STREAM_FIELDS_PER_PARSER);
        assert_eq!(shape.fields_dropped, 10, "the rows of the field that did not fit");
        assert_eq!(
            shape.fields.iter().find(|f| f.name == "f0").unwrap().present,
            20,
            "a known field still sums"
        );
    }

    fn memory_store() -> RegistryStore {
        RegistryStore::open(Path::new(":memory:")).unwrap()
    }

    fn log(seq: u64, site: &str) -> crate::heartbeat::LogDto {
        crate::heartbeat::LogDto {
            seq,
            timestamp_ms: 1,
            level: "warn".to_string(),
            site: site.to_string(),
            message: "a source is blocked".to_string(),
            thread: Some("reader-0".to_string()),
            boot_id: None,
            fields: vec![(
                "subject".to_string(),
                crate::heartbeat::FieldValueDto::Str("orders".to_string()),
            )],
        }
    }

    fn kept(store: &RegistryStore) -> i64 {
        store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM logs", [], |r| r.get(0))
            .unwrap()
    }

    /// The group a log was delivered under is stored beside it, including for a worker running nothing --
    /// which is the case the column exists for, since such a worker reports no threads and so no group anywhere
    /// else in its heartbeat.
    #[test]
    fn a_log_keeps_the_group_it_was_delivered_under() {
        let store = memory_store();
        store
            .record_logs("w-1", 7, Some("west"), &[log(0, "worker.commit_not_loadable")])
            .unwrap();
        // A worker too old to state one: no group, rather than a group named "".
        store.record_logs("w-2", 7, None, &[log(0, "worker.started")]).unwrap();

        let groups: Vec<Option<String>> = store
            .conn
            .lock()
            .unwrap()
            .prepare("SELECT group_id FROM logs ORDER BY worker_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(groups, vec![Some("west".to_string()), None]);
    }

    fn at(seq: u64, at_ms: i64, level: &str, site: &str, message: &str) -> crate::heartbeat::LogDto {
        crate::heartbeat::LogDto {
            seq,
            timestamp_ms: at_ms,
            level: level.to_string(),
            site: site.to_string(),
            message: message.to_string(),
            thread: Some("reader-0".to_string()),
            boot_id: None,
            fields: Vec::new(),
        }
    }

    fn seeded() -> RegistryStore {
        let store = memory_store();
        store
            .record_logs(
                "w-1",
                7,
                Some("west"),
                &[
                    at(0, 100, "info", "worker.started", "worker starting"),
                    at(
                        1,
                        200,
                        "warn",
                        "worker.enroll_failed",
                        "enrollment failed, and will be retried",
                    ),
                    at(2, 300, "error", "runtime.rows_rejected", "a destination refused rows"),
                ],
            )
            .unwrap();
        store
            .record_logs(
                "w-2",
                7,
                Some("east"),
                &[at(0, 400, "error", "io.elastic.refused", "100% refused")],
            )
            .unwrap();
        store
    }

    /// One entry per worker and site: the latest row's words and group, beside the earliest row's time. Levels
    /// not asked for and rows outside the window are not read at all.
    #[test]
    fn latest_log_by_site_gives_each_sites_latest_row_and_earliest_time() {
        let store = memory_store();
        store
            .record_logs(
                "w-1",
                7,
                Some("west"),
                &[
                    at(0, 50, "warn", "worker.fetch_failed", "before the window"),
                    at(1, 100, "warn", "worker.fetch_failed", "first"),
                    at(2, 150, "info", "worker.fetch_failed", "not a level asked for"),
                    at(3, 200, "warn", "worker.heartbeat_failed", "other site"),
                ],
            )
            .unwrap();
        // The same site, delivered later under another group.
        store
            .record_logs(
                "w-1",
                8,
                Some("east"),
                &[at(0, 300, "error", "worker.fetch_failed", "latest")],
            )
            .unwrap();
        store
            .record_logs(
                "w-2",
                7,
                None,
                &[at(0, 250, "warn", "worker.fetch_failed", "another worker")],
            )
            .unwrap();

        let levels = ["warn".to_string(), "error".to_string()];
        let mut rows = store.latest_log_by_site(&levels, 100, 1_000).unwrap();
        rows.sort_by(|a, b| (&a.worker_id, &a.site).cmp(&(&b.worker_id, &b.site)));
        #[allow(clippy::type_complexity)]
        let read: Vec<(&str, &str, Option<&str>, &str, &str, i64, i64)> = rows
            .iter()
            .map(|r| {
                (
                    r.worker_id.as_str(),
                    r.site.as_str(),
                    r.group_id.as_deref(),
                    r.level.as_str(),
                    r.message.as_str(),
                    r.at_ms,
                    r.first_ms,
                )
            })
            .collect();
        assert_eq!(
            read,
            vec![
                ("w-1", "worker.fetch_failed", Some("east"), "error", "latest", 300, 100),
                (
                    "w-1",
                    "worker.heartbeat_failed",
                    Some("west"),
                    "warn",
                    "other site",
                    200,
                    200
                ),
                ("w-2", "worker.fetch_failed", None, "warn", "another worker", 250, 250),
            ]
        );
        assert!(store.latest_log_by_site(&[], 0, 1_000).unwrap().is_empty());
    }

    fn sites(rows: &[StoredLog]) -> Vec<&str> {
        rows.iter().map(|r| r.site.as_str()).collect()
    }

    /// Newest first, which is what makes paging safe against the row cap: `maintain` takes the oldest, so the top
    /// of the table never moves under a reader.
    #[test]
    fn an_unfiltered_read_gives_the_newest_first() {
        let rows = seeded().logs(&LogFilter::default(), 10).unwrap();
        assert_eq!(
            sites(&rows),
            vec![
                "io.elastic.refused",
                "runtime.rows_rejected",
                "worker.enroll_failed",
                "worker.started"
            ]
        );
        assert_eq!(rows[0].group_id.as_deref(), Some("east"));
    }

    #[test]
    fn a_limit_takes_the_newest_rather_than_the_first_found() {
        let rows = seeded().logs(&LogFilter::default(), 2).unwrap();
        assert_eq!(sites(&rows), vec!["io.elastic.refused", "runtime.rows_rejected"]);
    }

    /// `before_id`/`after_id` bound `id` directly -- the mechanism the pager rests on. `after_id` is fetched
    /// ascending to keep `LIMIT` on the right edge; this is what proves the result comes back reversed to
    /// newest-first regardless.
    #[test]
    fn before_and_after_id_bound_by_id_rather_than_skip() {
        let store = seeded();
        let all = store.logs(&LogFilter::default(), 10).unwrap();
        let ids: Vec<i64> = all.iter().map(|r| r.id).collect(); // newest first, same order as `all`

        let older = store
            .logs(
                &LogFilter {
                    before_id: Some(ids[0]),
                    ..Default::default()
                },
                10,
            )
            .unwrap();
        assert_eq!(
            sites(&older),
            sites(&all[1..]),
            "everything below the cursor, still newest first"
        );

        let newer = store
            .logs(
                &LogFilter {
                    after_id: Some(ids[2]),
                    ..Default::default()
                },
                10,
            )
            .unwrap();
        assert_eq!(
            sites(&newer),
            sites(&all[..2]),
            "everything above the cursor, restored to newest first"
        );
    }

    /// Levels do not nest: asking for `warn` and `error` together is asking for both by name.
    #[test]
    fn levels_are_named_rather_than_a_floor() {
        let levels = ["warn".to_string(), "error".to_string()];
        let rows = seeded()
            .logs(
                &LogFilter {
                    levels: &levels,
                    ..Default::default()
                },
                10,
            )
            .unwrap();
        assert_eq!(
            sites(&rows),
            vec!["io.elastic.refused", "runtime.rows_rejected", "worker.enroll_failed"]
        );
    }

    #[test]
    fn every_criterion_narrows_and_they_combine() {
        let store = seeded();
        let one = |f: LogFilter| store.logs(&f, 10).unwrap();

        assert_eq!(
            sites(&one(LogFilter {
                worker_id: Some("w-2"),
                ..Default::default()
            })),
            vec!["io.elastic.refused"]
        );
        assert_eq!(
            sites(&one(LogFilter {
                group_id: Some("east"),
                ..Default::default()
            })),
            vec!["io.elastic.refused"]
        );
        // Matched anywhere, not just at the front: "elastic" is in the middle of the one site that has it.
        assert_eq!(
            sites(&one(LogFilter {
                site_contains: Some("elastic"),
                ..Default::default()
            })),
            vec!["io.elastic.refused"]
        );
        assert_eq!(
            sites(&one(LogFilter {
                message_contains: Some("refused"),
                ..Default::default()
            })),
            vec!["io.elastic.refused", "runtime.rows_rejected"]
        );
        assert_eq!(
            sites(&one(LogFilter {
                since_ms: Some(200),
                until_ms: Some(300),
                ..Default::default()
            })),
            vec!["runtime.rows_rejected", "worker.enroll_failed"]
        );
        // Combined, not one winning.
        let levels = ["error".to_string()];
        assert_eq!(
            sites(&one(LogFilter {
                levels: &levels,
                worker_id: Some("w-1"),
                ..Default::default()
            })),
            vec!["runtime.rows_rejected"]
        );
    }

    /// `%` and `_` are `LIKE`'s own wildcards. Searching for one must find the character, not every row.
    #[test]
    fn a_search_for_a_wildcard_character_finds_that_character() {
        let store = seeded();
        let hits = |text: &str| {
            store
                .logs(
                    &LogFilter {
                        message_contains: Some(text),
                        ..Default::default()
                    },
                    10,
                )
                .unwrap()
        };
        assert_eq!(
            sites(&hits("100%")),
            vec!["io.elastic.refused"],
            "the percent is a character here"
        );
        assert!(hits("%").len() == 1, "an unescaped `%` would match every row");
        assert!(
            hits("_").is_empty(),
            "an unescaped `_` would match every row with any character"
        );
    }

    /// What the fleet said is bounded by a row cap rather than an age: a quiet fleet keeps its last words, and a
    /// loud one cannot grow the store without bound.
    #[test]
    fn maintain_keeps_the_newest_logs_and_drops_the_rest() {
        // The cap, lowered rather than reached, so this runs `maintain` itself rather than a copy of its `DELETE`.
        let mut store = memory_store();
        store.logs_kept = 4;

        let events: Vec<_> = (0..10).map(|i| log(i, &format!("site.{i}"))).collect();
        store.record_logs("w-1", 7, Some("prod"), &events).unwrap();
        assert_eq!(kept(&store), 10, "everything a heartbeat carried is written");

        store.maintain(1).unwrap();
        assert_eq!(kept(&store), 4, "past the cap, oldest first");

        let newest: String = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT site FROM logs ORDER BY id DESC LIMIT 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(newest, "site.9", "and the end it keeps is the recent one");
    }

    /// `HeartbeatClient::send` retries once, and its own doc comment says an accumulating downstream needs a key
    /// before it may: a response lost after the leader committed would otherwise write every row twice.
    #[test]
    fn the_same_batch_delivered_twice_is_written_once() {
        let store = memory_store();
        let events: Vec<_> = (0..5).map(|i| log(i, "runtime.rows_rejected")).collect();

        store.record_logs("w-1", 7, Some("prod"), &events).unwrap();
        store.record_logs("w-1", 7, Some("prod"), &events).unwrap();
        assert_eq!(kept(&store), 5, "the retry left the table as the first arrival did");

        // A restart reuses the sequence from zero, and must not be mistaken for that retry.
        store.record_logs("w-1", 8, Some("prod"), &events).unwrap();
        assert_eq!(kept(&store), 10, "a new life of the same worker is new events");

        store.record_logs("w-2", 7, Some("prod"), &events).unwrap();
        assert_eq!(
            kept(&store),
            15,
            "and so is another worker that happens to share a boot id"
        );
    }

    fn temp_path(label: &str) -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("smeltr-series-test-{label}-{}-{n}.db", std::process::id()));
        cleanup(&path);
        path
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        for suffix in ["-wal", "-shm"] {
            let mut p = path.as_os_str().to_owned();
            p.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(p));
        }
    }

    /// What lets a heartbeat's mirror cost one commit rather than one disk sync per sample -- and only in that
    /// pairing: NORMAL is safe from a process crash because the journal is WAL. Asserted against a file, since
    /// an in-memory database has no journal to be in and no disk to sync. Incremental vacuum holds only when it
    /// is asked for before the first write to the file, so the order of the pragmas is asserted with them.
    #[test]
    fn the_connection_opens_at_wal_synchronous_normal_and_incremental_vacuum() {
        let path = std::env::temp_dir().join(format!("smeltr-series-pragmas-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let store = RegistryStore::open(&path).unwrap();
            let conn = store.conn.lock().unwrap();
            let journal: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();
            let level: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0)).unwrap();
            let vacuum: i64 = conn.pragma_query_value(None, "auto_vacuum", |r| r.get(0)).unwrap();
            assert_eq!(journal, "wal");
            assert_eq!(level, 1, "1 is NORMAL; the default, 2, is FULL");
            assert_eq!(
                vacuum, 2,
                "2 is INCREMENTAL; 0 is NONE, what a file fixed before the pragma keeps"
            );
        }
        for suffix in ["", "-wal", "-shm"] {
            let mut p = path.as_os_str().to_owned();
            p.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(p));
        }
    }

    /// A panic under the lock is one bug, not a leader that has stopped answering. This test panics on purpose, so
    /// the "thread panicked" line it prints is the test working.
    #[test]
    fn a_poisoned_connection_still_serves_reads_and_writes() {
        let store = memory_store();
        store
            .record_delta("smeltr_rows_polled_total", "k", 10_000, 5.0)
            .unwrap();

        std::thread::scope(|s| {
            let panicked = s.spawn(|| {
                let _held = store.conn.lock().unwrap();
                panic!("a bug under the lock");
            });
            assert!(panicked.join().is_err());
        });
        assert!(store.conn.is_poisoned());

        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![(10_000, 5.0)],
            "the read a chart or a warning makes"
        );
        store
            .record_delta("smeltr_rows_polled_total", "k", 10_000, 3.0)
            .unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![(10_000, 8.0)],
            "and the write a heartbeat makes"
        );
    }

    /// One call, one transaction, every row: both threads' latest state and both subjects' points.
    #[test]
    fn one_heartbeat_writes_every_thread_and_every_point_it_carried() {
        let store = memory_store();
        let threads = [
            LatestRow {
                thread_id: "reader-0",
                report_json: r#"{"a":1}"#.to_string(),
            },
            LatestRow {
                thread_id: "egress-1",
                report_json: r#"{"a":2}"#.to_string(),
            },
        ];
        let points = [
            SeriesPoint {
                metric: "smeltr_rows_polled_total",
                subject_key: "src".to_string(),
                counter: true,
                value: 5.0,
            },
            SeriesPoint {
                metric: "smeltr_source_lag",
                subject_key: "src".to_string(),
                counter: false,
                value: 12.0,
            },
        ];
        let beat = HeartbeatRows {
            worker_id: "w1",
            threads: &threads,
            points: &points,
            stop_after_unreachable_secs: Some(300),
            commit: Some("c0ffee"),
            smeltr_version: "2026.9.20+main.c0ffee",
            release: Some("1.0.0"),
            supervising_json: Some("{\"phase\":\"running\"}".to_string()),
            authority_expires_millis: Some(99_000),
            stopped_at_ms: None,
            now_ms: 10_000,
        };
        store.record_heartbeat(&beat).unwrap();

        let mut latest = store.load_latest_state().unwrap();
        latest.sort_by(|a, b| a.thread_id.cmp(&b.thread_id));
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0].thread_id, "egress-1");
        assert_eq!(latest[1].report_json, r#"{"a":1}"#);
        assert_eq!(latest[1].commit.as_deref(), Some("c0ffee"));
        assert_eq!(latest[1].smeltr_version, "2026.9.20+main.c0ffee");
        assert_eq!(latest[1].release.as_deref(), Some("1.0.0"));
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "src", Tier::TenSeconds)
                .unwrap(),
            vec![(10_000, 5.0)]
        );
        assert_eq!(
            store.points("smeltr_source_lag", "src", Tier::TenSeconds).unwrap(),
            vec![(10_000, 12.0)]
        );
    }

    /// The batch path folds into an open bucket the way a single point does -- six or seven heartbeats land in each
    /// ten-second bucket, and a counter sums while a gauge keeps its maximum.
    #[test]
    fn heartbeats_landing_in_one_bucket_sum_their_counters_and_max_their_gauges() {
        let store = memory_store();
        let beat = |counter: f64, gauge: f64, now_ms: i64| {
            let points = [
                SeriesPoint {
                    metric: "smeltr_rows_polled_total",
                    subject_key: "src".to_string(),
                    counter: true,
                    value: counter,
                },
                SeriesPoint {
                    metric: "smeltr_source_lag",
                    subject_key: "src".to_string(),
                    counter: false,
                    value: gauge,
                },
            ];
            store
                .record_heartbeat(&HeartbeatRows {
                    worker_id: "w1",
                    threads: &[],
                    points: &points,
                    stop_after_unreachable_secs: None,
                    commit: None,
                    smeltr_version: "",
                    release: None,
                    supervising_json: None,
                    authority_expires_millis: None,
                    stopped_at_ms: None,
                    now_ms,
                })
                .unwrap();
        };
        beat(5.0, 12.0, 10_000);
        beat(3.0, 7.0, 11_500);

        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "src", Tier::TenSeconds)
                .unwrap(),
            vec![(10_000, 8.0)],
            "counters sum"
        );
        assert_eq!(
            store.points("smeltr_source_lag", "src", Tier::TenSeconds).unwrap(),
            vec![(10_000, 12.0)],
            "a gauge keeps the spike, not the last word"
        );
    }

    #[test]
    fn a_delta_counter_sums_within_one_bucket() {
        let store = memory_store();
        store
            .record_delta("smeltr_rows_polled_total", "k", 10_000, 5.0)
            .unwrap();
        store
            .record_delta("smeltr_rows_polled_total", "k", 10_000, 3.0)
            .unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![(10_000, 8.0)]
        );
    }

    #[test]
    fn a_delta_lands_in_the_bucket_its_timestamp_aligns_to() {
        let store = memory_store();
        store
            .record_delta("smeltr_rows_polled_total", "k", 14_999, 5.0)
            .unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![(10_000, 5.0)]
        );
    }

    #[test]
    fn a_gauge_keeps_the_max_within_one_bucket_not_the_sum() {
        let store = memory_store();
        store.record_gauge_max("smeltr_source_lag", "k", 10_000, 5.0).unwrap();
        store.record_gauge_max("smeltr_source_lag", "k", 10_000, 2.0).unwrap();
        assert_eq!(
            store.points("smeltr_source_lag", "k", Tier::TenSeconds).unwrap(),
            vec![(10_000, 5.0)]
        );
    }

    #[test]
    fn a_bucket_nothing_landed_in_is_absent_not_zero() {
        let store = memory_store();
        store.record_delta("smeltr_rows_polled_total", "k", 0, 5.0).unwrap();
        store
            .record_delta("smeltr_rows_polled_total", "k", 20_000, 7.0)
            .unwrap();
        // 10_000, the bucket between them, never got a sample.
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![(0, 5.0), (20_000, 7.0)],
            "no zero-valued row for the skipped bucket"
        );
    }

    #[test]
    fn distinct_subjects_of_the_same_metric_are_separate_series() {
        let store = memory_store();
        store
            .record_delta("smeltr_rows_polled_total", "source-a", 0, 5.0)
            .unwrap();
        store
            .record_delta("smeltr_rows_polled_total", "source-b", 0, 9.0)
            .unwrap();
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "source-a", Tier::TenSeconds)
                .unwrap(),
            vec![(0, 5.0)]
        );
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "source-b", Tier::TenSeconds)
                .unwrap(),
            vec![(0, 9.0)]
        );
    }

    #[test]
    fn for_window_picks_the_finest_tier_whose_retention_covers_it() {
        assert_eq!(Tier::for_window(Duration::from_secs(60)), Tier::TenSeconds);
        assert_eq!(
            Tier::for_window(Duration::from_secs(60 * 60)),
            Tier::TenSeconds,
            "exactly sixty minutes still fits the finest tier"
        );
        assert_eq!(Tier::for_window(Duration::from_secs(60 * 60 + 1)), Tier::OneMinute);
        assert_eq!(Tier::for_window(Duration::from_secs(24 * 60 * 60)), Tier::OneMinute);
        assert_eq!(
            Tier::for_window(Duration::from_secs(24 * 60 * 60 + 1)),
            Tier::FiveMinutes
        );
        assert_eq!(
            Tier::for_window(Duration::from_secs(8 * 24 * 60 * 60)),
            Tier::FiveMinutes
        );
        assert_eq!(
            Tier::for_window(Duration::from_secs(8 * 24 * 60 * 60 + 1)),
            Tier::OneHour
        );
        assert_eq!(Tier::for_window(Duration::from_secs(30 * 24 * 60 * 60)), Tier::OneHour);
        assert_eq!(
            Tier::for_window(Duration::from_secs(365 * 24 * 60 * 60)),
            Tier::OneHour,
            "past thirty days there is still only the coarsest tier to serve from"
        );
    }

    #[test]
    fn series_since_groups_every_subjects_points_ordered_oldest_first() {
        let store = memory_store();
        store
            .record_delta("smeltr_rows_polled_total", "a", 10_000, 2.0)
            .unwrap();
        store.record_delta("smeltr_rows_polled_total", "a", 0, 1.0).unwrap();
        store.record_delta("smeltr_rows_polled_total", "b", 0, 9.0).unwrap();

        let series = store
            .series_since("smeltr_rows_polled_total", Tier::TenSeconds, 0)
            .unwrap();
        assert_eq!(
            series,
            vec![
                ("a".to_string(), vec![(0, 1.0), (10_000, 2.0)]),
                ("b".to_string(), vec![(0, 9.0)]),
            ]
        );
    }

    #[test]
    fn series_since_omits_a_subject_with_nothing_at_or_after_the_cutoff() {
        let store = memory_store();
        store.record_delta("smeltr_rows_polled_total", "a", 0, 1.0).unwrap();
        let series = store
            .series_since("smeltr_rows_polled_total", Tier::TenSeconds, 10_000)
            .unwrap();
        assert!(
            series.is_empty(),
            "the only point is before the cutoff -- absent, not an empty series"
        );
    }

    #[test]
    fn series_since_drops_only_the_points_before_the_cutoff_not_the_whole_subject() {
        let store = memory_store();
        store.record_delta("smeltr_rows_polled_total", "a", 0, 1.0).unwrap();
        store
            .record_delta("smeltr_rows_polled_total", "a", 20_000, 2.0)
            .unwrap();
        let series = store
            .series_since("smeltr_rows_polled_total", Tier::TenSeconds, 10_000)
            .unwrap();
        assert_eq!(series, vec![("a".to_string(), vec![(20_000, 2.0)])]);
    }

    #[test]
    fn maintain_leaves_a_fine_bucket_within_its_window_untouched() {
        let store = memory_store();
        let now = 100_000i64;
        store
            .record_delta("smeltr_rows_polled_total", "k", now - 5_000, 5.0)
            .unwrap();
        store.maintain(now).unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![(Tier::TenSeconds.align(now - 5_000), 5.0)],
            "well within the 60-minute window, so it stays in the fine tier"
        );
    }

    #[test]
    fn a_rollup_sums_counters_into_the_coarse_tier_without_double_counting() {
        let store = memory_store();
        let coarse_bucket = Tier::OneMinute.align(-4_000_000);
        store
            .record_delta("smeltr_rows_polled_total", "k", coarse_bucket, 5.0)
            .unwrap();
        store
            .record_delta("smeltr_rows_polled_total", "k", coarse_bucket + 10_000, 7.0)
            .unwrap();

        store.maintain(0).unwrap();
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "k", Tier::TenSeconds)
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::OneMinute).unwrap(),
            vec![(coarse_bucket, 12.0)]
        );

        // Nothing left in the fine tier to roll a second time.
        store.maintain(0).unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::OneMinute).unwrap(),
            vec![(coarse_bucket, 12.0)],
            "a repeat rollup must not add the same points again"
        );
    }

    #[test]
    fn a_rollup_keeps_the_gauge_maximum_not_the_sum() {
        let store = memory_store();
        let coarse_bucket = Tier::OneMinute.align(-4_000_000);
        store
            .record_gauge_max("smeltr_source_lag", "k", coarse_bucket, 3.0)
            .unwrap();
        store
            .record_gauge_max("smeltr_source_lag", "k", coarse_bucket + 10_000, 9.0)
            .unwrap();
        store
            .record_gauge_max("smeltr_source_lag", "k", coarse_bucket + 20_000, 4.0)
            .unwrap();

        store.maintain(0).unwrap();
        assert_eq!(
            store.points("smeltr_source_lag", "k", Tier::OneMinute).unwrap(),
            vec![(coarse_bucket, 9.0)]
        );
    }

    /// One point, driven through every tier by advancing `now_ms` past each retention window in turn -- proof the
    /// whole pipeline (fine -> minute -> five minutes -> hour -> gone) holds one value the entire way, and that the
    /// last tier really does expire rather than roll up again.
    #[test]
    fn a_point_rolls_all_the_way_to_the_hour_tier_and_then_truly_expires() {
        let store = memory_store();
        store.record_delta("smeltr_rows_polled_total", "k", 0, 42.0).unwrap();

        store.maintain(Tier::TenSeconds.retention_ms() + 1).unwrap();
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "k", Tier::TenSeconds)
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::OneMinute).unwrap(),
            vec![(0, 42.0)]
        );

        store
            .maintain(Tier::OneMinute.retention_ms() + Tier::TenSeconds.retention_ms() + 1)
            .unwrap();
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "k", Tier::OneMinute)
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "k", Tier::FiveMinutes)
                .unwrap(),
            vec![(0, 42.0)]
        );

        store
            .maintain(
                Tier::FiveMinutes.retention_ms() + Tier::OneMinute.retention_ms() + Tier::TenSeconds.retention_ms() + 1,
            )
            .unwrap();
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "k", Tier::FiveMinutes)
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::OneHour).unwrap(),
            vec![(0, 42.0)]
        );

        let far_future = Tier::OneHour.retention_ms()
            + Tier::FiveMinutes.retention_ms()
            + Tier::OneMinute.retention_ms()
            + Tier::TenSeconds.retention_ms()
            + 1;
        store.maintain(far_future).unwrap();
        assert_eq!(
            store
                .points("smeltr_rows_polled_total", "k", Tier::OneHour)
                .unwrap()
                .len(),
            0,
            "past 30 days there is nowhere left to roll to"
        );
        let series: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM series", [], |r| r.get(0))
            .unwrap();
        assert_eq!(series, 0, "a series with no point left is gone too");
    }

    /// One thread's state, written by the heartbeat it arrived on.
    fn record_latest(
        store: &RegistryStore,
        thread_id: &str,
        report_json: &str,
        stop_after_unreachable_secs: Option<u64>,
        commit: Option<&str>,
        authority_expires_millis: Option<i64>,
        now_ms: i64,
    ) {
        store
            .record_heartbeat(&HeartbeatRows {
                worker_id: "w1",
                threads: &[LatestRow {
                    thread_id,
                    report_json: report_json.to_string(),
                }],
                points: &[],
                stop_after_unreachable_secs,
                commit,
                smeltr_version: "",
                release: None,
                supervising_json: None,
                authority_expires_millis,
                stopped_at_ms: None,
                now_ms,
            })
            .unwrap();
    }

    #[test]
    fn latest_state_round_trips_through_record_and_load() {
        let store = memory_store();
        record_latest(
            &store,
            "reader-0",
            "{\"threadId\":\"reader-0\"}",
            Some(300),
            Some("c0ffee"),
            Some(99_000),
            12_345,
        );
        assert_eq!(
            store.load_latest_state().unwrap(),
            vec![LatestState {
                worker_id: "w1".to_string(),
                thread_id: "reader-0".to_string(),
                report_json: "{\"threadId\":\"reader-0\"}".to_string(),
                stop_after_unreachable_secs: Some(300),
                commit: Some("c0ffee".to_string()),
                smeltr_version: String::new(),
                release: None,
                supervising_json: None,
                authority_expires_millis: Some(99_000),
                stopped_at_ms: None,
                last_seen_ms: 12_345,
            }]
        );
    }

    #[test]
    fn recording_latest_state_again_for_the_same_thread_replaces_it() {
        let store = memory_store();
        record_latest(&store, "reader-0", "old", Some(60), Some("c0ffee"), Some(99_000), 1);
        record_latest(&store, "reader-0", "new", None, None, None, 2);
        let rows = store.load_latest_state().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].report_json, "new");
        assert_eq!(rows[0].stop_after_unreachable_secs, None);
        assert_eq!(
            rows[0].commit, None,
            "every column the upsert writes, not only the report"
        );
        assert_eq!(rows[0].last_seen_ms, 2);
    }

    #[test]
    fn fact_clock_sync_inserts_new_and_removes_gone_facts() {
        let store = memory_store();
        store
            .sync_fact_clock(&[], &[("source_blocked".to_string(), "7".to_string())], 1_000)
            .unwrap();
        assert_eq!(
            store.load_fact_clock().unwrap(),
            vec![("source_blocked".to_string(), "7".to_string(), 1_000)]
        );

        store
            .sync_fact_clock(&[("source_blocked".to_string(), "7".to_string())], &[], 2_000)
            .unwrap();
        assert!(store.load_fact_clock().unwrap().is_empty());
    }

    #[test]
    fn a_fact_that_ends_is_kept_with_its_span_until_the_last_tier_forgets_it() {
        let store = memory_store();
        let fact = || vec![("source_blocked".to_string(), "7".to_string())];
        store.sync_fact_clock(&[], &fact(), 1_000).unwrap();
        store.sync_fact_clock(&fact(), &[], 2_000).unwrap();
        store.sync_fact_clock(&[], &fact(), 3_000).unwrap();

        let spans = store.fact_spans(&["source_blocked"], "7", 0).unwrap();
        assert_eq!(
            spans,
            vec![
                ("source_blocked".to_string(), 1_000, Some(2_000)),
                ("source_blocked".to_string(), 3_000, None),
            ],
            "the ended span and the active one, oldest first"
        );
        assert_eq!(
            store.fact_spans(&["source_blocked"], "7", 2_500).unwrap().len(),
            1,
            "one that ended before"
        );
        assert!(
            store.fact_spans(&["source_unreachable"], "7", 0).unwrap().is_empty(),
            "another entry"
        );
        assert_eq!(
            store.facts_ended_since(&["source_blocked"], 2_000).unwrap(),
            vec!["7".to_string()]
        );
        assert!(
            store.facts_ended_since(&["source_blocked"], 2_001).unwrap().is_empty(),
            "ended before"
        );
        assert!(
            store.facts_ended_since(&["source_unreachable"], 0).unwrap().is_empty(),
            "another entry"
        );

        store.maintain(2_000 + Tier::OneHour.retention_ms() + 1).unwrap();
        assert_eq!(
            store.fact_spans(&["source_blocked"], "7", 0).unwrap(),
            vec![("source_blocked".to_string(), 3_000, None)],
            "past the last tier, an ended span is gone; an active one stays"
        );
    }

    #[test]
    fn fact_clock_sync_does_not_overwrite_an_existing_start() {
        let store = memory_store();
        store
            .sync_fact_clock(&[], &[("source_blocked".to_string(), "7".to_string())], 1_000)
            .unwrap();
        // Inserted again, as `Registry::observe_facts` would while the fact is still active.
        store
            .sync_fact_clock(&[], &[("source_blocked".to_string(), "7".to_string())], 9_000)
            .unwrap();
        assert_eq!(store.load_fact_clock().unwrap()[0].2, 1_000);
    }

    #[test]
    fn reopening_an_existing_database_does_not_fail_or_lose_data() {
        let path = temp_path("reopen");
        {
            let store = RegistryStore::open(&path).unwrap();
            store.record_delta("smeltr_rows_polled_total", "k", 0, 5.0).unwrap();
            store
                .sync_fact_clock(&[], &[("source_blocked".to_string(), "7".to_string())], 1_000)
                .unwrap();
        }
        let store = RegistryStore::open(&path).unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![(0, 5.0)]
        );
        assert_eq!(
            store.load_fact_clock().unwrap(),
            vec![("source_blocked".to_string(), "7".to_string(), 1_000)]
        );
        cleanup(&path);
    }

    /// A file SQLite cannot read is set aside, byte for byte, and the store opens fresh in its place.
    #[test]
    fn a_damaged_file_is_set_aside_and_started_fresh() {
        let path = temp_path("damaged");
        {
            let store = RegistryStore::open(&path).unwrap();
            store.record_delta("smeltr_rows_polled_total", "k", 0, 5.0).unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[..100].fill(0xA5);
        std::fs::write(&path, &bytes).unwrap();

        let store = RegistryStore::open(&path).unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![]
        );
        assert_eq!(store.load_latest_state().unwrap(), vec![]);

        let dir = path.parent().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        let kept: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with(&format!("{name}.damaged-"))
            })
            .collect();
        assert_eq!(kept.len(), 1, "{kept:?}");
        assert_eq!(std::fs::read(&kept[0]).unwrap(), bytes);
        drop(store);
        cleanup(&path);
        let _ = std::fs::remove_file(&kept[0]);
    }

    /// Step 2 drops the points a file kept under the text-keyed layout, and the file takes new ones in the new
    /// layout straight after.
    #[test]
    fn a_file_from_step_1_drops_its_points_and_takes_new_ones() {
        let path = temp_path("step-1");
        {
            let mut conn = Connection::open(&path).unwrap();
            super::super::steps::upgrade_sqlite(&mut conn, "registry.db", &STEPS[..1]).unwrap();
            conn.execute(
                "INSERT INTO series_points (tier, metric, labels_json, bucket_ms, kind, value)
                 VALUES (0, 'smeltr_rows_polled_total', 'k', 0, 'counter', 5.0)",
                [],
            )
            .unwrap();
        }
        let store = RegistryStore::open(&path).unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![]
        );
        store.record_delta("smeltr_rows_polled_total", "k", 0, 3.0).unwrap();
        assert_eq!(
            store.points("smeltr_rows_polled_total", "k", Tier::TenSeconds).unwrap(),
            vec![(0, 3.0)]
        );
        cleanup(&path);
    }

    /// Step 8 renames each stored series by the Prometheus standard, and a time's points read in seconds.
    #[test]
    fn a_file_from_step_7_keeps_its_series_under_their_new_names() {
        let path = temp_path("step-7");
        {
            let mut conn = Connection::open(&path).unwrap();
            super::super::steps::upgrade_sqlite(&mut conn, "registry.db", &STEPS[..7]).unwrap();
            for (id, metric, kind, value) in [
                (1, "smeltr_rows_polled", COUNTER, 5.0),
                (2, "smeltr_tick_poll_micros", COUNTER, 2_500_000.0),
                (3, "smeltr_destination_rtt_ms", GAUGE, 120.0),
                (4, "smeltr_destination_queue_bytes", GAUGE, 64.0),
            ] {
                conn.execute(
                    "INSERT INTO series (id, metric, labels_json, kind) VALUES (?1, ?2, 'k', ?3)",
                    params![id, metric, kind],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO series_points (series_id, tier, bucket_ms, value) VALUES (?1, 0, 0, ?2)",
                    params![id, value],
                )
                .unwrap();
            }
        }
        let store = RegistryStore::open(&path).unwrap();
        for (metric, value) in [
            ("smeltr_rows_polled_total", 5.0),
            ("smeltr_tick_poll_seconds_total", 2.5),
            ("smeltr_destination_rtt_seconds", 0.12),
            ("smeltr_destination_queue_bytes", 64.0),
        ] {
            assert_eq!(
                store.points(metric, "k", Tier::TenSeconds).unwrap(),
                vec![(0, value)],
                "{metric}"
            );
        }
        assert_eq!(
            store.points("smeltr_rows_polled", "k", Tier::TenSeconds).unwrap(),
            vec![]
        );
        cleanup(&path);
    }
}
