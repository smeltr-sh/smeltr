//! In-memory sources and destinations for tests.
//!
//! Test-only: the crate's unit tests reach it as `crate::fake`, and `tests/harness` compiles this same file as its
//! own `fake`. The module that holds it must have `io`, `plan` and `sync` as its own items, as the crate root does and
//! `tests/harness` does by importing them from `smeltr`.

use super::io::parquet::Columns;
use super::io::{
    BatchCompletion, BatchId, CommitMap, Destination, Dispatched, IoError, OwnedRecord, Rejection, Source,
    SourceRecord, TopicPartition, Undelivered,
};
use super::plan::config::Payload;
use super::sync::Mutex;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct SourceState {
    pending: VecDeque<SourceRecord>,
    committed: Vec<CommitMap>,
    blocked: bool,
    /// How many further `commit` calls should fail.
    fail_commits: usize,
    /// `keep_membership` calls so far.
    membership_kept: usize,
    /// Partitions `revoke` took, until `take_revoked` hands them over.
    revoked: Vec<TopicPartition>,
}

/// Replays a fixed queue of records, never blocking, and records `commit` calls. `Arc`-backed so a test keeps a
/// handle after boxing a clone into a worker.
#[derive(Clone, Default)]
pub struct FakeSource(Arc<Mutex<SourceState>>);

impl FakeSource {
    pub fn new(records: impl IntoIterator<Item = SourceRecord>) -> Self {
        FakeSource(Arc::new(Mutex::new(SourceState {
            pending: records.into_iter().collect(),
            committed: Vec::new(),
            blocked: false,
            fail_commits: 0,
            membership_kept: 0,
            revoked: Vec::new(),
        })))
    }

    /// Every `commit` call so far, in order.
    pub fn committed(&self) -> Vec<CommitMap> {
        self.0.lock().committed.clone()
    }

    pub fn is_blocked(&self) -> bool {
        self.0.lock().blocked
    }

    /// Move `partition` to another member of the group, as a rebalance does.
    pub fn revoke(&self, partition: TopicPartition) {
        self.0.lock().revoked.push(partition);
    }

    /// How many times the reader kept this source's membership in place of polling it.
    pub fn membership_kept(&self) -> usize {
        self.0.lock().membership_kept
    }

    /// Make the next `n` `commit` calls fail.
    pub fn fail_commits(&self, n: usize) {
        self.0.lock().fail_commits = n;
    }

    /// Put records back for the next poll.
    pub fn requeue(&self, records: impl IntoIterator<Item = SourceRecord>) {
        self.0.lock().pending.extend(records);
    }
}

impl Source for FakeSource {
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        _timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        let mut s = self.0.lock();
        let mut out = Vec::new();
        // A blocked source keeps getting polled and hands back nothing, as a paused Kafka consumer does.
        if s.blocked {
            return Ok(out);
        }
        let mut bytes = 0usize;
        while out.len() < max_rows {
            let Some(r) = s.pending.front() else {
                break;
            };
            if !out.is_empty() && bytes + r.size() > max_bytes {
                break;
            }
            bytes += r.size();
            out.push(s.pending.pop_front().unwrap());
        }
        Ok(out)
    }

    fn block(&mut self) -> Result<(), IoError> {
        self.0.lock().blocked = true;
        Ok(())
    }

    fn keep_membership(&mut self, _timeout: Duration) -> Result<(), IoError> {
        self.0.lock().membership_kept += 1;
        Ok(())
    }

    fn take_revoked(&mut self) -> Vec<TopicPartition> {
        std::mem::take(&mut self.0.lock().revoked)
    }

    fn release(&mut self) -> Result<(), IoError> {
        self.0.lock().blocked = false;
        Ok(())
    }

    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
        let mut s = self.0.lock();
        if s.fail_commits > 0 {
            s.fail_commits -= 1;
            return Err(IoError::transient("commit refused".to_string()));
        }
        s.committed.push(offsets.clone());
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SentRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub payload: Payload,
    pub placement: Option<String>,
    pub doc_id: Option<String>,
    pub kept: Vec<(Payload, Arc<[u8]>)>,
    pub columns: Option<Columns>,
}

#[derive(Default)]
struct DestinationState {
    sent: Vec<SentRecord>,
    flushes: usize,
    fail_sends: usize,
    reject: bool,
    flush_rejections: Vec<Rejection>,
    fail_flushes: usize,
}

/// A destination that acknowledges out of band: `send` returns without waiting and a batch completes only when
/// the test says so. `FakeDestination` completes every batch inside `send`, so it cannot exercise anything in
/// flight.
#[derive(Clone, Default)]
pub struct AsyncDestination(Arc<Mutex<AsyncState>>);

#[derive(Default)]
struct AsyncState {
    dispatched: Vec<BatchId>,
    ready: Vec<BatchCompletion>,
    sent: usize,
    /// Every batch `send` took, with the rows it carried.
    units: Vec<(BatchId, Vec<Vec<u8>>)>,
    /// The same rows whole, as `egress` keeps them to hand back with a refusal.
    records: Vec<(BatchId, Vec<OwnedRecord>)>,
    fail_next_poll: Option<String>,
    fail_next_deliver: Option<String>,
    connection_error: Option<String>,
}

impl AsyncDestination {
    pub fn new() -> Self {
        AsyncDestination::default()
    }

    /// Batches handed over and not yet acknowledged.
    pub fn outstanding(&self) -> Vec<BatchId> {
        self.0.lock().dispatched.clone()
    }

    pub fn sent(&self) -> usize {
        self.0.lock().sent
    }

    /// Every batch `send` took, in order, with the row values it carried.
    pub fn units(&self) -> Vec<(BatchId, Vec<Vec<u8>>)> {
        self.0.lock().units.clone()
    }

    /// Acknowledge `batch`, in any order.
    pub fn complete(&self, batch: BatchId) {
        self.complete_with(batch, Vec::new());
    }

    /// Acknowledge `batch`, reporting records it refused.
    pub fn complete_with(&self, batch: BatchId, mut rejections: Vec<Rejection>) {
        let mut st = self.0.lock();
        st.dispatched.retain(|b| *b != batch);
        // Each refusal gets its document back from the latest unit taken under `batch`, as egress does.
        if let Some((_, rows)) = st.records.iter().rev().find(|(b, _)| *b == batch) {
            for r in &mut rejections {
                if r.record.is_none() {
                    r.record = rows.get(r.doc).cloned();
                }
            }
        }
        st.ready.push(BatchCompletion {
            batch,
            rejections,
            throttled: false,
            undelivered: None,
        });
    }

    /// Answer `batch` with `rows` of it undelivered, for `why`, and `rejections` refused: the rest were delivered.
    pub fn fail_rows(&self, batch: BatchId, rows: Vec<usize>, rejections: Vec<Rejection>, why: &str) {
        let mut st = self.0.lock();
        st.dispatched.retain(|b| *b != batch);
        st.ready.push(BatchCompletion {
            batch,
            rejections,
            throttled: false,
            undelivered: Some(Undelivered {
                error: IoError::transient(why),
                rows,
            }),
        });
    }

    /// Answer `batch` as the service asking to be sent less, `wait` first, of its `rows`.
    pub fn throttle_rows(&self, batch: BatchId, rows: Vec<usize>, wait: Duration) {
        let mut st = self.0.lock();
        st.dispatched.retain(|b| *b != batch);
        st.ready.push(BatchCompletion {
            batch,
            rejections: Vec::new(),
            throttled: true,
            undelivered: Some(Undelivered {
                error: IoError::throttled("asked to be sent less", wait),
                rows,
            }),
        });
    }

    /// Every batch `send` took, in order, with the rows it carried whole.
    pub fn records(&self) -> Vec<(BatchId, Vec<OwnedRecord>)> {
        self.0.lock().records.clone()
    }

    /// Make the next `poll_completions` report a delivery failure.
    pub fn fail_next_poll(&self, msg: &str) {
        self.0.lock().fail_next_poll = Some(msg.to_string());
    }

    /// Make the next `send` fail, as a full producer queue does.
    pub fn fail_next_deliver(&self, msg: &str) {
        self.0.lock().fail_next_deliver = Some(msg.to_string());
    }

    /// What `connection_error` answers from now on, as a client that loses and regains its brokers does.
    pub fn set_connection_error(&self, error: Option<&str>) {
        self.0.lock().connection_error = error.map(str::to_string);
    }
}

impl Destination for AsyncDestination {
    fn send(&mut self, batch: BatchId, rows: Cow<'_, [OwnedRecord]>) -> Result<(Dispatched, Vec<Rejection>), IoError> {
        let mut st = self.0.lock();
        st.sent += rows.len();
        if let Some(msg) = st.fail_next_deliver.take() {
            return Err(IoError::transient(msg));
        }
        st.units.push((batch, rows.iter().map(|r| r.value.clone()).collect()));
        st.records.push((batch, rows.into_owned()));
        st.dispatched.push(batch);
        Ok((Dispatched::InFlight, Vec::new()))
    }

    fn flush(&mut self) -> Result<(), IoError> {
        let mut st = self.0.lock();
        let pending = std::mem::take(&mut st.dispatched);
        st.ready.extend(pending.into_iter().map(|batch| BatchCompletion {
            batch,
            rejections: Vec::new(),
            throttled: false,
            undelivered: None,
        }));
        Ok(())
    }

    /// As a file destination does: what it holds is acknowledged without waiting for `complete`.
    fn finish(&mut self) {
        let _ = self.flush();
    }

    fn poll_completions(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        let mut st = self.0.lock();
        if let Some(msg) = st.fail_next_poll.take() {
            // What was outstanding is forgotten, as the Kafka
            // destination's tracker does on a delivery failure.
            st.dispatched.clear();
            st.ready.clear();
            return Err(IoError::transient(msg));
        }
        Ok(std::mem::take(&mut st.ready))
    }

    fn connection_error(&self) -> Option<String> {
        self.0.lock().connection_error.clone()
    }
}

/// Collects every row verbatim and completes each batch inside `send`.
#[derive(Clone, Default)]
pub struct FakeDestination(Arc<Mutex<DestinationState>>);

impl FakeDestination {
    pub fn new() -> Self {
        FakeDestination::default()
    }

    /// Every row taken so far, in order.
    pub fn sent(&self) -> Vec<SentRecord> {
        self.0.lock().sent.clone()
    }

    pub fn flushes(&self) -> usize {
        self.0.lock().flushes
    }

    /// Make the next `n` rows handed over fail transiently. A batch with any of them fails whole.
    pub fn fail_next_sends(&self, n: usize) {
        self.0.lock().fail_sends = n;
    }

    /// A destination whose every hand-over fails.
    pub fn always_failing() -> Self {
        let d = FakeDestination::default();
        d.fail_next_sends(usize::MAX);
        d
    }

    /// Make the next `n` rows handed over be refused.
    pub fn reject_next_sends(&self, n: usize) {
        let mut st = self.0.lock();
        st.fail_sends = n;
        st.reject = true;
    }

    /// Refuse these positions of the next batch once it is flushed, as a batched acknowledgment does.
    pub fn reject_on_flush(&self, docs: &[usize]) {
        self.0.lock().flush_rejections = docs
            .iter()
            .map(|&doc| Rejection {
                doc,
                reason: "fake refusal".into(),
                record: None,
            })
            .collect();
    }

    /// Make the next `n` `flush` calls fail: accepted, never drained.
    pub fn fail_next_flushes(&self, n: usize) {
        self.0.lock().fail_flushes = n;
    }

    /// A destination that takes records and never acknowledges them.
    pub fn stalled() -> Self {
        let d = FakeDestination::default();
        d.fail_next_flushes(usize::MAX);
        d
    }
}

impl Destination for FakeDestination {
    fn send(&mut self, _batch: BatchId, rows: Cow<'_, [OwnedRecord]>) -> Result<(Dispatched, Vec<Rejection>), IoError> {
        let mut rejections = Vec::new();
        let mut failed = None;
        {
            let mut st = self.0.lock();
            for (i, row) in rows.iter().enumerate() {
                if let Some(why) = &row.unfit {
                    rejections.push(Rejection::of(&rows, i, why.as_str()));
                    continue;
                }
                if st.fail_sends > 0 {
                    st.fail_sends -= 1;
                    if st.reject {
                        rejections.push(Rejection::of(&rows, i, "fake destination: record refused"));
                    } else {
                        failed = Some(IoError::transient("fake destination: send failed"));
                    }
                    continue;
                }
                st.sent.push(SentRecord {
                    key: row.key.clone(),
                    value: row.value.clone(),
                    payload: row.payload.clone(),
                    placement: row.placement.clone(),
                    doc_id: row.doc_id.clone(),
                    kept: row.kept.clone(),
                    columns: row.columns.clone(),
                });
            }
        }
        if let Some(e) = failed {
            return Err(e);
        }
        self.flush()?;
        for mut r in std::mem::take(&mut self.0.lock().flush_rejections) {
            r.record = rows.get(r.doc).cloned();
            rejections.push(r);
        }
        Ok((Dispatched::Complete, rejections))
    }

    fn flush(&mut self) -> Result<(), IoError> {
        let mut st = self.0.lock();
        if st.fail_flushes > 0 {
            st.fail_flushes = st.fail_flushes.saturating_sub(1);
            return Err(IoError::transient(
                "fake destination: stalled, flush timed out".to_string(),
            ));
        }
        st.flushes += 1;
        Ok(())
    }
}
