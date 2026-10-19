//! The commit stage: which batches are outstanding, which destinations still owe an acknowledgment, and what may be
//! recorded as progress.
//!
//! One component rather than methods on the reader because the rule it enforces -- a commit is a watermark, so nothing
//! commits past a batch still outstanding -- is one invariant, and its bugs are silent loss.
//!
//! It never sees rows. Egress keeps what may have to be re-sent; a batch here is its ack tokens and the destinations
//! yet to report it.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::id::DestId;
use crate::io::{BatchId, CommitMap, TopicPartition};

/// A batch handed to its destinations and not yet acknowledged by all of them.
struct Tracked {
    id: BatchId,
    tokens: CommitMap,
    waiting: HashSet<DestId>,
    /// What the batch read from its source.
    bytes: usize,
}

/// What may be committed, and what is still owed.
#[derive(Default)]
pub struct CommitTracker {
    /// Oldest first: `retire` walks a prefix of it.
    in_flight: VecDeque<Tracked>,
    /// Their bytes, summed.
    bytes: usize,
}

/// Ids are unique across the process rather than per tracker: a completion carries only the id, and a transform thread
/// draining more than one reader has to tell whose batch came back.
fn next_batch_id() -> BatchId {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// What a batch that reaches no destination waits on instead: transform, until it has finished with the batch. Waiting on
/// nothing, the batch would retire at poll time, and a batch transform holds would commit and release its source.
pub const TRANSFORM: DestId = DestId::MAX;

/// One retirement: the merged watermark to commit and how many batches it covers.
pub struct Retired {
    pub tokens: CommitMap,
    pub batches: usize,
}

impl CommitTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Claim the next batch id. Increasing, never dense: `retire`'s prefix rule walks `in_flight` in push order.
    pub fn next_id(&self) -> BatchId {
        next_batch_id()
    }

    /// Record a batch as outstanding, at poll time and in poll order -- what makes [`retire`](Self::retire)'s prefix a
    /// watermark. Tracked at dispatch instead, a later batch that transform finishes first could commit its offsets
    /// past an outstanding earlier one.
    ///
    /// `waiting` is every destination reachable from this source: all of them are dispatched, and one with nothing
    /// outstanding completes immediately. Empty, the batch waits on [`TRANSFORM`]. `bytes` is what the batch read.
    pub fn track(&mut self, id: BatchId, tokens: CommitMap, mut waiting: HashSet<DestId>, bytes: usize) {
        if waiting.is_empty() {
            waiting.insert(TRANSFORM);
        }
        self.bytes += bytes;
        self.in_flight.push_back(Tracked {
            id,
            tokens,
            waiting,
            bytes,
        });
    }

    /// Give up on delivering a batch through its routes (the unprocessable path): stop waiting on every destination but
    /// `waits_on`, where it was sent instead. Kept rather than removed, so its offsets commit in poll order rather than
    /// riding on whatever comes next. A destination in `waits_on` that has already acknowledged stays done.
    pub fn abandon(&mut self, id: BatchId, waits_on: &[DestId]) {
        if let Some(f) = self.in_flight.iter_mut().find(|f| f.id == id) {
            f.waiting.retain(|d| waits_on.contains(d));
        }
    }

    /// `batch` waits on `dest` again, for rows sealed there after the batch was dispatched. Unknown ids are ignored.
    pub fn owe(&mut self, batch: BatchId, dest: DestId) {
        if let Some(f) = self.in_flight.iter_mut().find(|f| f.id == batch) {
            f.waiting.insert(dest);
        }
    }

    /// `dest` has finished with `batch`. Unknown ids are ignored.
    pub fn acknowledge(&mut self, batch: BatchId, dest: DestId) {
        if let Some(f) = self.in_flight.iter_mut().find(|f| f.id == batch) {
            f.waiting.remove(&dest);
        }
    }

    /// The longest run of batches, from the oldest, that every destination has finished with, merged into one
    /// watermark. Committing batch 7 asserts everything before it is done, so a batch that finishes early waits for its
    /// elders.
    pub fn retire(&mut self) -> Option<Retired> {
        let mut tokens = CommitMap::new();
        let mut batches = 0usize;
        while let Some(front) = self.in_flight.front() {
            if !front.waiting.is_empty() {
                break;
            }
            for (tp, off) in &front.tokens {
                tokens
                    .entry(tp.clone())
                    .and_modify(|o| *o = (*o).max(*off))
                    .or_insert(*off);
            }
            self.bytes -= front.bytes;
            self.in_flight.pop_front();
            batches += 1;
        }
        (batches > 0).then_some(Retired { tokens, batches })
    }

    /// The source no longer owns `tp`: no batch polled before now may commit an offset there. Its new owner reads
    /// from the last committed offset, and may already be past these; one of them committed late would move it back.
    /// The batches stay, and commit their other partitions as before.
    pub fn forget(&mut self, tp: &TopicPartition) {
        for f in &mut self.in_flight {
            f.tokens.remove(tp);
        }
    }

    /// Outstanding batch ids, oldest first.
    pub fn outstanding(&self) -> impl Iterator<Item = BatchId> + '_ {
        self.in_flight.iter().map(|f| f.id)
    }

    pub fn depth(&self) -> usize {
        self.in_flight.len()
    }

    /// What the outstanding batches read from their source.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TopicPartition;

    fn tokens(partition: i32, offset: i64) -> CommitMap {
        let mut m = CommitMap::new();
        m.insert(TopicPartition::new("t".into(), partition), offset);
        m
    }

    fn track_one(t: &mut CommitTracker, offset: i64, waiting: &[u32]) -> BatchId {
        let id = t.next_id();
        t.track(id, tokens(0, offset), waiting.iter().copied().collect(), 0);
        id
    }

    /// A partition the source lost commits nothing from the batches polled before, however they finish. Their other
    /// partitions still commit.
    #[test]
    fn a_forgotten_partition_commits_nothing_from_batches_already_polled() {
        let mut t = CommitTracker::new();
        let id = t.next_id();
        let mut both = tokens(0, 100);
        both.extend(tokens(1, 50));
        t.track(id, both, [0].into_iter().collect(), 0);

        t.forget(&TopicPartition::new("t".into(), 0));
        t.acknowledge(id, 0);
        let retired = t.retire().expect("the batch still retires");
        assert_eq!(retired.tokens, tokens(1, 50), "partition 0 is someone else's now");
    }

    /// Poll order is what the prefix rule is a prefix of.
    #[test]
    fn a_batch_dispatched_first_cannot_commit_past_an_earlier_one() {
        let mut t = CommitTracker::new();
        // Polled in offset order, as one reader always polls them.
        let first = track_one(&mut t, 100, &[0]);
        let second = track_one(&mut t, 200, &[0]);

        // Reached its destination first, and finished first.
        t.acknowledge(second, 0);
        assert!(
            t.retire().is_none(),
            "offset 200 committed while 100 was still outstanding"
        );

        t.acknowledge(first, 0);
        let retired = t.retire().expect("both are done");
        assert_eq!(retired.batches, 2);
        assert_eq!(
            retired.tokens[&TopicPartition::new("t".into(), 0)],
            200,
            "the watermark is the newest offset of the run, once the run is whole"
        );
    }

    /// An abandoned batch sent to a DLQ waits for that DLQ alone, whichever comes first: the acknowledgment or the
    /// abandonment.
    #[test]
    fn an_abandoned_batch_waits_for_where_it_was_sent_instead() {
        let mut t = CommitTracker::new();
        let first = track_one(&mut t, 10, &[0, 1, 2]);
        t.abandon(first, &[2]);
        assert!(t.retire().is_none(), "destination 2 still has it");
        t.acknowledge(first, 2);
        assert_eq!(t.retire().expect("2 has acknowledged").batches, 1);

        let second = track_one(&mut t, 20, &[0, 1, 2]);
        t.acknowledge(second, 2);
        t.abandon(second, &[2]);
        assert_eq!(
            t.retire()
                .expect("an acknowledgment before the abandonment counts")
                .batches,
            1
        );
    }

    /// An abandoned batch still has offsets to commit, in its own place.
    #[test]
    fn an_abandoned_batch_commits_in_its_own_place() {
        let mut t = CommitTracker::new();
        let poisoned = track_one(&mut t, 10, &[0]);
        let after = track_one(&mut t, 20, &[0]);

        t.acknowledge(after, 0);
        assert!(t.retire().is_none(), "the poisoned batch is still ahead");

        t.abandon(poisoned, &[]);
        let retired = t.retire().expect("nothing is in the way now");
        assert_eq!(
            retired.batches, 2,
            "the abandoned batch retires in poll order, carrying the one behind it"
        );
        assert_eq!(
            retired.tokens[&TopicPartition::new("t".into(), 0)],
            20,
            "and the watermark reaches the newest of the run"
        );
        assert!(t.is_empty());
    }

    /// A batch is done only when *every* destination it reached says so.
    #[test]
    fn one_silent_destination_holds_the_whole_batch() {
        let mut t = CommitTracker::new();
        let id = track_one(&mut t, 10, &[0, 1]);

        t.acknowledge(id, 0);
        assert!(t.retire().is_none());

        t.acknowledge(id, 1);
        assert_eq!(t.retire().expect("now").batches, 1);
    }

    /// An acknowledgment for an unknown batch must not retire a live one.
    #[test]
    fn a_completion_for_a_batch_that_was_never_tracked_is_ignored() {
        let mut t = CommitTracker::new();
        let live = track_one(&mut t, 20, &[0]);

        t.acknowledge(live + 999, 0);
        assert!(
            t.retire().is_none(),
            "an acknowledgment for an unknown batch must not retire a live one"
        );

        t.acknowledge(live, 0);
        assert_eq!(t.retire().expect("the live batch").batches, 1);
    }

    /// A late completion cannot land on a batch reusing the same id.
    #[test]
    fn batch_ids_are_never_reused() {
        let mut t = CommitTracker::new();
        let first = track_one(&mut t, 10, &[1]);
        t.acknowledge(first, 1);
        assert_eq!(t.retire().expect("done").batches, 1);
        let second = track_one(&mut t, 20, &[]);
        assert_ne!(first, second, "a retired id must not come round again");
    }

    /// Two readers hand batches to one transform thread, which tells them apart by id alone.
    #[test]
    fn two_trackers_never_mint_the_same_id() {
        let a = CommitTracker::new();
        let b = CommitTracker::new();
        let ids: Vec<BatchId> = (0..4).flat_map(|_| [a.next_id(), b.next_id()]).collect();
        let mut unique = ids.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), ids.len(), "{ids:?} repeats an id");
    }

    /// A batch transform holds must not commit, so one that reaches no destination waits on transform.
    #[test]
    fn a_batch_reaching_no_destination_waits_on_transform() {
        let mut t = CommitTracker::new();
        let id = track_one(&mut t, 10, &[]);
        assert!(t.retire().is_none(), "not done on arrival");
        t.acknowledge(id, TRANSFORM);
        assert_eq!(t.retire().expect("done once transform is").batches, 1);
    }

    /// Watermarks are per partition.
    #[test]
    fn each_partition_keeps_its_own_watermark() {
        let mut t = CommitTracker::new();
        let id = t.next_id();
        let mut m = tokens(0, 10);
        m.insert(TopicPartition::new("t".into(), 1), 99);
        t.track(id, m, HashSet::new(), 0);
        t.acknowledge(id, TRANSFORM);

        let r = t.retire().expect("done");
        assert_eq!(r.tokens[&TopicPartition::new("t".into(), 0)], 10);
        assert_eq!(r.tokens[&TopicPartition::new("t".into(), 1)], 99);
    }
}
