//! The reads a source has handed out and not yet seen committed whole: each a partition of its own, retired in the
//! order they were made once every record of it is committed, and what each carries, a cursor or a ledger's
//! movement, taken as progress only then. Progress so moves only past reads delivered whole and in order, so a
//! failover repeats records and never loses one.

use std::collections::VecDeque;
use std::sync::Arc;

use super::CommitMap;

/// One read, until every record of it is committed.
struct Read<C> {
    partition: i32,
    topic: Arc<str>,
    /// Records handed out so far.
    handed: i64,
    /// Whether every record of it is handed out.
    exhausted: bool,
    /// The highest offset committed. -1 is none.
    committed: i64,
    carries: C,
}

impl<C> Read<C> {
    fn done(&self) -> bool {
        self.exhausted && self.committed >= self.handed - 1
    }
}

/// Reads not yet committed whole, oldest first. The newest is the one being handed out, and the methods on it hold
/// only while one is open.
pub struct Reads<C> {
    open: VecDeque<Read<C>>,
    next_partition: i32,
}

impl<C> Default for Reads<C> {
    fn default() -> Self {
        Reads {
            open: VecDeque::new(),
            next_partition: 0,
        }
    }
}

impl<C> Reads<C> {
    /// Opens a read whose records are handed out one at a time by [`hand_out`](Self::hand_out) until
    /// [`end`](Self::end), and returns its partition, which no other open read has.
    pub fn open(&mut self, topic: Arc<str>, carries: C) -> i32 {
        self.push(topic, 0, false, carries)
    }

    /// Opens a read of `handed` records handed out whole, at offsets from 0, and returns its partition.
    pub fn open_whole(&mut self, topic: Arc<str>, handed: i64, carries: C) -> i32 {
        self.push(topic, handed, true, carries)
    }

    /// Queues what a read of nothing carries: done at once, it is taken as soon as every read before it is done. It
    /// takes no partition, so the reads that hand out records are numbered without a gap.
    pub fn carry(&mut self, carries: C) {
        self.open.push_back(Read {
            partition: -1,
            topic: Arc::from(""),
            handed: 0,
            exhausted: true,
            committed: -1,
            carries,
        });
    }

    fn push(&mut self, topic: Arc<str>, handed: i64, exhausted: bool, carries: C) -> i32 {
        let partition = self.next_partition;
        self.next_partition = self.next_partition.wrapping_add(1);
        self.open.push_back(Read {
            partition,
            topic,
            handed,
            exhausted,
            committed: -1,
            carries,
        });
        partition
    }

    fn newest(&mut self) -> &mut Read<C> {
        self.open.back_mut().expect("a read is open")
    }

    /// Hands out the newest read's next record: its offset, partition and topic.
    pub fn hand_out(&mut self) -> (i64, i32, Arc<str>) {
        let read = self.newest();
        read.handed += 1;
        (read.handed - 1, read.partition, read.topic.clone())
    }

    /// Takes back the newest read's last record, so it is handed out again at the same offset.
    pub fn hand_back(&mut self) {
        self.newest().handed -= 1;
    }

    /// How many records of the newest read are handed out.
    pub fn handed_out(&mut self) -> i64 {
        self.newest().handed
    }

    /// Ends the newest read: every record of it is handed out, so it is done once its last one commits.
    pub fn end(&mut self) {
        self.newest().exhausted = true;
    }

    /// What the newest read carries, to change before it ends.
    pub fn carries_mut(&mut self) -> &mut C {
        &mut self.newest().carries
    }

    /// Records committed, by partition and topic. A partition no read has is of a read already retired.
    pub fn commit(&mut self, offsets: &CommitMap) {
        for (at, &offset) in offsets {
            if let Some(read) = self
                .open
                .iter_mut()
                .find(|r| r.partition == at.partition && r.topic == at.topic)
            {
                read.committed = read.committed.max(offset);
            }
        }
    }

    /// Retires the oldest reads that are done, one as each is taken, yielding what it carried: oldest first, and
    /// only as far as the caller takes.
    pub fn settle(&mut self) -> impl Iterator<Item = C> + '_ {
        std::iter::from_fn(|| {
            let read = self
                .open
                .front()
                .is_some_and(Read::done)
                .then(|| self.open.pop_front())??;
            Some(read.carries)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TopicPartition;

    fn committed(reads: &mut Reads<&'static str>, topic: &str, partition: i32, offset: i64) -> Vec<&'static str> {
        let mut offsets = CommitMap::new();
        offsets.insert(TopicPartition::new(Arc::from(topic), partition), offset);
        reads.commit(&offsets);
        reads.settle().collect()
    }

    #[test]
    fn a_read_retires_once_handed_out_whole_and_committed_to_its_last_record() {
        let mut reads = Reads::default();
        let partition = reads.open(Arc::from("a"), "A");
        assert_eq!(reads.hand_out(), (0, partition, Arc::from("a")));
        assert_eq!(reads.hand_out().0, 1);
        reads.hand_back();
        assert_eq!(reads.handed_out(), 1);
        assert_eq!(reads.hand_out().0, 1);
        assert!(
            committed(&mut reads, "a", partition, 0).is_empty(),
            "the last record is not committed"
        );
        reads.end();
        assert!(
            committed(&mut reads, "a", partition, 0).is_empty(),
            "the last record is still not committed"
        );
        assert_eq!(committed(&mut reads, "a", partition, 1), ["A"]);
        let partition = reads.open(Arc::from("b"), "B");
        reads.hand_out();
        assert!(
            committed(&mut reads, "b", partition, 0).is_empty(),
            "not ended, so more may be handed out"
        );
        reads.end();
        assert_eq!(reads.settle().collect::<Vec<_>>(), ["B"]);
    }

    #[test]
    fn reads_retire_in_the_order_they_were_made_whatever_order_they_commit_in() {
        let mut reads = Reads::default();
        let first = reads.open_whole(Arc::from("t"), 2, "first");
        let second = reads.open_whole(Arc::from("t"), 1, "second");
        reads.carry("none");
        let third = reads.open_whole(Arc::from("t"), 1, "third");
        assert_eq!(
            [first + 1, first + 2],
            [second, third],
            "a read of nothing takes no partition"
        );
        assert!(
            committed(&mut reads, "t", second, 0).is_empty(),
            "the first still holds it back"
        );
        assert_eq!(committed(&mut reads, "t", first, 1), ["first", "second", "none"]);
        assert!(
            committed(&mut reads, "t", first, 1).is_empty(),
            "a retired read's commit lands nowhere"
        );
        assert_eq!(committed(&mut reads, "t", third, 0), ["third"]);
    }

    #[test]
    fn a_commit_names_a_read_by_its_topic_too() {
        let mut reads = Reads::default();
        let partition = reads.open_whole(Arc::from("a"), 1, "A");
        assert!(committed(&mut reads, "b", partition, 0).is_empty());
        assert_eq!(committed(&mut reads, "a", partition, 0), ["A"]);
    }
}
