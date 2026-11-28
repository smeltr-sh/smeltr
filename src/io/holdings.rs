//! What a source holds in memory, taken from one account so that together it stays within what its worker gives it.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use super::receiver::INBOX_BYTES;
use crate::egress::QueuePool;
use crate::sync::Mutex;

/// Memory its takers share: up to its `ceiling`, borrowed from a pool where there is one. Its `reserve`, held above
/// that, is its turn's alone: one taker at a time that found no shared room takes from it, so a taker whose shared room
/// is spent still finishes and moves. Nothing takes past the two.
pub(super) struct Holdings {
    account: Mutex<Account>,
    reserve: usize,
    ceiling: usize,
    /// Its name in `pool`.
    lender: u64,
    pool: Option<Arc<QueuePool>>,
    /// Woken when it gives bytes back or its turn is let go.
    pub(super) freed: Notify,
    /// Times a taker found no room, once for each wait, since the reader last asked.
    pub(super) full: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct Account {
    counted: Counted,
    /// Whether a taker holds the turn.
    turn: bool,
}

/// Bytes taken of a source's holdings: those it shares, and those of its reserve.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Counted {
    shared: usize,
    pub(super) reserved: usize,
}

impl Counted {
    pub(super) fn total(self) -> usize {
        self.shared + self.reserved
    }

    pub(super) fn add(&mut self, other: Counted) {
        self.shared += other.shared;
        self.reserved += other.reserved;
    }

    /// `bytes` of it, its reserve first so the turn has room again soonest.
    pub(super) fn split_off(&mut self, bytes: usize) -> Counted {
        assert!(bytes <= self.total(), "gives back no more than it took");
        let reserved = bytes.min(self.reserved);
        let part = Counted {
            shared: bytes - reserved,
            reserved,
        };
        self.shared -= part.shared;
        self.reserved -= part.reserved;
        part
    }
}

impl Default for Holdings {
    /// A source alone: no pool and no reserve, held to its ceiling.
    fn default() -> Holdings {
        Holdings::reserving(0, None)
    }
}

impl Holdings {
    /// Holdings with `reserve` bytes for its turn and shared bytes from `pool`, as far as the pool lends. Without a
    /// pool, `INBOX_BYTES` shared.
    pub(super) fn reserving(reserve: usize, pool: Option<Arc<QueuePool>>) -> Holdings {
        Holdings {
            account: Mutex::new(Account::default()),
            reserve,
            // A source buffers into the memory its worker gives it: the pool is sized to that.
            ceiling: pool.as_ref().map_or(INBOX_BYTES, |p| p.limit()),
            lender: pool.as_ref().map_or(0, |p| p.lender()),
            pool,
            freed: Notify::new(),
            full: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Holdings with `reserve` bytes for its turn and `ceiling` shared, of no pool.
    pub(super) fn within(ceiling: usize, reserve: usize) -> Holdings {
        Holdings {
            ceiling,
            ..Holdings::reserving(reserve, None)
        }
    }

    /// Takes `bytes` more for a taker that holds `counted` and, when `turn`, the turn: shared where they fit, else of
    /// the reserve, taking the turn when it is free. Whether it took them.
    pub(super) fn take(&self, bytes: usize, counted: &mut Counted, turn: &mut bool) -> bool {
        let mut account = self.account.lock();
        if self.share(&mut account, bytes, counted) {
            return true;
        }
        if !*turn && !account.turn {
            account.turn = true;
            *turn = true;
        }
        if *turn && account.counted.reserved + bytes <= self.reserve {
            account.counted.reserved += bytes;
            counted.reserved += bytes;
            return true;
        }
        false
    }

    /// Takes `bytes` more for a taker that holds `counted`, where they fit in what is shared. It never takes the turn.
    /// Whether it took them.
    pub(super) fn take_shared(&self, bytes: usize, counted: &mut Counted) -> bool {
        self.share(&mut self.account.lock(), bytes, counted)
    }

    fn share(&self, account: &mut Account, bytes: usize, counted: &mut Counted) -> bool {
        let fits = account.counted.shared + bytes <= self.ceiling
            && self
                .pool
                .as_ref()
                .is_none_or(|p| p.take(self.lender, account.counted.shared, bytes, false));
        if fits {
            account.counted.shared += bytes;
            counted.shared += bytes;
        }
        fits
    }

    pub(super) fn give(&self, counted: Counted) {
        if counted.total() == 0 {
            return;
        }
        let mut account = self.account.lock();
        account.counted.shared -= counted.shared;
        account.counted.reserved -= counted.reserved;
        if let Some(pool) = &self.pool {
            pool.give(self.lender, account.counted.shared, counted.shared);
        }
        drop(account);
        self.freed.notify_waiters();
    }

    /// Lets the turn go, so the next taker short of room may take it.
    pub(super) fn release(&self) {
        self.account.lock().turn = false;
        self.freed.notify_waiters();
    }

    /// Whether it could ever hold `bytes` for one taker: of its reserve, or shared when the rest is let go.
    pub(super) fn could_hold(&self, bytes: usize) -> bool {
        bytes <= self.reserve || (bytes <= self.ceiling && self.pool.as_ref().is_none_or(|p| bytes <= p.limit()))
    }

    /// What its takers hold now, shared and of its reserve.
    pub(super) fn held(&self) -> usize {
        self.account.lock().counted.total()
    }

    /// Whether a taker without the turn, or one that would take it, takes another byte.
    pub(super) fn has_room(&self) -> bool {
        let account = self.account.lock();
        (account.counted.shared < self.ceiling
            && self
                .pool
                .as_ref()
                .is_none_or(|p| p.fits(self.lender, account.counted.shared, 1)))
            || (!account.turn && account.counted.reserved < self.reserve)
    }

    /// Counts a taker that found no room.
    pub(super) fn note_full(&self) {
        self.full.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Waits for `room` or `freed`, enabled before the caller looked. Another source giving back to the pool wakes
    /// neither, so a source borrowing looks again after a while.
    pub(super) async fn wait(
        &self,
        room: std::pin::Pin<&mut tokio::sync::futures::Notified<'_>>,
        freed: std::pin::Pin<&mut tokio::sync::futures::Notified<'_>>,
    ) {
        tokio::select! {
            () = room => {}
            () = freed => {}
            () = tokio::time::sleep(POOL_LOOK), if self.pool.is_some() => {}
        }
    }
}

/// How often a source waiting on the pool looks again.
const POOL_LOOK: Duration = Duration::from_millis(100);
