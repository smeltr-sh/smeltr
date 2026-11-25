//! When a source whose rows wait in memory hands its reader a batch.

use std::time::{Duration, Instant};

use crate::worker::LEAST_BATCH_BYTES;

/// How long the oldest row an inbox holds waits for a batch's worth ([`LEAST_BATCH_BYTES`]) to arrive behind it: the
/// longest a row waits on this while traffic is light.
pub(crate) const SMALL_BATCH_WAIT: Duration = Duration::from_millis(2);

/// When the reader of an inbox hands over what it holds: once the inbox holds a batch's worth, or once its oldest row
/// has waited [`SMALL_BATCH_WAIT`]. Rows arriving a few at a time would otherwise make batches of a few rows, whose
/// own work in every stage after outweighs them, and a reader that keeps up with arrivals would cut every batch that
/// small. The inbox counts its own bytes and says how many it holds.
#[derive(Default)]
pub(crate) struct BatchWait {
    /// When the oldest row arrived. Set by the first row into an empty inbox; read only while the inbox holds rows.
    since: Option<Instant>,
}

impl BatchWait {
    /// Rows arrived at an inbox that held `before` bytes and holds `after` now; `first` when it held no row. Whether a
    /// reader waiting on the inbox needs waking: these are its first rows, or they make a batch's worth.
    pub(crate) fn arrived(&mut self, first: bool, before: usize, after: usize) -> bool {
        if first {
            self.since = Some(Instant::now());
        }
        first || (before < LEAST_BATCH_BYTES && after >= LEAST_BATCH_BYTES)
    }

    /// How much longer the reader of an inbox holding `held` bytes waits before it hands them over: none once they
    /// make a batch's worth or the oldest has waited [`SMALL_BATCH_WAIT`].
    pub(crate) fn left(&self, held: usize) -> Duration {
        match self.since {
            Some(at) if held < LEAST_BATCH_BYTES => (at + SMALL_BATCH_WAIT).saturating_duration_since(Instant::now()),
            _ => Duration::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_rows_and_a_batchs_worth_wake_the_reader_and_nothing_between() {
        let mut wait = BatchWait::default();
        assert!(wait.arrived(true, 0, 100), "the first rows into an empty inbox");
        assert!(!wait.arrived(false, 100, 200), "more, short of a batch's worth");
        assert!(
            wait.arrived(false, 200, LEAST_BATCH_BYTES),
            "the rows that make a batch's worth"
        );
        assert!(
            !wait.arrived(false, LEAST_BATCH_BYTES, LEAST_BATCH_BYTES + 100),
            "more past it"
        );
    }

    #[test]
    fn a_reader_waits_for_a_batchs_worth_until_the_oldest_row_has_waited_its_time() {
        let mut wait = BatchWait::default();
        wait.arrived(true, 0, 100);
        let left = wait.left(100);
        assert!(left > Duration::ZERO && left <= SMALL_BATCH_WAIT, "{left:?}");
        assert_eq!(
            wait.left(LEAST_BATCH_BYTES),
            Duration::ZERO,
            "a batch's worth goes at once"
        );
        wait.since = Some(Instant::now() - SMALL_BATCH_WAIT);
        assert_eq!(wait.left(100), Duration::ZERO, "the oldest row has waited its time");
    }
}
