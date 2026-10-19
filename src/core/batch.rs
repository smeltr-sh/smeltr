//! A batch: the slot arena for one homogeneous run of rows.
//!
//! `SlotId` is a column index fixed by the compiler before any data arrives. Every column in a batch has exactly `rows`
//! rows; a cardinality change produces a new batch through `gather`.

use super::id::SlotId;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType};

/// The prefix every plan's layout begins with, so `gather` and Drop need no special case for it: one column, the
/// liveness mask. The record follows it -- whatever fields the source declares (`plan::topology::SourceMeta`) -- and
/// the source's `__source` fields follow the record.
///
/// The first `RESERVED_SLOTS` of them are reserved: the runtime reads each back after the run at its layout's type,
/// so no step removes one or changes its type. A record's fields are not: a step may drop or replace any of them.
pub const SLOT_LIVE: SlotId = 0; // Bool — row liveness, written by routing (`plan::topology::route_masks`) and by `Drop`
pub const PREFIX_SLOTS: usize = 1;
pub const RESERVED_SLOTS: usize = 1;

/// A byte source's record is one field, `_raw`, so it takes the first slot after the prefix.
///
/// `_raw` is `Str`, not `Bytes`: each record is decoded before routing, from the encoding its source declares
/// (`format::encoding`), so everything downstream reads valid UTF-8 without revalidating or repairing.
pub const SLOT_RAW: SlotId = PREFIX_SLOTS; // Str  — the unparsed message, decoded
pub const RAW_SLOTS: usize = PREFIX_SLOTS + 1;

/// The prefix every plan's slot layout must begin with.
pub fn prefix_layout() -> Vec<DataType> {
    vec![DataType::Bool]
}

/// The prefix and a byte source's record: what every layout of a byte source begins with.
pub fn raw_layout() -> Vec<DataType> {
    vec![DataType::Bool, DataType::Str]
}

pub struct Batch {
    /// Fixed length = plan.slot_count. Unbound slots hold `Empty`, not `None`.
    slots: Vec<Column>,
    rows: usize,
    partition: i32,

    /// Debug-only: which slots have been written. Reading an unwritten slot is a compiler bug (it resolved a read to a
    /// slot no instruction writes), and the design says assert, not fall back.
    #[cfg(debug_assertions)]
    bound: Vec<bool>,
}

impl Batch {
    /// Every slot starts `Empty`: zero buffers, `rows` rows.
    pub fn new(layout: &[DataType], rows: usize, partition: i32) -> Batch {
        debug_assert!(layout.len() >= PREFIX_SLOTS, "layout must begin with the prefix");
        Batch {
            slots: layout.iter().map(|t| Column::empty(t.clone(), rows)).collect(),
            rows,
            partition,
            #[cfg(debug_assertions)]
            bound: vec![false; layout.len()],
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    pub fn get(&self, slot: SlotId) -> &Column {
        #[cfg(debug_assertions)]
        debug_assert!(
            self.bound[slot],
            "slot {slot} read before it was ever written — the compiler resolved a \
             read to a slot nothing produces"
        );
        &self.slots[slot]
    }

    /// Read without the bound check, for the serializer and for kernels that tolerate an unbound slot as null.
    #[inline]
    pub fn peek(&self, slot: SlotId) -> &Column {
        &self.slots[slot]
    }

    /// Write a bound column in place: a region's mask, whose bits clear as rows leave it.
    #[inline]
    pub fn get_mut(&mut self, slot: SlotId) -> &mut Column {
        &mut self.slots[slot]
    }

    /// Bind `col` to `slot`, reclaiming whatever was there. `col` must have exactly `rows` rows.
    #[inline]
    pub fn put(&mut self, slot: SlotId, col: Column, pool: &mut BufferPool) {
        assert_eq!(
            col.len(),
            self.rows,
            "every column in a batch must have exactly `rows` rows"
        );
        let old = std::mem::replace(&mut self.slots[slot], col);
        old.reclaim(pool);
        #[cfg(debug_assertions)]
        {
            self.bound[slot] = true;
        }
    }

    /// Move a column out, leaving `Empty` behind. Only legal once liveness has proven the slot dead.
    #[inline]
    pub fn take_dead(&mut self, slot: SlotId) -> Column {
        let ty = self.slots[slot].data_type();
        #[cfg(debug_assertions)]
        {
            self.bound[slot] = false;
        }
        std::mem::replace(&mut self.slots[slot], Column::empty(ty, self.rows))
    }

    // -- cardinality ---------------------------------------------------------

    /// A new batch containing only `indices`: row `i` of the result is row `indices[i]` of `self`, across every slot. Consuming, because `rows` is fixed for a batch's lifetime.
    pub fn gather(self, pool: &mut BufferPool, indices: &[u32]) -> Batch {
        let new_slots: Vec<Column> = self.slots.iter().map(|c| c.gather(pool, indices)).collect();
        let partition = self.partition;
        #[cfg(debug_assertions)]
        let bound = self.bound.clone();
        self.reclaim(pool);
        Batch {
            slots: new_slots,
            rows: indices.len(),
            partition,
            #[cfg(debug_assertions)]
            bound,
        }
    }

    /// Return every buffer to the pool.
    pub fn reclaim(mut self, pool: &mut BufferPool) {
        for col in self.slots.drain(..) {
            col.reclaim(pool);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::column::Column;

    const EXTRA_I64: SlotId = RAW_SLOTS;
    const EXTRA_STR: SlotId = RAW_SLOTS + 1;
    const EXTRA_I64_B: SlotId = RAW_SLOTS + 2;

    fn layout() -> Vec<DataType> {
        let mut l = raw_layout();
        l.push(DataType::I64); // EXTRA_I64
        l.push(DataType::Str); // EXTRA_STR
        l.push(DataType::I64); // EXTRA_I64_B
        l
    }

    /// `PREFIX_SLOTS` and `prefix_layout()` state the same fact twice, as `RAW_SLOTS` and `raw_layout()` do.
    #[test]
    fn each_layout_is_exactly_its_slot_count_long() {
        assert_eq!(prefix_layout().len(), PREFIX_SLOTS);
        assert_eq!(raw_layout().len(), RAW_SLOTS);
        assert_eq!(raw_layout()[..PREFIX_SLOTS], prefix_layout()[..]);
    }

    #[test]
    fn each_raw_index_names_its_own_declared_type() {
        let l = raw_layout();
        assert_eq!(l[SLOT_LIVE], DataType::Bool);
        assert_eq!(l[SLOT_RAW], DataType::Str);
    }

    #[test]
    fn new_allocates_no_buffers() {
        let b = Batch::new(&layout(), 100, 0);
        assert_eq!(b.rows(), 100);
        assert_eq!(b.slot_count(), RAW_SLOTS + 3);
        for s in 0..b.slot_count() {
            assert!(b.peek(s).is_unbound());
            assert_eq!(b.peek(s).len(), 100);
        }
        b.reclaim(&mut BufferPool::new());
    }

    #[test]
    fn empty_slots_keep_their_declared_type() {
        let b = Batch::new(&layout(), 10, 0);
        assert_eq!(b.peek(SLOT_LIVE).data_type(), DataType::Bool);
        assert_eq!(b.peek(SLOT_RAW).data_type(), DataType::Str);
        assert_eq!(b.peek(EXTRA_STR).data_type(), DataType::Str);
        b.reclaim(&mut BufferPool::new());
    }

    #[test]
    fn put_then_get() {
        let mut p = BufferPool::new();
        let mut b = Batch::new(&layout(), 8, 0);
        let col = Column::alloc(&mut p, &DataType::I64, 8);
        b.put(EXTRA_I64, col, &mut p);
        assert!(!b.get(EXTRA_I64).is_unbound());
        assert_eq!(b.get(EXTRA_I64).len(), 8);
        b.reclaim(&mut p);
    }

    #[test]
    fn put_reclaims_the_previous_occupant() {
        let mut p = BufferPool::new();
        let mut b = Batch::new(&layout(), 8, 0);
        b.put(EXTRA_I64, Column::alloc(&mut p, &DataType::I64, 8), &mut p);
        let ptr = b.get(EXTRA_I64).i64().unwrap().values.as_ptr();
        b.put(EXTRA_I64, Column::alloc(&mut p, &DataType::I64, 8), &mut p);
        // Taken before the batch is reclaimed, which would give the second column's buffer back on top of it.
        assert_eq!(p.take(64).as_slice::<i64>().as_ptr(), ptr);
        b.reclaim(&mut p);
    }

    #[test]
    fn take_dead_leaves_empty_of_the_same_type() {
        let mut p = BufferPool::new();
        let mut b = Batch::new(&layout(), 8, 0);
        b.put(EXTRA_I64, Column::alloc(&mut p, &DataType::I64, 8), &mut p);
        let taken = b.take_dead(EXTRA_I64);
        assert!(!taken.is_unbound());
        assert!(b.peek(EXTRA_I64).is_unbound());
        assert_eq!(b.peek(EXTRA_I64).data_type(), DataType::I64);
        assert_eq!(b.peek(EXTRA_I64).len(), 8);
        taken.reclaim(&mut p);
        b.reclaim(&mut p);
    }

    #[test]
    #[should_panic(expected = "read before it was ever written")]
    fn get_on_unbound_slot_panics_in_debug() {
        let b = Batch::new(&layout(), 4, 0);
        let _ = b.get(EXTRA_I64);
    }

    #[test]
    #[should_panic(expected = "exactly `rows` rows")]
    fn put_with_wrong_row_count_panics() {
        let mut p = BufferPool::new();
        let mut b = Batch::new(&layout(), 8, 0);
        b.put(EXTRA_I64, Column::alloc(&mut p, &DataType::I64, 7), &mut p);
    }

    #[test]
    fn reclaim_returns_buffers_to_the_pool() {
        let mut p = BufferPool::new();
        let mut b = Batch::new(&layout(), 64, 0);
        b.put(EXTRA_I64, Column::alloc(&mut p, &DataType::I64, 64), &mut p);
        let ptr = b.get(EXTRA_I64).i64().unwrap().values.as_ptr();
        b.reclaim(&mut p);
        let reused = p.take(512);
        assert_eq!(reused.as_slice::<i64>().as_ptr(), ptr);
    }

    // --- gather ---------------------------------------------------------

    #[test]
    fn gather_selects_rows_across_every_slot() {
        let mut p = BufferPool::new();
        let mut b = Batch::new(&layout(), 4, 0);
        let mut offset = Column::alloc(&mut p, &DataType::I64, 4);
        offset.i64_mut().unwrap().values.copy_from_slice(&[100, 200, 300, 400]);
        b.put(EXTRA_I64_B, offset, &mut p);
        let mut extra = Column::alloc(&mut p, &DataType::I64, 4);
        extra.i64_mut().unwrap().values.copy_from_slice(&[1, 2, 3, 4]);
        b.put(EXTRA_I64, extra, &mut p);

        let g = b.gather(&mut p, &[2, 0]);
        assert_eq!(g.rows(), 2);
        assert_eq!(g.get(EXTRA_I64_B).i64().unwrap().values, &[300, 100]);
        assert_eq!(g.get(EXTRA_I64).i64().unwrap().values, &[3, 1]);
        g.reclaim(&mut p);
    }

    #[test]
    fn gather_leaves_an_unbound_slot_unbound() {
        let mut p = BufferPool::new();
        let b = Batch::new(&layout(), 4, 0);
        let g = b.gather(&mut p, &[1, 0]);
        assert!(g.peek(EXTRA_I64).is_unbound());
        assert_eq!(g.peek(EXTRA_I64).len(), 2);
        g.reclaim(&mut p);
    }

    #[test]
    #[should_panic(expected = "read before it was ever written")]
    fn gather_does_not_launder_a_never_written_slot_into_a_bound_one() {
        let mut p = BufferPool::new();
        let b = Batch::new(&layout(), 4, 0);
        let g = b.gather(&mut p, &[0, 1]);
        let _ = g.get(EXTRA_I64);
    }

    #[test]
    fn gather_to_zero_rows_is_a_valid_empty_batch() {
        let mut p = BufferPool::new();
        let mut b = Batch::new(&layout(), 3, 0);
        b.put(EXTRA_I64, Column::alloc(&mut p, &DataType::I64, 3), &mut p);
        let g = b.gather(&mut p, &[]);
        assert_eq!(g.rows(), 0);
        g.reclaim(&mut p);
    }
}
