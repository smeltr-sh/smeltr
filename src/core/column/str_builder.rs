//! Builds a variable-length column.

use super::{Column, Storage, Validity, VarType};
use crate::core::buffer::{Buffer, BufferPool};

// ---------------------------------------------------------------------------
// StrBuilder
// ---------------------------------------------------------------------------

/// Builds a variable-length column. Exists exactly as long as the column is half-written; `finish` consumes it.
///
/// ```text
///   new      -> exact offsets buffer, empty data buffer
///   reserve  -> one exact allocation, from the measured total
///   push     -> per row, no growth
///   finish   -> seal
/// ```
///
/// `reserve` is optional (`push_grow` grows on demand), but a kernel that can measure its output from the input offsets
/// should reserve, so `grow` never fires on the hot path.
pub struct StrBuilder {
    var_type: VarType,
    offsets: Buffer,
    data: Buffer,
    validity: Validity,
    /// Bytes written so far; also the next offset value.
    pos: u32,
    /// Rows pushed so far. Checked against `len` at `finish`.
    row: usize,
    len: usize,
    /// Capacity of `data` in bytes. Never exceeds `u32::MAX`, so `pos` cannot overflow.
    cap: usize,
    /// Whether `len` grows with the rows pushed (`growing`).
    growing: bool,
}

impl StrBuilder {
    pub fn new(pool: &mut BufferPool, var_type: VarType, len: usize) -> Self {
        let mut offsets = pool.take((len + 1) * 4);
        offsets.as_mut_slice::<u32>()[0] = 0;
        StrBuilder {
            var_type,
            offsets,
            data: pool.take(0),
            validity: Validity::alloc(pool, len),
            pos: 0,
            row: 0,
            len,
            cap: 0,
            growing: false,
        }
    }

    /// A builder for a column whose length is not known until it is done, as a source's poll: each `push_grow` grows
    /// the offsets as it does the data, and `finish` seals the rows pushed. Every row is a value; `push_null` is for a
    /// builder of known length.
    pub fn growing(pool: &mut BufferPool, var_type: VarType) -> Self {
        StrBuilder::growing_with(pool, var_type, 0, 0)
    }

    /// `growing`, with room already for `rows` rows of `bytes` bytes in all, as a guess from the last column like it.
    pub fn growing_with(pool: &mut BufferPool, var_type: VarType, rows: usize, bytes: usize) -> Self {
        let mut b = StrBuilder {
            growing: true,
            ..StrBuilder::new(pool, var_type, rows)
        };
        b.reserve(pool, bytes);
        b
    }

    /// Whether `bytes` more fit as a row with no growing.
    #[inline]
    pub fn fits(&self, bytes: usize) -> bool {
        self.row < self.len && self.pos as usize + bytes <= self.cap
    }

    /// Ensure room for `bytes` more.
    pub fn reserve(&mut self, pool: &mut BufferPool, bytes: usize) {
        let need = self.pos as usize + bytes;
        if need > self.cap {
            self.grow_to(pool, need);
        }
    }

    fn grow_to(&mut self, pool: &mut BufferPool, cap: usize) {
        assert!(
            cap <= u32::MAX as usize,
            "a Str/Bytes column holds at most u32::MAX bytes; asked for {cap}"
        );
        self.data.grow(pool, cap);
        self.cap = cap;
    }

    /// Panics if the reserved space runs out; a kernel that measured correctly never lands there.
    #[inline]
    pub fn push(&mut self, s: &[u8]) {
        debug_assert!(self.row < self.len, "pushed more rows than the column holds");
        if self.pos as usize + s.len() > self.cap {
            let requested = self.pos as usize + s.len();
            panic!(
                "StrBuilder overflow: needed {requested} bytes, reserved {}. \
                 Use push_grow() or reserve() the measured total.",
                self.cap
            );
        }
        self.data.as_mut_slice::<u8>()[self.pos as usize..self.pos as usize + s.len()].copy_from_slice(s);
        self.pos += s.len() as u32;
        self.row += 1;
        self.offsets.as_mut_slice::<u32>()[self.row] = self.pos;
    }

    /// `push` for kernels whose output size is unknowable in advance. Grows by doubling.
    pub fn push_grow(&mut self, pool: &mut BufferPool, s: &[u8]) {
        self.push_parts_grow(pool, &[s]);
    }

    /// `push_grow` of `parts` written one after another as one row, so a caller joins them without a copy of its own.
    pub fn push_parts_grow(&mut self, pool: &mut BufferPool, parts: &[&[u8]]) {
        self.room_for(pool, parts.iter().map(|p| p.len()).sum());
        for part in parts {
            self.data.as_mut_slice::<u8>()[self.pos as usize..self.pos as usize + part.len()].copy_from_slice(part);
            self.pos += part.len() as u32;
        }
        self.row += 1;
        self.offsets.as_mut_slice::<u32>()[self.row] = self.pos;
    }

    /// `push_grow` of a row of `len` bytes that `write` fills in place, so a row larger than its input is held to the
    /// pool's budget before it is written, not after a copy of it is.
    pub fn push_written_grow(&mut self, pool: &mut BufferPool, len: usize, write: impl FnOnce(&mut [u8])) {
        self.room_for(pool, len);
        let start = self.pos as usize;
        write(&mut self.data.as_mut_slice::<u8>()[start..start + len]);
        self.pos += len as u32;
        self.row += 1;
        self.offsets.as_mut_slice::<u32>()[self.row] = self.pos;
    }

    /// Grow, as `push_grow` does, so one more row of `len` bytes fits.
    fn room_for(&mut self, pool: &mut BufferPool, len: usize) {
        if self.growing && self.row == self.len {
            self.len = (self.len * 2).max(64);
            self.offsets.grow(pool, (self.len + 1) * 4);
        }
        debug_assert!(self.row < self.len, "pushed more rows than the column holds");
        let need = self.pos as usize + len;
        if need > self.cap {
            // Double what the buffer holds, not what is needed: a need just past a power of two would otherwise take a
            // buffer four times its size. The room the size class already gives is used first.
            let room = self.data.room().min(u32::MAX as usize);
            let want = if need <= room {
                room
            } else {
                (room * 2).clamp(64, u32::MAX as usize).max(need)
            };
            self.grow_to(pool, want);
        }
    }

    /// A null row: zero-length span, validity bit clear.
    #[inline]
    pub fn push_null(&mut self) {
        debug_assert!(!self.growing, "a growing builder's rows are all values");
        debug_assert!(self.row < self.len);
        self.validity.set_valid(self.row, false);
        self.row += 1;
        self.offsets.as_mut_slice::<u32>()[self.row] = self.pos;
    }

    /// The bytes of the row last pushed.
    pub fn last(&self) -> &[u8] {
        debug_assert!(self.row > 0, "nothing pushed");
        let offsets = self.offsets.as_slice::<u32>();
        &self.data.as_slice::<u8>()[offsets[self.row - 1] as usize..offsets[self.row] as usize]
    }

    /// Take back the row last pushed, so the next push replaces it. Its bytes are overwritten by that push.
    pub fn pop_last(&mut self) {
        debug_assert!(self.row > 0, "nothing pushed to take back");
        self.row -= 1;
        self.pos = self.offsets.as_slice::<u32>()[self.row];
        if !self.growing {
            self.validity.set_valid(self.row, true);
        }
    }

    /// Seal a `growing` builder's column: the rows pushed, every one a value.
    pub fn finish_growing(mut self, pool: &mut BufferPool) -> Column {
        debug_assert!(self.growing, "a builder of known length finishes with `finish`");
        self.len = self.row;
        let validity = std::mem::replace(&mut self.validity, Validity::alloc(pool, self.row));
        validity.reclaim(pool);
        // A reader's bytes are decoded before any string operation reads them, so nothing asks whether they are ASCII
        // and the scan would be a pass over every byte read for nothing. `false` claims nothing.
        self.seal(false)
    }

    /// Seal the column. Every row must have been pushed or nulled.
    pub fn finish(self) -> Column {
        debug_assert_eq!(
            self.row, self.len,
            "StrBuilder finished with {} of {} rows written — the kernel contract \
             requires every row to be pushed or nulled",
            self.row, self.len
        );
        // Once over the whole buffer, not once per push: `is_ascii` runs a word at a time.
        let ascii = self.data.as_slice::<u8>()[..self.pos as usize].is_ascii();
        self.seal(ascii)
    }

    fn seal(self, ascii: bool) -> Column {
        Column {
            len: self.len,
            storage: Storage::VarLen {
                var_type: self.var_type,
                offsets: self.offsets,
                data: self.data,
                validity: self.validity,
                ascii,
            },
        }
    }
}
