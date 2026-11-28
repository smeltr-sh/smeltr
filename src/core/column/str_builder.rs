//! Builds a variable-length column.

use super::{Column, SealedRows, Storage, Validity, VarType};
use crate::core::buffer::{Buffer, BufferPool};
use crate::core::variant::{self, Encoded};

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
    /// Whether a push wrote bytes not known to be checked, so sealing checks the text.
    unchecked: bool,
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
            unchecked: false,
        }
    }

    /// A builder for a column whose length is not known until it is done, such as the rows of one poll of a source:
    /// `push_grow` and `push_null_grow` grow its offsets as they grow its data, and `finish_growing` seals the rows
    /// pushed.
    pub(crate) fn growing(pool: &mut BufferPool, var_type: VarType) -> Self {
        StrBuilder::growing_with(pool, var_type, 0, 0)
    }

    /// `growing`, with room already for `rows` rows of `bytes` bytes in all, as a guess from the last column like it.
    pub(crate) fn growing_with(pool: &mut BufferPool, var_type: VarType, rows: usize, bytes: usize) -> Self {
        let mut b = StrBuilder {
            growing: true,
            ..StrBuilder::new(pool, var_type, rows)
        };
        b.reserve(pool, bytes);
        b
    }

    /// Whether `bytes` more fit as a row with no growing.
    #[inline]
    pub(crate) fn fits(&self, bytes: usize) -> bool {
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
        self.unchecked = true;
        self.put(s);
    }

    /// `push` of text known to be UTF-8, so a text column need not check it again.
    #[inline]
    pub(crate) fn push_text(&mut self, s: &str) {
        self.unchecked |= self.var_type != VarType::Str;
        self.put(s.as_bytes());
    }

    /// `push` of row `row` of `from`, so a builder of `from`'s type need not check it again.
    #[inline]
    pub(crate) fn push_row(&mut self, from: &SealedRows, row: usize) {
        self.unchecked |= from.var_type != self.var_type;
        self.put(from.rows.get(row));
    }

    /// `push_grow` of row `row` of `from`, as `push_row`.
    pub(crate) fn push_row_grow(&mut self, pool: &mut BufferPool, from: &SealedRows, row: usize) {
        self.unchecked |= from.var_type != self.var_type;
        self.put_parts_grow(pool, &[from.rows.get(row)]);
    }

    #[inline]
    fn put(&mut self, s: &[u8]) {
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
    pub(crate) fn push_grow(&mut self, pool: &mut BufferPool, s: &[u8]) {
        self.push_parts_grow(pool, &[s]);
    }

    /// `push_grow` of `parts` written one after another as one row, so a caller joins them without a copy of its own.
    fn push_parts_grow(&mut self, pool: &mut BufferPool, parts: &[&[u8]]) {
        self.unchecked = true;
        self.put_parts_grow(pool, parts);
    }

    /// Like `push_text`, for a `growing` builder: `push_grow` of text known to be UTF-8.
    pub(crate) fn push_text_grow(&mut self, pool: &mut BufferPool, s: &str) {
        self.unchecked |= self.var_type != VarType::Str;
        self.put_parts_grow(pool, &[s.as_bytes()]);
    }

    /// `push_grow` of a variant whose text is already checked, so sealing a variant column need not check it again.
    pub(crate) fn push_encoded(&mut self, pool: &mut BufferPool, v: Encoded) {
        // `v`'s bytes hold checked text only in a variant column; in any other they are unchecked.
        self.unchecked |= self.var_type != VarType::Variant;
        self.put_parts_grow(pool, &[v.bytes()]);
    }

    fn put_parts_grow(&mut self, pool: &mut BufferPool, parts: &[&[u8]]) {
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
    pub(crate) fn push_written_grow(&mut self, pool: &mut BufferPool, len: usize, write: impl FnOnce(&mut [u8])) {
        self.unchecked = true;
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
            let len = (self.len * 2).max(64);
            self.offsets.grow(pool, (len + 1) * 4);
            self.validity.grow(pool, len);
            self.len = len;
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
        debug_assert!(self.row < self.len, "pushed more rows than the column holds");
        self.validity.set_valid(self.row, false);
        self.row += 1;
        self.offsets.as_mut_slice::<u32>()[self.row] = self.pos;
    }

    /// `push_null` for a `growing` builder.
    pub(crate) fn push_null_grow(&mut self, pool: &mut BufferPool) {
        self.room_for(pool, 0);
        self.push_null();
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
        self.truncate(self.row - 1);
    }

    /// Take back the rows pushed after the first `rows`, so the next push is row `rows`.
    pub(crate) fn truncate(&mut self, rows: usize) {
        debug_assert!(rows <= self.row, "taking back rows not pushed");
        for row in rows..self.row {
            self.validity.set_valid(row, true);
        }
        self.row = rows;
        self.pos = self.offsets.as_slice::<u32>()[rows];
    }

    /// Seal a `growing` builder's column, which holds the rows pushed.
    pub(crate) fn finish_growing(mut self) -> Column {
        debug_assert!(self.growing, "a builder of known length finishes with `finish`");
        self.len = self.row;
        self.validity.truncate(self.row);
        // A reader's bytes are decoded before a string operation reads them, so a bytes column is not worth the scan.
        let ascii = self.var_type != VarType::Bytes && self.check_text();
        self.seal(ascii)
    }

    /// Seal the column. Every row must have been pushed or nulled.
    pub fn finish(self) -> Column {
        // A row that was never pushed has no offsets written, so its place in the buffer is unknown and its text is
        // unchecked.
        assert_eq!(
            self.row, self.len,
            "StrBuilder finished with {} of {} rows written — the kernel contract \
             requires every row to be pushed or nulled",
            self.row, self.len
        );
        let ascii = self.check_text();
        self.seal(ascii)
    }

    /// Whether the column's bytes are all ASCII. Finding that also checks, once, that the text is UTF-8: a text
    /// column's whole buffer, a variant column's strings and keys. That check is what lets `Column::str` and
    /// `Column::variant` hand out their rows unchecked. Text that is not UTF-8 is a bug in the step that wrote it.
    fn check_text(&self) -> bool {
        let bytes = &self.data.as_slice::<u8>()[..self.pos as usize];
        // Once over the whole buffer, not once per push: `is_ascii` runs a word at a time.
        let ascii = bytes.is_ascii();
        if !self.unchecked {
            return ascii;
        }
        match self.var_type {
            // A valid buffer can still split a character between two rows, so each row must start on a character
            // boundary.
            VarType::Str => assert!(
                ascii
                    || simdutf8::basic::from_utf8(bytes).is_ok_and(|text| {
                        let starts = &self.offsets.as_slice::<u32>()[1..self.row];
                        starts.iter().all(|&o| text.is_char_boundary(o as usize))
                    }),
                "a str column holds UTF-8"
            ),
            VarType::Variant if !ascii => {
                for span in self.offsets.as_slice::<u32>()[..=self.row].windows(2) {
                    let row = &bytes[span[0] as usize..span[1] as usize];
                    assert!(
                        row.is_empty() || variant::text_is_utf8(row),
                        "a variant's strings and keys are UTF-8"
                    );
                }
            }
            VarType::Bytes | VarType::Variant => {}
        }
        ascii
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
