//! Null tracking: one bit per row.

use crate::core::{
    bits,
    buffer::{Buffer, BufferPool},
};

// ---------------------------------------------------------------------------
// Validity
// ---------------------------------------------------------------------------

/// Null tracking. One bit per row, 1 = present.
///
/// `valid_count` is maintained by `set_valid`. A kernel that writes `bits_mut()` directly must call `recount()` before
/// finishing.
pub struct Validity {
    bitmap: Buffer,
    len: usize,
    valid_count: usize,
}

impl Validity {
    pub fn alloc_null(pool: &mut BufferPool, len: usize) -> Self {
        let mut bitmap = pool.take(bits::bytes_for(len));
        bits::fill_invalid(bitmap.as_mut_slice::<u64>());
        Validity {
            bitmap,
            len,
            valid_count: 0,
        }
    }

    pub fn alloc(pool: &mut BufferPool, len: usize) -> Self {
        let mut bitmap = pool.take(bits::bytes_for(len));
        bits::fill_valid(bitmap.as_mut_slice::<u64>(), len);
        Validity {
            bitmap,
            len,
            valid_count: len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn valid_count(&self) -> usize {
        self.valid_count
    }

    /// The fast-path gate: when false, a kernel can skip validity entirely.
    #[inline]
    pub fn has_nulls(&self) -> bool {
        debug_assert!(self.valid_count <= self.len);
        self.valid_count < self.len
    }

    pub fn recount(&mut self) -> usize {
        self.valid_count = bits::popcount(self.bits());
        self.valid_count
    }

    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        debug_assert!(row < self.len);
        if self.valid_count == self.len {
            return true;
        }
        bits::get_row(self.bits(), row)
    }

    pub fn set_valid(&mut self, row: usize, valid: bool) {
        debug_assert!(row < self.len);
        let words = bits::words_for(self.len);
        let b = &mut self.bitmap.as_mut_slice::<u64>()[..words];
        if bits::set_row(b, row, valid) {
            if valid {
                self.valid_count += 1;
            } else {
                self.valid_count -= 1;
            }
        }
    }

    pub fn set_all_valid(&mut self) {
        let len = self.len;
        let words = bits::words_for(len);
        bits::fill_valid(&mut self.bitmap.as_mut_slice::<u64>()[..words], len);
        self.valid_count = len;
    }

    pub fn set_all_null(&mut self) {
        let words = bits::words_for(self.len);
        bits::fill_invalid(&mut self.bitmap.as_mut_slice::<u64>()[..words]);
        self.valid_count = 0;
    }

    /// Raw bitmap. Bits past `len` are always 0.
    #[inline]
    pub fn bits(&self) -> &[u64] {
        &self.bitmap.as_slice::<u64>()[..bits::words_for(self.len)]
    }

    /// Raw mutable bitmap. Writing here bypasses `valid_count`; call `recount()` afterward.
    #[inline]
    pub fn bits_mut(&mut self) -> &mut [u64] {
        let words = bits::words_for(self.len);
        &mut self.bitmap.as_mut_slice::<u64>()[..words]
    }

    pub fn reclaim(self, pool: &mut BufferPool) {
        pool.give(self.bitmap);
    }

    pub fn deep_copy(&self, pool: &mut BufferPool) -> Validity {
        let mut b = pool.take(bits::bytes_for(self.len));
        let words = bits::words_for(self.len);
        b.as_mut_slice::<u64>()[..words].copy_from_slice(self.bits());
        Validity {
            bitmap: b,
            len: self.len,
            valid_count: self.valid_count,
        }
    }
}

impl std::fmt::Debug for Validity {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Validity({}/{} valid)", self.valid_count, self.len)
    }
}
