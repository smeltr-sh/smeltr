//! Columns: typed, validity-tracked, pool-backed arrays of one field's values.
//!
//! Arrow-shaped layout (typed buffer + validity bitmap), hand-rolled.
//!
//! Three rules govern everything here:
//!
//! 1. **Reclaim is explicit.** `Column::reclaim(self, pool)` returns buffers to the pool. `Drop` cannot reach the pool,
//!    so it only asserts that reclaim already happened.
//!
//! 2. **`Empty` is the fifth state.** Zero buffers, all null. Every slot of a new `Batch` holds one until it is
//!    written, a kernel returns one for a value null on every row, and it is the sentinel `Batch::take_dead` leaves
//!    behind.
//!
//! 3. **Buffers from the pool are dirty.** Nothing is zeroed on `alloc`. A kernel must write every row or clear its
//!    validity bit. The one exception is a bitmap's tail past `len`, which `alloc` zeroes so the `bits` invariant holds
//!    for value bitmaps too.

mod str_builder;
mod types;
mod validity;

#[cfg(test)]
mod tests;

pub use str_builder::StrBuilder;
pub use types::{DataType, FixedType, TypeError, VarType};
pub use validity::Validity;

use crate::core::{
    bits,
    buffer::{Buffer, BufferPool},
};

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

enum Storage {
    /// Zero buffers, every row null.
    Empty(DataType),

    /// One value, every row. A literal assigned to a field is this, and a reader that can use the value -- the
    /// serializer -- never asks for more. A reader that needs rows calls `expand` and pays the pool, once.
    Constant(Scalar),

    /// One 8-byte-wide buffer.
    Fixed {
        fixed_type: FixedType,
        buffer: Buffer,
        validity: Validity,
    },

    /// Bool. Values are a bitmap, same shape as validity.
    Bits { values: Buffer, validity: Validity },

    /// Str/Bytes: u32 offsets (len + 1) into a flat data buffer.
    VarLen {
        var_type: VarType,
        offsets: Buffer,
        data: Buffer,
        validity: Validity,
        /// True when every byte is < 0x80, so `.length`/`.slice`/`.indexOf` are O(1) byte ops instead of a character scan.
        ascii: bool,
    },

    /// Array: u32 offsets (len + 1) into a flattened child column.
    List {
        offsets: Buffer,
        child: Box<Column>,
        validity: Validity,
    },

    /// Named child columns, each the parent's length, and the unclaimed fields: where a declared object met a value
    /// with keys it does not name, a variant column of those keys, one object per row. No validity of its own: a row
    /// is null when every field and the unclaimed fields are.
    Struct(Vec<(String, Column)>, Option<Box<Column>>),
}

// ---------------------------------------------------------------------------
// Column
// ---------------------------------------------------------------------------

/// One value of a scalar type, as a constant column holds it. Null is not one: an all-null column is `Empty`.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Bool(bool),
    I64(i64),
    F64(f64),
    DateTime(i64),
    Duration(i64),
    Str(String),
    Bytes(Vec<u8>),
    /// One variant value, in its encoded form.
    Variant(Vec<u8>),
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Scalar::Bool(_) => DataType::Bool,
            Scalar::I64(_) => DataType::I64,
            Scalar::F64(_) => DataType::F64,
            Scalar::DateTime(_) => DataType::DateTime,
            Scalar::Duration(_) => DataType::Duration,
            Scalar::Str(_) => DataType::Str,
            Scalar::Bytes(_) => DataType::Bytes,
            Scalar::Variant(_) => DataType::Variant,
        }
    }
}

pub struct Column {
    len: usize,
    storage: Storage,
}

impl std::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Column({:?} x{}{})",
            self.data_type(),
            self.len,
            if self.is_unbound() { ", unbound" } else { "" }
        )
    }
}

/// Values and validity of a fixed-width column, bundled so a kernel cannot read one column's values against another's
/// validity.
#[derive(Debug)]
pub struct Fixed<'a, T> {
    pub values: &'a [T],
    pub validity: &'a Validity,
}

#[derive(Debug)]
pub struct FixedMut<'a, T> {
    pub values: &'a mut [T],
    pub validity: &'a mut Validity,
}

#[derive(Debug)]
pub struct Bits<'a> {
    pub values: &'a [u64],
    pub validity: &'a Validity,
}

#[derive(Debug)]
pub struct BitsMut<'a> {
    pub values: &'a mut [u64],
    pub validity: &'a mut Validity,
}

#[derive(Debug)]
pub struct VarLen<'a> {
    pub offsets: &'a [u32],
    pub data: &'a [u8],
    pub validity: &'a Validity,
    pub ascii: bool,
}

impl<'a> VarLen<'a> {
    #[inline]
    pub fn get(&self, row: usize) -> &'a [u8] {
        let s = self.offsets[row] as usize;
        let e = self.offsets[row + 1] as usize;
        &self.data[s..e]
    }

    #[inline]
    pub fn row_len(&self, row: usize) -> usize {
        (self.offsets[row + 1] - self.offsets[row]) as usize
    }

    /// Total bytes across all rows, from the offsets alone.
    #[inline]
    pub fn total_bytes(&self) -> usize {
        let n = self.offsets.len();
        if n == 0 {
            0
        } else {
            (self.offsets[n - 1] - self.offsets[0]) as usize
        }
    }
}

#[derive(Debug)]
pub struct List<'a> {
    pub offsets: &'a [u32],
    pub child: &'a Column,
    pub validity: &'a Validity,
}

impl Column {
    // -- construction --------------------------------------------------------

    /// A zero-buffer, all-null column.
    pub fn empty(data_type: DataType, len: usize) -> Column {
        Column {
            len,
            storage: Storage::Empty(data_type),
        }
    }

    /// `len` rows of one `value`, held as the value. No buffers until someone calls `expand`.
    pub fn constant(value: Scalar, len: usize) -> Column {
        Column {
            len,
            storage: Storage::Constant(value),
        }
    }

    /// The one value every row holds, when this is a constant column. A reader that can use it, such as the
    /// serializer or a kernel with a scalar-operand path, need not ask for rows at all.
    pub fn constant_value(&self) -> Option<&Scalar> {
        match &self.storage {
            Storage::Constant(value) => Some(value),
            _ => None,
        }
    }

    /// `len` rows of `value` as buffers: what a constant column becomes when a kernel asks for rows.
    pub fn filled(pool: &mut BufferPool, value: &Scalar, len: usize) -> Column {
        match value {
            Scalar::I64(x) => {
                let mut c = Column::alloc(pool, &DataType::I64, len);
                c.i64_mut().unwrap().values.fill(*x);
                c
            }
            Scalar::F64(x) => {
                let mut c = Column::alloc(pool, &DataType::F64, len);
                c.f64_mut().unwrap().values.fill(*x);
                c
            }
            Scalar::DateTime(x) => {
                let mut c = Column::alloc(pool, &DataType::DateTime, len);
                c.datetime_mut().unwrap().values.fill(*x);
                c
            }
            Scalar::Duration(x) => {
                let mut c = Column::alloc(pool, &DataType::Duration, len);
                c.duration_mut().unwrap().values.fill(*x);
                c
            }
            Scalar::Bool(x) => {
                let mut c = Column::alloc(pool, &DataType::Bool, len);
                {
                    let b = c.bool_mut().unwrap();
                    if *x {
                        bits::fill_valid(b.values, len);
                    } else {
                        bits::fill_invalid(b.values);
                    }
                }
                c
            }
            Scalar::Str(text) => Column::repeated(pool, VarType::Str, text.as_bytes(), len),
            Scalar::Bytes(bytes) => Column::repeated(pool, VarType::Bytes, bytes, len),
            Scalar::Variant(value) => Column::repeated(pool, VarType::Variant, value, len),
        }
    }

    fn repeated(pool: &mut BufferPool, var_type: VarType, bytes: &[u8], len: usize) -> Column {
        let mut b = StrBuilder::new(pool, var_type, len);
        b.reserve(pool, bytes.len() * len);
        for _ in 0..len {
            b.push(bytes);
        }
        b.finish()
    }

    /// Turn a constant column into its rows, from `pool`. A no-op for any other column.
    ///
    /// Every accessor refuses an unexpanded constant, the way it refuses an `Empty`: a reader either uses
    /// `constant_value` or calls this first. That is what keeps the rows pooled. Built from a pool of its own they
    /// would be a fresh allocation, zeroed, where `take` hands back a recycled buffer for nothing.
    pub fn expand(&mut self, pool: &mut BufferPool) {
        let Storage::Constant(value) = &self.storage else {
            return;
        };
        let mut rows = Column::filled(pool, value, self.len);
        self.storage = std::mem::replace(&mut rows.storage, Storage::Empty(DataType::Bool));
    }

    /// Allocate real buffers from the pool. Values are dirty; validity starts all-valid. A Str/Bytes data buffer starts
    /// at zero capacity — `StrBuilder` measures and reserves the exact total.
    pub fn alloc(pool: &mut BufferPool, data_type: &DataType, len: usize) -> Column {
        let storage = match data_type {
            // No row of a `null` column has a value to write.
            DataType::Null => return Column::empty(DataType::Null, len),
            DataType::Bool => {
                let mut values = pool.take(bits::bytes_for(len));
                bits::mask_tail(values.as_mut_slice::<u64>(), len);
                Storage::Bits {
                    values,
                    validity: Validity::alloc(pool, len),
                }
            }
            DataType::I64 => Self::alloc_fixed(pool, FixedType::I64, len),
            DataType::F64 => Self::alloc_fixed(pool, FixedType::F64, len),
            DataType::DateTime => Self::alloc_fixed(pool, FixedType::DateTime, len),
            DataType::Duration => Self::alloc_fixed(pool, FixedType::Duration, len),
            DataType::Str => Self::alloc_varlen(pool, VarType::Str, len),
            DataType::Bytes => Self::alloc_varlen(pool, VarType::Bytes, len),
            DataType::Variant => Self::alloc_varlen(pool, VarType::Variant, len),
            DataType::Struct(fields, _) => Storage::Struct(
                fields
                    .iter()
                    .map(|(n, t)| (n.clone(), Column::alloc(pool, t, len)))
                    .collect(),
                None,
            ),
            DataType::Array(child) => return Self::alloc_list(pool, child, len),
        };
        Column { len, storage }
    }

    fn alloc_fixed(pool: &mut BufferPool, fixed_type: FixedType, len: usize) -> Storage {
        let validity = Validity::alloc(pool, len);
        Storage::Fixed {
            fixed_type,
            buffer: pool.take(len * fixed_type.size_of()),
            validity,
        }
    }

    fn alloc_varlen(pool: &mut BufferPool, var_type: VarType, len: usize) -> Storage {
        let validity = Validity::alloc(pool, len);
        let mut offsets = pool.take((len + 1) * 4);
        offsets.as_mut_slice::<u32>().fill(0u32);
        Storage::VarLen {
            var_type,
            offsets,
            data: pool.take(0),
            validity,
            ascii: true,
        }
    }

    /// `len` rows, all zero-length, with an empty child.
    fn alloc_list(pool: &mut BufferPool, child_type: &DataType, len: usize) -> Column {
        let validity = Validity::alloc(pool, len);
        let mut offsets = pool.take((len + 1) * 4);
        offsets.as_mut_slice::<u32>().fill(0u32);
        Column {
            len,
            storage: Storage::List {
                offsets,
                child: Box::new(Column::empty(child_type.clone(), 0)),
                validity,
            },
        }
    }

    /// Build a list column from a fully-populated child and its offsets.
    pub fn list_from_parts(offsets: Buffer, child: Column, validity: Validity, len: usize) -> Column {
        Column {
            len,
            storage: Storage::List {
                offsets,
                child: Box::new(child),
                validity,
            },
        }
    }

    /// Build a struct column from its field columns, each `len` rows.
    pub fn struct_from_parts(fields: Vec<(String, Column)>, len: usize) -> Column {
        Column {
            len,
            storage: Storage::Struct(fields, None),
        }
    }

    // -- inspection ----------------------------------------------------------

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// True when this column has no buffers.
    #[inline]
    pub fn is_unbound(&self) -> bool {
        matches!(self.storage, Storage::Empty(_))
    }

    /// The logical type. Allocates for Array and Struct; hot paths should take the type from the instruction instead.
    pub fn data_type(&self) -> DataType {
        match &self.storage {
            Storage::Empty(t) => t.clone(),
            Storage::Constant(value) => value.data_type(),
            Storage::Fixed { fixed_type, .. } => (*fixed_type).into(),
            Storage::Bits { .. } => DataType::Bool,
            Storage::VarLen { var_type, .. } => (*var_type).into(),
            Storage::List { child, .. } => DataType::Array(Box::new(child.data_type())),
            // Whether the object is open is the declaration's, which the column does not hold.
            Storage::Struct(fields, _) => {
                DataType::Struct(fields.iter().map(|(n, c)| (n.clone(), c.data_type())).collect(), false)
            }
        }
    }

    /// `None` for a column that carries no bitmap: `Empty` (every row null), `Struct` (each field carries its own),
    /// and an unexpanded `Constant` (every row valid). `has_nulls` and `is_valid` answer for all three without one.
    pub fn validity(&self) -> Option<&Validity> {
        match &self.storage {
            Storage::Empty(_) | Storage::Struct(..) | Storage::Constant(_) => None,
            Storage::Fixed { validity, .. }
            | Storage::Bits { validity, .. }
            | Storage::VarLen { validity, .. }
            | Storage::List { validity, .. } => Some(validity),
        }
    }

    pub fn validity_mut(&mut self) -> Option<&mut Validity> {
        match &mut self.storage {
            Storage::Empty(_) | Storage::Struct(..) | Storage::Constant(_) => None,
            Storage::Fixed { validity, .. }
            | Storage::Bits { validity, .. }
            | Storage::VarLen { validity, .. }
            | Storage::List { validity, .. } => Some(validity),
        }
    }

    #[inline]
    pub fn has_nulls(&self) -> bool {
        match &self.storage {
            Storage::Empty(_) => true,
            Storage::Struct(..) | Storage::Constant(_) => false,
            _ => self.validity().unwrap().has_nulls(),
        }
    }

    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        match &self.storage {
            Storage::Empty(_) => false,
            Storage::Struct(..) | Storage::Constant(_) => true,
            _ => self.validity().unwrap().is_valid(row),
        }
    }

    /// Turn this column into an all-null one of the same type, returning its buffers to the pool.
    pub fn set_all_null(&mut self, pool: &mut BufferPool) {
        let ty = self.data_type();
        self.reclaim_in_place(pool);
        self.storage = Storage::Empty(ty);
    }

    // -- typed accessors -----------------------------------------------------
    //
    // Err for the wrong type AND for Empty: a kernel that reaches an accessor with an Empty input has skipped its
    // prologue.

    fn wrong(&self, expected: &'static str) -> TypeError {
        TypeError {
            expected,
            found: self.data_type(),
        }
    }

    fn fixed_slice<T: bytemuck::AnyBitPattern>(
        &self,
        want: FixedType,
        name: &'static str,
    ) -> Result<Fixed<'_, T>, TypeError> {
        match &self.storage {
            Storage::Fixed {
                fixed_type,
                buffer,
                validity,
            } if *fixed_type == want => Ok(Fixed {
                values: &buffer.as_slice::<T>()[..self.len],
                validity,
            }),
            _ => Err(self.wrong(name)),
        }
    }

    fn fixed_slice_mut<T: bytemuck::NoUninit + bytemuck::AnyBitPattern>(
        &mut self,
        want: FixedType,
        name: &'static str,
    ) -> Result<FixedMut<'_, T>, TypeError> {
        let len = self.len;
        let found = self.data_type();
        match &mut self.storage {
            Storage::Fixed {
                fixed_type,
                buffer,
                validity,
            } if *fixed_type == want => Ok(FixedMut {
                values: &mut buffer.as_mut_slice::<T>()[..len],
                validity,
            }),
            _ => Err(TypeError { expected: name, found }),
        }
    }

    pub fn i64(&self) -> Result<Fixed<'_, i64>, TypeError> {
        self.fixed_slice(FixedType::I64, "I64")
    }
    pub fn i64_mut(&mut self) -> Result<FixedMut<'_, i64>, TypeError> {
        self.fixed_slice_mut(FixedType::I64, "I64")
    }
    pub fn f64(&self) -> Result<Fixed<'_, f64>, TypeError> {
        self.fixed_slice(FixedType::F64, "F64")
    }
    pub fn f64_mut(&mut self) -> Result<FixedMut<'_, f64>, TypeError> {
        self.fixed_slice_mut(FixedType::F64, "F64")
    }
    pub fn datetime(&self) -> Result<Fixed<'_, i64>, TypeError> {
        self.fixed_slice(FixedType::DateTime, "DateTime")
    }
    pub fn datetime_mut(&mut self) -> Result<FixedMut<'_, i64>, TypeError> {
        self.fixed_slice_mut(FixedType::DateTime, "DateTime")
    }
    pub fn duration(&self) -> Result<Fixed<'_, i64>, TypeError> {
        self.fixed_slice(FixedType::Duration, "Duration")
    }
    pub fn duration_mut(&mut self) -> Result<FixedMut<'_, i64>, TypeError> {
        self.fixed_slice_mut(FixedType::Duration, "Duration")
    }

    pub fn bool(&self) -> Result<Bits<'_>, TypeError> {
        match &self.storage {
            Storage::Bits { values, validity } => Ok(Bits {
                values: &values.as_slice::<u64>()[..bits::words_for(self.len)],
                validity,
            }),
            _ => Err(self.wrong("Bool")),
        }
    }

    pub fn bool_mut(&mut self) -> Result<BitsMut<'_>, TypeError> {
        let words = bits::words_for(self.len);
        let found = self.data_type();
        match &mut self.storage {
            Storage::Bits { values, validity } => Ok(BitsMut {
                values: &mut values.as_mut_slice::<u64>()[..words],
                validity,
            }),
            _ => Err(TypeError {
                expected: "Bool",
                found,
            }),
        }
    }

    /// A `Str` or `Bytes` column's rows. A variant column is refused: its bytes are no text.
    pub fn var(&self) -> Result<VarLen<'_>, TypeError> {
        match &self.storage {
            Storage::VarLen { var_type, .. } if *var_type != VarType::Variant => self.varlen(),
            _ => Err(self.wrong("Str or Bytes")),
        }
    }

    /// A variant column's rows, each one encoded value (`core::variant`).
    pub fn variant(&self) -> Result<VarLen<'_>, TypeError> {
        match &self.storage {
            Storage::VarLen {
                var_type: VarType::Variant,
                ..
            } => self.varlen(),
            _ => Err(self.wrong("Variant")),
        }
    }

    /// The rows of any variable-length column, whatever its bytes mean. For code that moves rows and reads none.
    pub fn varlen(&self) -> Result<VarLen<'_>, TypeError> {
        match &self.storage {
            Storage::VarLen {
                offsets,
                data,
                validity,
                ascii,
                ..
            } => {
                let o = &offsets.as_slice::<u32>()[..self.len + 1];
                Ok(VarLen {
                    offsets: o,
                    data: &data.as_slice::<u8>()[..o[self.len] as usize],
                    validity,
                    ascii: *ascii,
                })
            }
            _ => Err(self.wrong("a variable-length column")),
        }
    }

    pub fn list(&self) -> Result<List<'_>, TypeError> {
        match &self.storage {
            Storage::List {
                offsets,
                child,
                validity,
            } => Ok(List {
                offsets: &offsets.as_slice::<u32>()[..self.len + 1],
                child,
                validity,
            }),
            _ => Err(self.wrong("Array")),
        }
    }

    pub fn field(&self, name: &str) -> Option<&Column> {
        match &self.storage {
            Storage::Struct(fields, _) => fields.iter().find(|(n, _)| n == name).map(|(_, c)| c),
            _ => None,
        }
    }

    /// A struct's unclaimed fields: the keys a declared object met that it does not name, a variant object per row.
    /// `None` where there are none, and for any other column.
    pub fn unclaimed_fields(&self) -> Option<&Column> {
        match &self.storage {
            Storage::Struct(_, unclaimed_fields) => unclaimed_fields.as_deref(),
            _ => None,
        }
    }

    /// Build a struct column from its field columns and its unclaimed fields, each `len` rows.
    pub fn struct_with_unclaimed_fields(
        fields: Vec<(String, Column)>,
        unclaimed_fields: Option<Column>,
        len: usize,
    ) -> Column {
        Column {
            len,
            storage: Storage::Struct(fields, unclaimed_fields.map(Box::new)),
        }
    }

    // -- reclaim -------------------------------------------------------------

    /// Return every buffer to the pool.
    pub fn reclaim(mut self, pool: &mut BufferPool) {
        self.reclaim_in_place(pool);
    }

    /// Give buffers back and leave `Empty` of the same type. Idempotent.
    fn reclaim_in_place(&mut self, pool: &mut BufferPool) {
        let ty = self.data_type();
        // `mem::replace`, because a field cannot be moved out of a `Drop` type.
        let old = std::mem::replace(&mut self.storage, Storage::Empty(ty));
        match old {
            Storage::Empty(_) => {}
            Storage::Constant(_) => {}
            Storage::Fixed { buffer, validity, .. } => {
                pool.give(buffer);
                validity.reclaim(pool);
            }
            Storage::Bits { values, validity } => {
                pool.give(values);
                validity.reclaim(pool);
            }
            Storage::VarLen {
                offsets,
                data,
                validity,
                ..
            } => {
                pool.give(offsets);
                pool.give(data);
                validity.reclaim(pool);
            }
            Storage::List {
                offsets,
                child,
                validity,
            } => {
                pool.give(offsets);
                (*child).reclaim(pool);
                validity.reclaim(pool);
            }
            Storage::Struct(fields, unclaimed_fields) => {
                for (_, c) in fields {
                    c.reclaim(pool);
                }
                if let Some(unclaimed_fields) = unclaimed_fields {
                    unclaimed_fields.reclaim(pool);
                }
            }
        }
    }
}

/// A `Column` dropped with buffers has leaked them past the pool. Debug-only; in release the buffers are freed
/// normally.
impl Drop for Column {
    fn drop(&mut self) {
        // While unwinding, a second panic here would abort and destroy the first panic's message.
        if std::thread::panicking() {
            return;
        }
        debug_assert!(
            matches!(self.storage, Storage::Empty(_)),
            "Column dropped without reclaim() — its buffers leaked past the pool"
        );
    }
}

impl Column {
    /// Deep copy into pool-backed buffers. What `x = y` needs: an owned column to `put` where the evaluator only has a
    /// borrow.
    pub fn deep_copy(&self, pool: &mut BufferPool) -> Column {
        match &self.storage {
            Storage::Empty(t) => Column::empty(t.clone(), self.len),
            Storage::Constant(value) => Column::constant(value.clone(), self.len),
            Storage::Fixed {
                fixed_type,
                buffer,
                validity,
            } => {
                let mut b = pool.take(self.len * fixed_type.size_of());
                b.as_mut_slice::<u8>()[..self.len * 8].copy_from_slice(&buffer.as_slice::<u8>()[..self.len * 8]);
                Column {
                    len: self.len,
                    storage: Storage::Fixed {
                        fixed_type: *fixed_type,
                        buffer: b,
                        validity: validity.deep_copy(pool),
                    },
                }
            }
            Storage::Bits { values, validity } => {
                let words = bits::words_for(self.len);
                let mut b = pool.take(bits::bytes_for(self.len));
                b.as_mut_slice::<u64>()[..words].copy_from_slice(&values.as_slice::<u64>()[..words]);
                Column {
                    len: self.len,
                    storage: Storage::Bits {
                        values: b,
                        validity: validity.deep_copy(pool),
                    },
                }
            }
            Storage::VarLen {
                var_type,
                offsets,
                data,
                validity,
                ascii,
            } => {
                let o = &offsets.as_slice::<u32>()[..self.len + 1];
                let nbytes = o[self.len] as usize;
                let mut no = pool.take((self.len + 1) * 4);
                no.as_mut_slice::<u32>()[..self.len + 1].copy_from_slice(o);
                let mut nd = pool.take(nbytes);
                if nbytes > 0 {
                    nd.as_mut_slice::<u8>()[..nbytes].copy_from_slice(&data.as_slice::<u8>()[..nbytes]);
                }
                Column {
                    len: self.len,
                    storage: Storage::VarLen {
                        var_type: *var_type,
                        offsets: no,
                        data: nd,
                        validity: validity.deep_copy(pool),
                        ascii: *ascii,
                    },
                }
            }
            Storage::List {
                offsets,
                child,
                validity,
            } => {
                let o = &offsets.as_slice::<u32>()[..self.len + 1];
                let mut no = pool.take((self.len + 1) * 4);
                no.as_mut_slice::<u32>()[..self.len + 1].copy_from_slice(o);
                Column {
                    len: self.len,
                    storage: Storage::List {
                        offsets: no,
                        child: Box::new(child.deep_copy(pool)),
                        validity: validity.deep_copy(pool),
                    },
                }
            }
            Storage::Struct(fields, unclaimed_fields) => Column {
                len: self.len,
                storage: Storage::Struct(
                    fields.iter().map(|(n, c)| (n.clone(), c.deep_copy(pool))).collect(),
                    unclaimed_fields.as_ref().map(|u| Box::new(u.deep_copy(pool))),
                ),
            },
        }
    }

    /// Row selection: a new column of `indices.len()` rows, row `i` copied from row `indices[i]`. Every index must be
    /// in bounds. The building block of `Batch::gather`, and handles every storage variant.
    pub fn gather(&self, pool: &mut BufferPool, indices: &[u32]) -> Column {
        let n = indices.len();
        match &self.storage {
            Storage::Empty(t) => Column::empty(t.clone(), n),
            Storage::Constant(value) => Column::constant(value.clone(), n),

            Storage::Fixed {
                fixed_type,
                buffer,
                validity,
            } => {
                // Every `FixedType` is 8 bytes; a `u64` view copies the bit pattern regardless of which one this is.
                let src = &buffer.as_slice::<u64>()[..self.len];
                let mut buf = pool.take(n * 8);
                let mut new_validity = Validity::alloc(pool, n);
                {
                    let dst = &mut buf.as_mut_slice::<u64>()[..n];
                    for (i, &r) in indices.iter().enumerate() {
                        let r = r as usize;
                        dst[i] = src[r];
                        if !validity.is_valid(r) {
                            new_validity.set_valid(i, false);
                        }
                    }
                }
                new_validity.recount();
                Column {
                    len: n,
                    storage: Storage::Fixed {
                        fixed_type: *fixed_type,
                        buffer: buf,
                        validity: new_validity,
                    },
                }
            }

            Storage::Bits { values, validity } => {
                let src = &values.as_slice::<u64>()[..bits::words_for(self.len)];
                let mut buf = pool.take(bits::bytes_for(n));
                let mut new_validity = Validity::alloc(pool, n);
                {
                    let dst = &mut buf.as_mut_slice::<u64>()[..bits::words_for(n)];
                    bits::mask_tail(dst, n);
                    for (i, &r) in indices.iter().enumerate() {
                        let r = r as usize;
                        bits::set_row(dst, i, bits::get_row(src, r));
                        if !validity.is_valid(r) {
                            new_validity.set_valid(i, false);
                        }
                    }
                }
                new_validity.recount();
                Column {
                    len: n,
                    storage: Storage::Bits {
                        values: buf,
                        validity: new_validity,
                    },
                }
            }

            Storage::VarLen { var_type, .. } => {
                let v = self.varlen().expect("VarLen storage matched varlen()");
                let total: usize = indices.iter().map(|&r| v.row_len(r as usize)).sum();
                let mut b = StrBuilder::new(pool, *var_type, n);
                b.reserve(pool, total);
                for &r in indices {
                    let r = r as usize;
                    if v.validity.is_valid(r) {
                        b.push(v.get(r));
                    } else {
                        b.push_null();
                    }
                }
                b.finish()
            }

            Storage::List { .. } => {
                let l = self.list().expect("List storage matched list()");
                let mut child_indices = Vec::new();
                let mut new_offsets = Vec::with_capacity(n + 1);
                new_offsets.push(0u32);
                let mut running = 0u32;
                let mut new_validity = Validity::alloc(pool, n);
                for (i, &r) in indices.iter().enumerate() {
                    let r = r as usize;
                    if !l.validity.is_valid(r) {
                        new_validity.set_valid(i, false);
                    }
                    let (s, e) = (l.offsets[r], l.offsets[r + 1]);
                    child_indices.extend(s..e);
                    running += e - s;
                    new_offsets.push(running);
                }
                new_validity.recount();
                // The selected rows' element spans, concatenated, are a row-selection out of `child`.
                let new_child = l.child.gather(pool, &child_indices);
                let mut offsets_buf = pool.take((n + 1) * 4);
                offsets_buf.as_mut_slice::<u32>()[..n + 1].copy_from_slice(&new_offsets);
                Column::list_from_parts(offsets_buf, new_child, new_validity, n)
            }

            Storage::Struct(fields, unclaimed_fields) => Column::struct_with_unclaimed_fields(
                fields
                    .iter()
                    .map(|(name, c)| (name.clone(), c.gather(pool, indices)))
                    .collect(),
                unclaimed_fields.as_ref().map(|u| u.gather(pool, indices)),
                n,
            ),
        }
    }
}
