//! Operators: `BinOp`/`UnOp`, the type-and-dispatch table, and the kernels.
//!
//! The type checker and the executor read one table (`table`), so a type rule and its kernel cannot drift.

pub mod arith;
pub mod arrayops;
pub mod civil;
pub mod cmp;
pub mod convert;
pub mod datetime;
pub mod hashops;
pub mod logic;
pub mod netops;
pub mod objectops;
pub mod rebuildops;
pub mod stdfuncs;
pub mod strops;
pub mod table;
pub mod timeparse;
pub mod useragent;
pub mod webops;

use crate::core::column::Column;
use crate::core::ctx::Ctx;

/// An operator kernel. Returns a fresh column: expression intermediates have no slot to write into.
pub type OpKernel = fn(&mut Ctx, &[&Column]) -> Column;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,

    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,

    /// Selects per row; both sides always evaluate.
    And,
    Or,

    /// i64-native. No int32 coercion, no `>>>`.
    BitAnd,
    BitOr,
    BitXor,
    Shl,
    Shr,

    /// `??` tests null only; `||` tests falsiness.
    Coalesce,
}

impl BinOp {
    /// Every variant, so a table or a document that claims to cover the operators can be checked against it.
    pub const ALL: [BinOp; 20] = {
        use BinOp::*;
        [
            Add, Sub, Mul, Div, Mod, Pow, Eq, Ne, Lt, Le, Gt, Ge, And, Or, BitAnd, BitOr, BitXor, Shl, Shr, Coalesce,
        ]
    };

    /// `==`, `!=`, `<`, `<=`, `>` and `>=`: true or false on every row, a null operand included.
    pub fn is_comparison(&self) -> bool {
        use BinOp::*;
        matches!(self, Eq | Ne | Lt | Le | Gt | Ge)
    }

    pub fn symbol(&self) -> &'static str {
        use BinOp::*;
        match self {
            Add => "+",
            Sub => "-",
            Mul => "*",
            Div => "/",
            Mod => "%",
            Pow => "**",
            Eq => "==",
            Ne => "!=",
            Lt => "<",
            Le => "<=",
            Gt => ">",
            Ge => ">=",
            And => "&&",
            Or => "||",
            BitAnd => "&",
            BitOr => "|",
            BitXor => "^",
            Shl => "<<",
            Shr => ">>",
            Coalesce => "??",
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
pub enum UnOp {
    Neg,
    Not,
    BitNot,
}

impl UnOp {
    pub const ALL: [UnOp; 3] = [UnOp::Neg, UnOp::Not, UnOp::BitNot];

    pub fn symbol(&self) -> &'static str {
        match self {
            UnOp::Neg => "-",
            UnOp::Not => "!",
            UnOp::BitNot => "~",
        }
    }
}

pub(crate) mod helpers {
    use std::borrow::Cow;

    /// The position index `i` names among `n` elements, counting from the end when negative: `-1` is the last. `None`
    /// outside them. What `at`, `remove` and an element write read and write.
    pub fn position(i: i64, n: usize) -> Option<usize> {
        let n = n as i64;
        let i = if i < 0 { i + n } else { i };
        (0..n).contains(&i).then_some(i as usize)
    }

    /// The position index `i` names among `n` elements, counting from the end when negative, clamped to `0..=n`:
    /// a gap between elements, as `insert` and `slice` take one.
    pub fn clamped(i: i64, n: usize) -> usize {
        let n = n as i64;
        (if i < 0 { i + n } else { i }).clamp(0, n) as usize
    }

    use crate::core::bits;
    use crate::core::column::{Column, DataType, Fixed, Scalar, Validity, VarLen};
    use crate::core::ctx::Ctx;

    /// One kernel operand as the loop reads it: a row of the column, or the one value a constant column holds for
    /// every row. A kernel body is generic over this, so the loop over rows is a slice loop and the loop over a
    /// constant reads a register.
    pub trait Operand<T: Copy> {
        fn at(&self, i: usize) -> T;
    }

    /// A fixed-width operand as one slice, for a loop over three operands. A constant fills a `Vec`, outside the
    /// pool.
    pub trait Rows<T: Copy>: Operand<T> {
        fn rows(&self, len: usize) -> Cow<'_, [T]>;
    }

    impl<T: Copy> Operand<T> for &[T] {
        #[inline(always)]
        fn at(&self, i: usize) -> T {
            self[i]
        }
    }

    impl<T: Copy> Rows<T> for &[T] {
        fn rows(&self, _len: usize) -> Cow<'_, [T]> {
            Cow::Borrowed(self)
        }
    }

    impl<'a> Operand<&'a [u8]> for VarLen<'a> {
        #[inline(always)]
        fn at(&self, i: usize) -> &'a [u8] {
            self.get(i)
        }
    }

    impl<T: Copy> Operand<T> for Cow<'_, [T]> {
        #[inline(always)]
        fn at(&self, i: usize) -> T {
            self[i]
        }
    }

    impl<T: Copy> Rows<T> for Cow<'_, [T]> {
        fn rows(&self, _len: usize) -> Cow<'_, [T]> {
            Cow::Borrowed(self)
        }
    }

    /// The constant form of an operand.
    #[derive(Clone, Copy)]
    pub struct One<T>(pub T);

    impl<T: Copy> Operand<T> for One<T> {
        #[inline(always)]
        fn at(&self, _i: usize) -> T {
            self.0
        }
    }

    impl<T: Copy> Rows<T> for One<T> {
        fn rows(&self, len: usize) -> Cow<'_, [T]> {
            Cow::Owned(vec![self.0; len])
        }
    }

    /// The element type behind a column accessor's name, so a macro that takes the accessor can name what `at` gives.
    macro_rules! native {
        (i64) => {
            i64
        };
        (f64) => {
            f64
        };
        (datetime) => {
            i64
        };
        (duration) => {
            i64
        };
    }
    pub(crate) use native;

    /// A column as a kernel sees it: its rows with their validity, or its one value. A constant has no bitmap; it is
    /// valid in every row.
    ///
    /// A kernel reads it one of three ways. A tight loop goes through `two_sides!`, which matches once and runs a body
    /// generic over `Operand`. A loop that builds a string per row reads `at` and `is_valid` straight off the `Side`,
    /// a match per row that costs nothing beside the row's own work. A loop over three operands takes each as a
    /// slice through `Rows`, a constant filled once.
    pub enum Side<'a, R, T> {
        Rows(R, &'a Validity),
        One(T),
    }

    impl<'a, R, T> Side<'a, R, T> {
        /// Whether row `i` holds a value. A constant does in every row.
        #[inline]
        pub fn is_valid(&self, i: usize) -> bool {
            match self {
                Side::Rows(_, v) => v.is_valid(i),
                Side::One(_) => true,
            }
        }

        /// The bitmap, for `alloc_with_merged_validity`. A constant has none.
        pub fn validity(&self) -> Option<&'a Validity> {
            match self {
                Side::Rows(_, v) => Some(v),
                Side::One(_) => None,
            }
        }
    }

    impl Side<'_, VarLen<'_>, &[u8]> {
        /// The bytes `len` rows hold: what a builder reserves. A constant holds its length in every row.
        pub fn total_bytes(&self, len: usize) -> usize {
            match self {
                Side::Rows(v, _) => v.total_bytes(),
                Side::One(s) => s.len() * len,
            }
        }
    }

    impl<R: Operand<T>, T: Copy> Operand<T> for Side<'_, R, T> {
        #[inline]
        fn at(&self, i: usize) -> T {
            match self {
                Side::Rows(r, _) => r.at(i),
                Side::One(v) => *v,
            }
        }
    }

    impl<R: Rows<T>, T: Copy> Rows<T> for Side<'_, R, T> {
        fn rows(&self, len: usize) -> Cow<'_, [T]> {
            match self {
                Side::Rows(r, _) => r.rows(len),
                Side::One(v) => Cow::Owned(vec![*v; len]),
            }
        }
    }

    /// The accessors that build a `Side`, named for the column accessor each one wraps so a macro can take the
    /// accessor's name and call both. `None` where the column accessor answers `Err`: the column is neither rows of
    /// the type nor a constant of it.
    pub mod side {
        use super::*;

        fn fixed<'a, T: Copy>(
            f: Result<Fixed<'a, T>, crate::core::column::TypeError>,
            one: Option<T>,
        ) -> Option<Side<'a, &'a [T], T>> {
            match (f, one) {
                (Ok(f), _) => Some(Side::Rows(f.values, f.validity)),
                (Err(_), Some(v)) => Some(Side::One(v)),
                (Err(_), None) => None,
            }
        }

        pub fn i64(c: &Column) -> Option<Side<'_, &[i64], i64>> {
            fixed(
                c.i64(),
                match c.constant_value() {
                    Some(Scalar::I64(x)) => Some(*x),
                    _ => None,
                },
            )
        }
        pub fn f64(c: &Column) -> Option<Side<'_, &[f64], f64>> {
            fixed(
                c.f64(),
                match c.constant_value() {
                    Some(Scalar::F64(x)) => Some(*x),
                    _ => None,
                },
            )
        }
        pub fn datetime(c: &Column) -> Option<Side<'_, &[i64], i64>> {
            fixed(
                c.datetime(),
                match c.constant_value() {
                    Some(Scalar::DateTime(x)) => Some(*x),
                    _ => None,
                },
            )
        }
        pub fn duration(c: &Column) -> Option<Side<'_, &[i64], i64>> {
            fixed(
                c.duration(),
                match c.constant_value() {
                    Some(Scalar::Duration(x)) => Some(*x),
                    _ => None,
                },
            )
        }
        pub fn var(c: &Column) -> Option<Side<'_, VarLen<'_>, &[u8]>> {
            match (c.var(), c.constant_value()) {
                (Ok(v), _) => {
                    let validity = v.validity;
                    Some(Side::Rows(v, validity))
                }
                (Err(_), Some(Scalar::Str(s))) => Some(Side::One(s.as_bytes())),
                (Err(_), Some(Scalar::Bytes(b))) => Some(Side::One(b.as_slice())),
                (Err(_), _) => None,
            }
        }
    }

    /// Run a two-operand kernel body on the four shapes its operands can take. `$run` is a generic function over two
    /// `Operand`s, given `(ctx, a, b, a's validity, b's validity, len)`.
    macro_rules! two_sides {
        ($ctx:ident, $ins:ident, $len:ident, $la:ident, $ra:ident, $run:ident) => {
            match (
                $crate::ops::helpers::side::$la($ins[0]).expect("an operand of the row's type"),
                $crate::ops::helpers::side::$ra($ins[1]).expect("an operand of the row's type"),
            ) {
                (Side::Rows(a, va), Side::Rows(b, vb)) => $run($ctx, a, b, Some(va), Some(vb), $len),
                (Side::Rows(a, va), Side::One(b)) => $run($ctx, a, One(b), Some(va), None, $len),
                (Side::One(a), Side::Rows(b, vb)) => $run($ctx, One(a), b, None, Some(vb), $len),
                (Side::One(a), Side::One(b)) => $run($ctx, One(a), One(b), None, None, $len),
            }
        };
    }
    pub(crate) use two_sides;

    /// Allocate an output column whose validity is the AND of every bitmap given (null in, null out). Values at null
    /// rows are still computed; nothing reads them. A constant operand has no bitmap and passes `None`: it is valid in
    /// every row, so it drops out of the AND. No bitmap at all and the result is valid in every row. An `Empty` input
    /// has no bitmap either and is null in every row; the caller has already answered for it, through
    /// `short_circuit_empty`.
    pub fn alloc_with_merged_validity(
        ctx: &mut Ctx,
        out_ty: &DataType,
        len: usize,
        bitmaps: &[Option<&Validity>],
    ) -> Column {
        let mut out = Column::alloc(ctx.pool, out_ty, len);
        let mut present = bitmaps.iter().flatten();
        let Some(first) = present.next() else {
            return out;
        };
        {
            let ov = out.validity_mut().expect("fixed/varlen output has validity");
            match present.next() {
                None => ov.bits_mut().copy_from_slice(first.bits()),
                Some(second) => {
                    bits::and_into(ov.bits_mut(), first.bits(), second.bits(), len);
                    for v in present {
                        for (d, s) in ov.bits_mut().iter_mut().zip(v.bits()) {
                            *d &= *s;
                        }
                    }
                }
            }
            ov.recount();
        }
        out
    }

    /// The kernel prologue: an unbound (`Empty`) input makes the whole result null, so return an `Empty` output without
    /// allocating.
    ///
    /// Not for the operators where null does not propagate (`??`, `||`, `&&`, `isNull`).
    #[inline]
    pub fn short_circuit_empty(ins: &[&Column], out_ty: &DataType, len: usize) -> Option<Column> {
        if ins.iter().any(|c| c.is_unbound()) {
            Some(Column::empty(out_ty.clone(), len))
        } else {
            None
        }
    }

    /// A numeric column's values as `f64`, widening `I64`. This is the one place the I64 -> F64 widening is written.
    /// `None` for a non-numeric column.
    pub fn f64_values(c: &Column) -> Option<Cow<'_, [f64]>> {
        if let Ok(f) = c.f64() {
            Some(Cow::Borrowed(f.values))
        } else {
            None
        }
    }

    /// Fold `unify` over the types a kernel's inputs carry: equal types stay, `I64` and `F64` widen, two structs take
    /// the union of their fields, and a variant takes any. Types the compiler let meet always unify; any that do not
    /// meet as a variant, which holds each value as it is. `None` for an empty iterator.
    ///
    /// The table is stated for the rows of one dynamically-parsed column; this is the same question asked across the
    /// columns a kernel was handed, and it takes the same answer: `null` takes the other's type, at any depth. What an
    /// unbound input contributes is the caller's to decide.
    pub fn blend_types(types: impl IntoIterator<Item = DataType>) -> Option<DataType> {
        let mut it = types.into_iter();
        let first = it.next()?;
        Some(it.fold(first, |acc, t| acc.unify(&t).unwrap_or(DataType::Variant)))
    }

    /// A fixed-width 8-byte column's values as `i64` (`I64`, `DateTime`, `Duration` share one storage shape). `None`
    /// otherwise.
    pub fn i64_values(c: &Column) -> Option<(&[i64], &Validity)> {
        [c.i64(), c.datetime(), c.duration()]
            .into_iter()
            .flatten()
            .next()
            .map(|f| (f.values, f.validity))
    }
}

/// Columns for the tests that run a kernel on a constant beside rows: `ops::table` and `ops::stdfuncs`.
#[cfg(test)]
pub(crate) mod testing {
    use crate::core::bits;
    use crate::core::buffer::BufferPool;
    use crate::core::column::{Column, DataType, Scalar, StrBuilder, Validity, VarType};
    use crate::core::ctx::Ctx;
    use crate::core::error::{ErrorSink, ExceptionKind};
    use crate::format::json::write_value;
    use crate::ops::OpKernel;
    use DataType::*;

    /// Four rows of `ty`: the value `one_of` gives, a null, a zero or empty, and an edge that overflows when added to
    /// a small constant. An array's rows are two elements, null, none, and two elements, over those four as its child.
    pub fn rows_of(p: &mut BufferPool, ty: &DataType) -> Column {
        match ty {
            I64 | DateTime | Duration => {
                let mut c = Column::alloc(p, ty, 4);
                let f = match ty {
                    I64 => c.i64_mut().unwrap(),
                    DateTime => c.datetime_mut().unwrap(),
                    _ => c.duration_mut().unwrap(),
                };
                f.values.copy_from_slice(&[2, 0, 0, i64::MAX]);
                f.validity.set_valid(1, false);
                c
            }
            F64 => {
                let mut c = Column::alloc(p, ty, 4);
                let f = c.f64_mut().unwrap();
                f.values.copy_from_slice(&[2.0, 0.0, 0.0, f64::MAX]);
                f.validity.set_valid(1, false);
                c
            }
            Bool => {
                let mut c = Column::alloc(p, ty, 4);
                let b = c.bool_mut().unwrap();
                for (i, v) in [true, false, false, true].into_iter().enumerate() {
                    bits::set_row(b.values, i, v);
                }
                b.validity.set_valid(1, false);
                c
            }
            Str | Bytes => {
                let var = if *ty == Str { VarType::Str } else { VarType::Bytes };
                let mut b = StrBuilder::new(p, var, 4);
                b.reserve(p, 8);
                b.push(b"s");
                b.push_null();
                b.push(b"");
                b.push(b"a");
                b.finish()
            }
            Array(elem) => {
                let child = rows_of(p, elem);
                let mut offsets = p.take(5 * 4);
                offsets.as_mut_slice::<u32>().copy_from_slice(&[0, 2, 2, 2, 4]);
                let mut validity = Validity::alloc(p, 4);
                validity.set_valid(1, false);
                Column::list_from_parts(offsets, child, validity, 4)
            }
            other => panic!("no rows for {other:?}"),
        }
    }

    /// One value of `ty`. It is in `rows_of`, so a search finds it, and the text is a unit `duration` knows.
    pub fn one_of(ty: &DataType) -> Scalar {
        match ty {
            I64 => Scalar::I64(2),
            F64 => Scalar::F64(2.0),
            DateTime => Scalar::DateTime(2),
            Duration => Scalar::Duration(2),
            Bool => Scalar::Bool(true),
            Str => Scalar::Str("s".into()),
            Bytes => Scalar::Bytes(b"s".to_vec()),
            other => panic!("no constant for {other:?}"),
        }
    }

    /// A kernel's output: each row as JSON or `None` for null, the type, and the exceptions it raised.
    pub fn run(
        p: &mut BufferPool,
        k: OpKernel,
        ins: &[&Column],
    ) -> (Vec<Option<String>>, DataType, Vec<ExceptionKind>) {
        let mut errors = ErrorSink::new();
        let out = {
            let mut ctx = Ctx::new(p, &mut errors);
            k(&mut ctx, ins)
        };
        let ty = out.data_type();
        let rows = (0..out.len())
            .map(|i| {
                let mut buf = Vec::new();
                write_value(&out, &ty, i, &mut buf).then(|| String::from_utf8_lossy(&buf).into_owned())
            })
            .collect();
        out.reclaim(p);
        (rows, ty, errors.entries().iter().map(|a| a.kind.clone()).collect())
    }
}
