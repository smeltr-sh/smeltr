//! Reads of a `bytes` value at a position: `readInt`, `readFloat` and `readBytes`, what a binary layout is read
//! with. Each reads only inside the value: a position or a count that runs past its end, or is negative, is null with
//! an `IndexOutOfRange` on its row, so no input reads more than the message holds.
//!
//! A number's type is named as `unpack`'s layout names it, `"u8"` to `"u64"`, `"i8"` to `"i64"`, `"f32"` and
//! `"f64"`, and its byte order `"big"` or `"little"`, big when the call names none: network order.

use crate::core::bits;
use crate::core::column::{Column, DataType, FixedMut, StrBuilder, VarLen, VarType};
use crate::core::ctx::Ctx;
use crate::core::error::ExceptionKind;
use crate::ops::helpers::{Labels, Operand, Side, clamped, short_circuit_empty, side};
use memchr::memmem;

/// A number's type in a layout.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum NumType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
}

/// Each type with its name and its width in bytes: what `NumType::of`, `name` and `width` read.
const NUM_TYPES: [(NumType, &str, usize); 10] = [
    (NumType::U8, "u8", 1),
    (NumType::U16, "u16", 2),
    (NumType::U32, "u32", 4),
    (NumType::U64, "u64", 8),
    (NumType::I8, "i8", 1),
    (NumType::I16, "i16", 2),
    (NumType::I32, "i32", 4),
    (NumType::I64, "i64", 8),
    (NumType::F32, "f32", 4),
    (NumType::F64, "f64", 8),
];

impl NumType {
    /// The type `name` spells, or `None`.
    pub fn of(name: &str) -> Option<NumType> {
        NUM_TYPES.iter().find(|(_, n, _)| *n == name).map(|(t, _, _)| *t)
    }

    fn row(self) -> &'static (NumType, &'static str, usize) {
        NUM_TYPES
            .iter()
            .find(|(t, _, _)| *t == self)
            .expect("every type has a row")
    }

    /// How a call spells it: what `of` reads.
    pub fn name(self) -> &'static str {
        self.row().1
    }

    pub fn width(self) -> usize {
        self.row().2
    }

    pub fn is_float(self) -> bool {
        matches!(self, NumType::F32 | NumType::F64)
    }

    /// The integer `bytes` hold, `bytes.len()` being this type's width. A `u64` above `i64::MAX` is an `Overflow`.
    fn int(self, bytes: &[u8], order: Order) -> Result<i64, ExceptionKind> {
        let mut wide = [0u8; 8];
        let w = bytes.len();
        // Laid out big-endian in the low `w` bytes, then sign-extended.
        match order {
            Order::Big => wide[8 - w..].copy_from_slice(bytes),
            Order::Little => {
                for (d, s) in wide[8 - w..].iter_mut().zip(bytes.iter().rev()) {
                    *d = *s;
                }
            }
        }
        let raw = u64::from_be_bytes(wide);
        match self {
            NumType::I8 | NumType::I16 | NumType::I32 | NumType::I64 => {
                let shift = 64 - 8 * w as u32;
                Ok(((raw << shift) as i64) >> shift)
            }
            _ => i64::try_from(raw).map_err(|_| ExceptionKind::Overflow),
        }
    }

    /// The float `bytes` hold, `bytes.len()` being this type's width.
    fn float(self, bytes: &[u8], order: Order) -> f64 {
        let mut wide = [0u8; 8];
        wide[..bytes.len()].copy_from_slice(bytes);
        let [a, b, c, d, ..] = wide;
        match (self, order) {
            (NumType::F32, Order::Big) => f32::from_be_bytes([a, b, c, d]) as f64,
            (NumType::F32, Order::Little) => f32::from_le_bytes([a, b, c, d]) as f64,
            (_, Order::Big) => f64::from_be_bytes(wide),
            (_, Order::Little) => f64::from_le_bytes(wide),
        }
    }
}

/// A byte order.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Order {
    Big,
    Little,
}

impl Order {
    /// The order `name` spells, or `None`.
    pub fn of(name: &str) -> Option<Order> {
        match name {
            "big" => Some(Order::Big),
            "little" => Some(Order::Little),
            _ => None,
        }
    }
}

/// The `n` bytes of `b` from `at`, or the `IndexOutOfRange` of a read outside it.
fn window(b: &[u8], at: i64, n: i64) -> Result<&[u8], ExceptionKind> {
    let (Ok(at), Ok(n)) = (usize::try_from(at), usize::try_from(n)) else {
        return Err(ExceptionKind::IndexOutOfRange);
    };
    at.checked_add(n)
        .and_then(|end| b.get(at..end))
        .ok_or(ExceptionKind::IndexOutOfRange)
}

/// A number read from its bytes: `None` for a type the call does not read, an integer to `readFloat` or a float to
/// `readInt`.
type NumRead<T> = fn(NumType, &[u8], Order) -> Option<Result<T, ExceptionKind>>;

/// The number at `ins[1]` in `ins[0]`, typed `ins[2]`, in the order `ins[3]` or big, read by `read` into a column
/// `slots` opens. A type or an order that is not one of the names above is a `CoercionRefused`, and so is a float type to
/// `readInt` or an integer one to `readFloat`.
fn read_num<T: Copy + Default>(
    ctx: &mut Ctx,
    ins: &[&Column],
    to: DataType,
    read: NumRead<T>,
    slots: for<'a> fn(&'a mut Column) -> FixedMut<'a, T>,
) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &to, len) {
        return e;
    }
    let (Some(b), Some(at), Some(ty)) = (side::var(ins[0]), side::i64(ins[1]), side::var(ins[2])) else {
        return Column::empty(to, len);
    };
    let order = match ins.get(3) {
        None => None,
        Some(c) => match side::var(c) {
            Some(o) => Some(o),
            None => return Column::empty(to, len),
        },
    };
    let refused = ExceptionKind::CoercionRefused { to: to.clone() };
    let mut types = Labels::new(NumType::of);
    let mut orders = Labels::new(Order::of);
    let mut out = Column::alloc(ctx.pool, &to, len);
    let mut raised = Vec::new();
    {
        let o = slots(&mut out);
        for i in 0..len {
            let given =
                b.is_valid(i) && at.is_valid(i) && ty.is_valid(i) && order.as_ref().is_none_or(|o| o.is_valid(i));
            let value = given.then(|| {
                let ty = types.get(ty.at(i));
                let order = match &order {
                    None => Some(Order::Big),
                    Some(o) => orders.get(o.at(i)),
                };
                let (Some(ty), Some(order)) = (ty, order) else {
                    return Err(refused.clone());
                };
                let bytes = window(b.at(i), at.at(i), ty.width() as i64)?;
                read(ty, bytes, order).unwrap_or_else(|| Err(refused.clone()))
            });
            // Every row is written: a pooled buffer is not zeroed.
            match value {
                Some(Ok(v)) => o.values[i] = v,
                failed => {
                    o.values[i] = T::default();
                    o.validity.set_valid(i, false);
                    if let Some(Err(kind)) = failed {
                        raised.push((i, kind));
                    }
                }
            }
        }
    }
    for (row, kind) in raised {
        ctx.raise(row, kind);
    }
    out
}

/// `readInt(b, at, type[, order])` -> `I64`: the integer of `type`, `"u8"` to `"i64"`, at byte `at`.
pub fn read_int(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    read_num(
        ctx,
        ins,
        DataType::I64,
        |ty, bytes, order| (!ty.is_float()).then(|| ty.int(bytes, order)),
        |c| c.i64_mut().expect("an i64 column"),
    )
}

/// `readFloat(b, at, type[, order])` -> `F64`: the float of `type`, `"f32"` or `"f64"`, at byte `at`. A NaN or an
/// infinity, which the language's `f64` does not hold, is null with a `NaN` error.
pub fn read_float(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    read_num(
        ctx,
        ins,
        DataType::F64,
        |ty, bytes, order| {
            ty.is_float().then(|| {
                let f = ty.float(bytes, order);
                if f.is_finite() { Ok(f) } else { Err(ExceptionKind::NaN) }
            })
        },
        |c| c.f64_mut().expect("an f64 column"),
    )
}

/// `readBytes(b, at, n)` -> `Bytes`: the `n` bytes from byte `at`.
pub fn read_bytes(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Bytes, len) {
        return e;
    }
    let (Some(b), Some(at), Some(n)) = (side::var(ins[0]), side::i64(ins[1]), side::i64(ins[2])) else {
        return Column::empty(DataType::Bytes, len);
    };
    let mut out = StrBuilder::new(ctx.pool, VarType::Bytes, len);
    let mut raised = Vec::new();
    for i in 0..len {
        if !(b.is_valid(i) && at.is_valid(i) && n.is_valid(i)) {
            out.push_null();
            continue;
        }
        match window(b.at(i), at.at(i), n.at(i)) {
            Ok(bytes) => out.push_grow(ctx.pool, bytes),
            Err(kind) => {
                out.push_null();
                raised.push((i, kind));
            }
        }
    }
    for (row, kind) in raised {
        ctx.raise(row, kind);
    }
    out.finish()
}

/// Where `needle` starts in `b`: the first start at or after `from`, or, with `from_end`, the last start at or before
/// it; `None` if there is none. `from` is read as JavaScript reads it on a byte array: a negative one counts from the
/// end.
fn find(b: &[u8], needle: &[u8], from_end: bool, from: Option<i64>) -> Option<usize> {
    let n = b.len();
    if from_end {
        let last = match from {
            None => n,
            Some(p) if p >= 0 => usize::try_from(p).map_or(n, |p| p.min(n)),
            Some(p) => usize::try_from(n as i64 + p).ok()?,
        };
        memmem::rfind(&b[..n.min(last.saturating_add(needle.len()))], needle)
    } else {
        let first = from.map_or(0, |p| clamped(p, n));
        memmem::find(&b[first..], needle).map(|at| first + at)
    }
}

/// What `indexOf`, `lastIndexOf` and `includes` search bytes for.
enum Needle<'a> {
    /// A byte's number.
    Byte(Side<'a, &'a [i64], i64>),
    /// A run of bytes.
    Run(Side<'a, VarLen<'a>, &'a [u8]>),
}

/// The search behind `indexOf`, `lastIndexOf` and `includes` on bytes: the position from [`find`], or `-1`;
/// `includes` reads it as `>= 0`. What is searched for is a byte's number or `bytes`; a number outside 0 to 255 is no
/// byte, so it is never found. A null value, search or `from` is a null answer.
fn search(ctx: &mut Ctx, ins: &[&Column], from_end: bool, to: DataType) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &to, len) {
        return e;
    }
    // The searched value must be a column, not a constant, as for `indexOf` on text and arrays.
    let Ok(b) = ins[0].var() else {
        return Column::empty(to, len);
    };
    let needle = match ins[1].data_type() {
        DataType::I64 => side::i64(ins[1]).map(Needle::Byte),
        _ => side::var(ins[1]).map(Needle::Run),
    };
    let Some(needle) = needle else {
        return Column::empty(to, len);
    };
    let from = ins.get(2).map(|c| side::i64(c));
    if matches!(from, Some(None)) {
        return Column::empty(to, len);
    }
    let from = from.flatten();
    let found = |i: usize| -> Option<Option<usize>> {
        let from = match &from {
            None => None,
            Some(f) if f.is_valid(i) => Some(f.at(i)),
            Some(_) => return None,
        };
        if !b.validity.is_valid(i) {
            return None;
        }
        let one;
        let needle: &[u8] = match &needle {
            Needle::Byte(n) if !n.is_valid(i) => return None,
            Needle::Byte(n) => match u8::try_from(n.at(i)) {
                Ok(v) => {
                    one = [v];
                    &one
                }
                Err(_) => return Some(None),
            },
            Needle::Run(r) if !r.is_valid(i) => return None,
            Needle::Run(r) => r.at(i),
        };
        Some(find(b.get(i), needle, from_end, from))
    };
    let mut out = Column::alloc(ctx.pool, &to, len);
    match to {
        DataType::Bool => {
            let o = out.bool_mut().expect("a bool column");
            for i in 0..len {
                let pos = found(i);
                bits::set_row(o.values, i, pos.flatten().is_some());
                o.validity.set_valid(i, pos.is_some());
            }
            o.validity.recount();
        }
        _ => {
            let o = out.i64_mut().expect("an i64 column");
            for i in 0..len {
                let pos = found(i);
                o.values[i] = pos.flatten().map_or(-1, |p| p as i64);
                o.validity.set_valid(i, pos.is_some());
            }
            o.validity.recount();
        }
    }
    out
}

/// `indexOf(b, search[, from])` -> `I64`: where `search`, a byte's number or `bytes`, first comes at or after `from`.
pub fn index_of(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    search(ctx, ins, false, DataType::I64)
}

/// `lastIndexOf(b, search[, from])` -> `I64`: where `search` last comes at or before `from`.
pub fn last_index_of(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    search(ctx, ins, true, DataType::I64)
}

/// `includes(b, search[, from])` -> `Bool`: whether `search` occurs at or after `from`.
pub fn includes(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    search(ctx, ins, false, DataType::Bool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_integer_reads_in_either_order_and_sign_extends() {
        let b = [0xff, 0xfe];
        assert_eq!(NumType::U16.int(&b, Order::Big), Ok(0xfffe));
        assert_eq!(NumType::U16.int(&b, Order::Little), Ok(0xfeff));
        assert_eq!(NumType::I16.int(&b, Order::Big), Ok(-2));
        assert_eq!(NumType::I8.int(&[0x80], Order::Big), Ok(-128));
        assert_eq!(NumType::I64.int(&[0xff; 8], Order::Little), Ok(-1));
    }

    #[test]
    fn a_u64_past_i64_overflows() {
        assert_eq!(NumType::U64.int(&[0xff; 8], Order::Big), Err(ExceptionKind::Overflow));
        assert_eq!(
            NumType::U64.int(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], Order::Big),
            Ok(i64::MAX)
        );
    }

    #[test]
    fn a_float_reads_in_either_order() {
        assert_eq!(NumType::F32.float(&1.5f32.to_be_bytes(), Order::Big), 1.5);
        assert_eq!(NumType::F64.float(&(-2.25f64).to_le_bytes(), Order::Little), -2.25);
    }

    #[test]
    fn a_run_is_found_from_a_position_in_either_direction() {
        let b = b"a\0b\0\0c";
        assert_eq!(find(b, b"\0", false, None), Some(1));
        assert_eq!(find(b, b"\0", false, Some(2)), Some(3));
        assert_eq!(find(b, b"\0\0", false, None), Some(3));
        assert_eq!(find(b, b"\0", true, None), Some(4));
        assert_eq!(find(b, b"\0", true, Some(3)), Some(3));
        assert_eq!(find(b, b"\0", true, Some(-4)), Some(1));
        assert_eq!(find(b, b"\0", false, Some(-2)), Some(4));
        assert_eq!(find(b, b"\0", true, Some(-9)), None);
        assert_eq!(find(b, b"z", false, None), None);
        assert_eq!(find(b, b"", false, Some(9)), Some(6));
    }

    #[test]
    fn a_window_outside_the_value_is_out_of_range() {
        assert_eq!(window(b"abc", 1, 2), Ok(&b"bc"[..]));
        assert_eq!(window(b"abc", 3, 0), Ok(&b""[..]));
        for (at, n) in [(2, 2), (-1, 1), (0, -1), (i64::MAX, 1), (4, 0)] {
            assert_eq!(window(b"abc", at, n), Err(ExceptionKind::IndexOutOfRange), "{at} {n}");
        }
    }
}
