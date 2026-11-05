//! Reads of a `bytes` value at a position: `readInt`, `readFloat` and `readBytes`, what a binary layout is read
//! with. Each reads only inside the value: a position or a count that runs past its end, or is negative, is null with
//! an `IndexOutOfRange` on its row, so no input reads more than the message holds.
//!
//! A number's type is named as `unpack`'s layout names it, `"u8"` to `"u64"`, `"i8"` to `"i64"`, `"f32"` and
//! `"f64"`, and its byte order `"big"` or `"little"`, big when the call names none: network order.

use crate::core::column::{Column, DataType, FixedMut, StrBuilder, VarType};
use crate::core::ctx::Ctx;
use crate::core::error::ExceptionKind;
use crate::ops::helpers::{Labels, Operand, short_circuit_empty, side};

/// A number's type in a layout: its width in bytes, and how its bytes are read.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum NumType {
    Unsigned(usize),
    Signed(usize),
    Float(usize),
}

impl NumType {
    /// The type `name` spells, or `None`.
    pub fn of(name: &str) -> Option<NumType> {
        Some(match name {
            "u8" => NumType::Unsigned(1),
            "u16" => NumType::Unsigned(2),
            "u32" => NumType::Unsigned(4),
            "u64" => NumType::Unsigned(8),
            "i8" => NumType::Signed(1),
            "i16" => NumType::Signed(2),
            "i32" => NumType::Signed(4),
            "i64" => NumType::Signed(8),
            "f32" => NumType::Float(4),
            "f64" => NumType::Float(8),
            _ => return None,
        })
    }

    /// How a call spells it: what `of` reads.
    pub fn name(self) -> &'static str {
        match self {
            NumType::Unsigned(1) => "u8",
            NumType::Unsigned(2) => "u16",
            NumType::Unsigned(4) => "u32",
            NumType::Unsigned(_) => "u64",
            NumType::Signed(1) => "i8",
            NumType::Signed(2) => "i16",
            NumType::Signed(4) => "i32",
            NumType::Signed(_) => "i64",
            NumType::Float(4) => "f32",
            NumType::Float(_) => "f64",
        }
    }

    pub fn width(self) -> usize {
        match self {
            NumType::Unsigned(w) | NumType::Signed(w) | NumType::Float(w) => w,
        }
    }

    pub fn is_float(self) -> bool {
        matches!(self, NumType::Float(_))
    }

    /// The integer `bytes` hold, `bytes.len()` being this type's width. A `u64` past `i64::MAX` is an `Overflow`.
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
            NumType::Signed(w) => {
                let shift = 64 - 8 * w as u32;
                Ok(((raw << shift) as i64) >> shift)
            }
            _ => i64::try_from(raw).map_err(|_| ExceptionKind::Overflow),
        }
    }

    fn float(self, bytes: &[u8], order: Order) -> f64 {
        match (bytes.try_into(), bytes.try_into(), order) {
            (Ok(b), _, Order::Big) => f32::from_be_bytes(b) as f64,
            (Ok(b), _, Order::Little) => f32::from_le_bytes(b) as f64,
            (_, Ok(b), Order::Big) => f64::from_be_bytes(b),
            (_, Ok(b), Order::Little) => f64::from_le_bytes(b),
            _ => unreachable!("a float is 4 or 8 bytes"),
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
/// `slots` opens. A type or an order the call does not name is a `CoercionRefused`, and so is a float type to `readInt`
/// or an integer one to `readFloat`.
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
/// infinity, which no `f64` value holds, is a `NaN`.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_integer_reads_in_either_order_and_sign_extends() {
        let b = [0xff, 0xfe];
        assert_eq!(NumType::Unsigned(2).int(&b, Order::Big), Ok(0xfffe));
        assert_eq!(NumType::Unsigned(2).int(&b, Order::Little), Ok(0xfeff));
        assert_eq!(NumType::Signed(2).int(&b, Order::Big), Ok(-2));
        assert_eq!(NumType::Signed(1).int(&[0x80], Order::Big), Ok(-128));
        assert_eq!(NumType::Signed(8).int(&[0xff; 8], Order::Little), Ok(-1));
    }

    #[test]
    fn a_u64_past_i64_overflows() {
        assert_eq!(
            NumType::Unsigned(8).int(&[0xff; 8], Order::Big),
            Err(ExceptionKind::Overflow)
        );
        assert_eq!(
            NumType::Unsigned(8).int(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], Order::Big),
            Ok(i64::MAX)
        );
    }

    #[test]
    fn a_float_reads_in_either_order() {
        assert_eq!(NumType::Float(4).float(&1.5f32.to_be_bytes(), Order::Big), 1.5);
        assert_eq!(NumType::Float(8).float(&(-2.25f64).to_le_bytes(), Order::Little), -2.25);
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
