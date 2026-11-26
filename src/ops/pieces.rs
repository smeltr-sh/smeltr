//! Build a column from pieces of other columns: runs of rows, single rows and nulls, in order.

use crate::core::bits;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType, Fixed, FixedMut, StrBuilder, Validity, VarType};
use crate::core::ctx::Ctx;

/// One piece of the elements being built: a run of rows of a column, one row of a column, or nulls.
#[derive(Clone, Copy)]
pub(crate) enum Piece<'a> {
    /// `column[start..end]`, an existing column's rows.
    Span(&'a Column, usize, usize),
    /// `column[row]`, one row.
    One(&'a Column, usize),
    /// This many null elements: a row, or a field, with nothing to read.
    Null(usize),
}

impl<'a> Piece<'a> {
    /// The column the piece reads and the rows it reads there. A null reads no column.
    fn source(self) -> (Option<&'a Column>, std::ops::Range<usize>) {
        match self {
            Piece::Span(c, s, e) => (Some(c), s..e),
            Piece::One(c, i) => (Some(c), i..i + 1),
            Piece::Null(n) => (None, 0..n),
        }
    }

    /// The same elements' field `name`, null where the column has none.
    fn field(self, name: &str) -> Piece<'a> {
        self.part(|c| c.field(name))
    }

    /// The same elements' unclaimed fields, null where the column has none.
    fn unclaimed_fields(self) -> Piece<'a> {
        self.part(Column::unclaimed_fields)
    }

    fn part(self, of: impl Fn(&'a Column) -> Option<&'a Column>) -> Piece<'a> {
        match self {
            Piece::Span(c, s, e) => of(c).map_or(Piece::Null(e - s), |f| Piece::Span(f, s, e)),
            Piece::One(c, i) => of(c).map_or(Piece::Null(1), |f| Piece::One(f, i)),
            Piece::Null(n) => Piece::Null(n),
        }
    }
}

/// `build` with the pieces given per row: row `r`'s are what `pieces(r)` emits.
///
/// One callback rather than "spans, then singles", because `insert` puts its values *between* two spans of the array
/// it is inserting into. `concat` emits only spans, `push` a span then its values, `insert` a span, its values, and
/// the rest of the span.
pub(crate) fn build_from_pieces<'a>(
    ctx: &mut Ctx,
    elem_ty: &DataType,
    rows: usize,
    total: usize,
    pieces: &dyn Fn(usize, &mut Vec<Piece<'a>>),
) -> Column {
    let mut flat = Vec::with_capacity(2 * rows);
    for row in 0..rows {
        pieces(row, &mut flat);
    }
    build(ctx.pool, elem_ty, &flat, total)
}

/// Build a column of `total` elements of `ty`: the elements of `pieces`, in order. A piece whose column does not hold
/// `ty` -- an unbound one, the null a literal gave -- is null elements. A source is never a constant: every caller
/// hands the builder rows. An element that is an array or a struct is built by the same walk one level down, so the
/// column nests as deep as its type. A `null` type holds no value: its column is `Empty`.
pub(crate) fn build<'a>(pool: &mut BufferPool, ty: &DataType, pieces: &[Piece<'a>], total: usize) -> Column {
    match ty {
        DataType::Null => Column::empty(DataType::Null, total),
        DataType::Array(inner) => build_list_elements(pool, inner, pieces, total),
        // A struct is its fields, each built from the same pieces. It has no validity of its own, so a null element
        // is null in every leaf.
        DataType::Struct(fields, _) => {
            let parts = fields
                .iter()
                .map(|(name, field_ty)| {
                    let field: Vec<Piece<'a>> = pieces.iter().map(|p| p.field(name)).collect();
                    (name.clone(), build(pool, field_ty, &field, total))
                })
                .collect();
            // The elements' unclaimed fields, where any element has some.
            let any_unclaimed_fields = pieces
                .iter()
                .any(|p| p.source().0.is_some_and(|c| c.unclaimed_fields().is_some()));
            let unclaimed_fields = any_unclaimed_fields.then(|| {
                let part: Vec<Piece<'a>> = pieces.iter().map(|p| p.unclaimed_fields()).collect();
                build(pool, &DataType::Variant, &part, total)
            });
            Column::struct_with_unclaimed_fields(parts, unclaimed_fields, total)
        }
        DataType::Str | DataType::Bytes | DataType::Variant => {
            let vt = match ty {
                DataType::Bytes => VarType::Bytes,
                DataType::Variant => VarType::Variant,
                _ => VarType::Str,
            };
            build_var(pool, vt, pieces, total)
        }
        DataType::Bool => build_bool(pool, pieces, total),
        DataType::I64 => build_fixed(pool, ty, pieces, total, |c| c.i64().ok(), |c| c.i64_mut().unwrap()),
        DataType::DateTime => build_fixed(
            pool,
            ty,
            pieces,
            total,
            |c| c.datetime().ok(),
            |c| c.datetime_mut().unwrap(),
        ),
        DataType::Duration => build_fixed(
            pool,
            ty,
            pieces,
            total,
            |c| c.duration().ok(),
            |c| c.duration_mut().unwrap(),
        ),
        DataType::F64 => build_fixed(pool, ty, pieces, total, |c| c.f64().ok(), |c| c.f64_mut().unwrap()),
    }
}

/// A fixed-width source as the builder reads it: rows `at` copied into `values` and the validity bits `valid` from
/// `pos`. The bits are written raw; the caller recounts once.
trait Read<T> {
    fn copy(&self, at: std::ops::Range<usize>, values: &mut [T], valid: &mut [u64], pos: usize);
}

impl<T: Copy> Read<T> for Fixed<'_, T> {
    // Always inlined: `build_fixed` calls it once per piece, and a piece is often one element.
    #[inline(always)]
    fn copy(&self, at: std::ops::Range<usize>, values: &mut [T], valid: &mut [u64], pos: usize) {
        for (k, i) in at.enumerate() {
            values[pos + k] = self.values[i];
            bits::set_row(valid, pos + k, self.validity.is_valid(i));
        }
    }
}

/// Each distinct source column's reader, made once: a ternary or a `??` moves between a few columns row by row.
struct Readers<'a, R> {
    seen: Vec<(&'a Column, Option<R>)>,
    last: usize,
}

impl<'a, R> Readers<'a, R> {
    fn new() -> Self {
        Readers {
            seen: Vec::new(),
            last: 0,
        }
    }

    #[inline]
    fn of(&mut self, c: &'a Column, read: impl Fn(&'a Column) -> Option<R>) -> Option<&R> {
        if !self.seen.get(self.last).is_some_and(|(p, _)| std::ptr::eq(*p, c)) {
            self.last = match self.seen.iter().position(|(p, _)| std::ptr::eq(*p, c)) {
                Some(k) => k,
                None => {
                    self.seen.push((c, read(c)));
                    self.seen.len() - 1
                }
            };
        }
        self.seen[self.last].1.as_ref()
    }
}

/// `build` for elements that are arrays: each element is its source's own run of that source's child, and those
/// runs, in element order, are the child one level down.
fn build_list_elements<'a>(pool: &mut BufferPool, inner: &DataType, pieces: &[Piece<'a>], total: usize) -> Column {
    let mut offsets = pool.take((total + 1) * 4);
    let mut validity = Validity::alloc(pool, total);
    let mut child: Vec<Piece<'a>> = Vec::new();
    let mut inner_total = 0usize;
    {
        let o = offsets.as_mut_slice::<u32>();
        o[0] = 0;
        let mut e = 0usize;
        for piece in pieces {
            let (src, at) = piece.source();
            let list = src.and_then(|c| c.list().ok());
            for i in at {
                match list.as_ref().filter(|l| l.validity.is_valid(i)) {
                    Some(l) => {
                        let (s, t) = (l.offsets[i] as usize, l.offsets[i + 1] as usize);
                        match child.last_mut() {
                            Some(Piece::Span(c, _, end)) if *end == s && std::ptr::eq(*c, l.child) => *end = t,
                            _ => child.push(Piece::Span(l.child, s, t)),
                        }
                        inner_total += t - s;
                    }
                    None => validity.set_valid(e, false),
                }
                e += 1;
                o[e] = inner_total as u32;
            }
        }
        debug_assert_eq!(e, total, "the pieces emit the elements the caller counted");
    }
    let child = build(pool, inner, &child, inner_total);
    Column::list_from_parts(offsets, child, validity, total)
}

fn build_fixed<'a, T: Copy + 'a, R: Read<T>>(
    pool: &mut BufferPool,
    ty: &DataType,
    pieces: &[Piece<'a>],
    total: usize,
    read: impl Fn(&'a Column) -> Option<R>,
    get_mut: impl Fn(&mut Column) -> FixedMut<'_, T>,
) -> Column {
    let mut out = Column::alloc(pool, ty, total);
    {
        let o = get_mut(&mut out);
        let valid = o.validity.bits_mut();
        let mut pos = 0usize;
        for piece in pieces {
            let (src, at) = piece.source();
            let n = at.len();
            match src.and_then(&read) {
                Some(r) => r.copy(at, o.values, valid, pos),
                None => {
                    for k in pos..pos + n {
                        bits::set_row(valid, k, false);
                    }
                }
            }
            pos += n;
        }
        o.validity.recount();
    }
    out
}

fn build_bool<'a>(pool: &mut BufferPool, pieces: &[Piece<'a>], total: usize) -> Column {
    let mut out = Column::alloc(pool, &DataType::Bool, total);
    {
        let o = out.bool_mut().unwrap();
        let mut pos = 0usize;
        for piece in pieces {
            let (src, at) = piece.source();
            match src.and_then(|c| c.bool().ok()) {
                Some(b) => {
                    for i in at {
                        let valid = b.validity.is_valid(i);
                        bits::set_row(o.values, pos, valid && bits::get_row(b.values, i));
                        o.validity.set_valid(pos, valid);
                        pos += 1;
                    }
                }
                None => {
                    for _ in at {
                        bits::set_row(o.values, pos, false);
                        o.validity.set_valid(pos, false);
                        pos += 1;
                    }
                }
            }
        }
    }
    out
}

fn build_var<'a>(pool: &mut BufferPool, var_type: VarType, pieces: &[Piece<'a>], total: usize) -> Column {
    let mut b = StrBuilder::new(pool, var_type, total);
    let mut readers = Readers::new();
    let mut total_bytes = 0usize;
    for piece in pieces {
        let (src, at) = piece.source();
        if let Some(v) = src.and_then(|c| readers.of(c, |c| c.sealed_rows().ok())) {
            total_bytes += (v.rows.offsets[at.end] - v.rows.offsets[at.start]) as usize;
        }
    }
    b.reserve(pool, total_bytes);
    for piece in pieces {
        let (src, at) = piece.source();
        let v = src.and_then(|c| readers.of(c, |c| c.sealed_rows().ok()));
        for i in at {
            match v {
                Some(v) if v.rows.validity.is_valid(i) => b.push_row(v, i),
                _ => b.push_null(),
            }
        }
    }
    b.finish()
}
