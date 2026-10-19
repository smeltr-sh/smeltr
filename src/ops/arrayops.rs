//! Array kernels: `length`, `split` (by string or regex), `match`, `push`, the JS `Array` surface (`slice`, `at`,
//! `includes`, `indexOf`, `join`, `concat`), and the `Array` arm of `ops::logic::take`.
//!
//! `push` reads a null array as zero elements, so a guarded `x = push(x, v)` works on the first assignment.

use super::helpers::{Operand, i64_values, short_circuit_empty, side};
use crate::core::bits;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType, Fixed, FixedMut, List, Scalar, StrBuilder, Validity, VarLen, VarType};
use crate::core::ctx::{CompiledPattern, Ctx};

pub fn array_length(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    let l = ins[0].list().unwrap();
    let mut out = Column::alloc(ctx.pool, &DataType::I64, len);
    {
        let o = out.i64_mut().unwrap();
        for i in 0..len {
            o.values[i] = (l.offsets[i + 1] - l.offsets[i]) as i64;
        }
        o.validity.bits_mut().copy_from_slice(l.validity.bits());
        o.validity.recount();
    }
    out
}

/// `split(value, delimiter)` -> `Array(Str)`. An empty delimiter splits into characters, as in JS. Null propagates.
pub fn str_split(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let out_ty = DataType::Array(Box::new(DataType::Str));
    if let Some(e) = short_circuit_empty(ins, &out_ty, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let d = side::var(ins[1]).unwrap();
    let mut rows = ListRows::new(ctx, &v, len);
    for i in 0..len {
        if !v.validity.is_valid(i) || !d.is_valid(i) {
            rows.null_row();
            continue;
        }
        let text = row_text(v.get(i));
        let delim = row_text(d.at(i));
        if delim.is_empty() {
            rows.row(text, |t| {
                t.char_indices().map(|(at, c)| Some(&t[at..at + c.len_utf8()]))
            });
        } else {
            rows.row(text, |t| t.split(delim).map(Some));
        }
    }
    rows.finish(ctx)
}

/// `split(value, /pattern/)`: a piece between every match.
pub fn regex_split(ctx: &mut Ctx, value: &Column, pattern: &CompiledPattern) -> Column {
    let len = value.len();
    let Ok(v) = value.var() else {
        return Column::empty(DataType::Array(Box::new(DataType::Str)), len);
    };
    let mut rows = ListRows::new(ctx, &v, len);
    for i in 0..len {
        if !v.validity.is_valid(i) {
            rows.null_row();
            continue;
        }
        let text = row_text(v.get(i));
        rows.row(text, |t| pattern.regex.split(t).map(Some));
    }
    rows.finish(ctx)
}

/// `match(value, /pattern/)`: JS's -- the whole match at `[0]` and each group after it, an unmatched group a null
/// element, and a null row when nothing matches.
pub fn regex_match(ctx: &mut Ctx, value: &Column, pattern: &CompiledPattern) -> Column {
    let len = value.len();
    let Ok(v) = value.var() else {
        return Column::empty(DataType::Array(Box::new(DataType::Str)), len);
    };
    // One set of capture slots for the batch, not one per row.
    let mut locs = pattern.regex.capture_locations();
    let mut rows = ListRows::new(ctx, &v, len);
    for i in 0..len {
        if !v.validity.is_valid(i) {
            rows.null_row();
            continue;
        }
        let text = row_text(v.get(i));
        if pattern.regex.captures_read(&mut locs, text).is_none() {
            rows.null_row();
            continue;
        }
        rows.row(text, |t| (0..locs.len()).map(|g| locs.get(g).map(|(s, e)| &t[s..e])));
    }
    rows.finish(ctx)
}

/// A `Str` row as the text it is.
fn row_text(bytes: &[u8]) -> &str {
    // SAFETY: a `Str` column holds valid UTF-8 by the columnar model's invariant.
    unsafe { std::str::from_utf8_unchecked(bytes) }
}

/// Builds an `Array(Str)` column one row at a time from pieces of the input column's rows. The piece count is only
/// known at the end, and `StrBuilder` needs it up front, so a piece is held as a span until `finish`, when each is
/// copied once into the child.
struct ListRows<'a> {
    data: &'a [u8],
    validity: Validity,
    offsets: Vec<u32>,
    parts: Vec<Span>,
    len: usize,
}

#[derive(Clone, Copy)]
enum Span {
    Null,
    Data(u32, u32),
}

impl<'a> ListRows<'a> {
    fn new(ctx: &mut Ctx, input: &VarLen<'a>, len: usize) -> ListRows<'a> {
        ListRows {
            data: input.data,
            validity: Validity::alloc_null(ctx.pool, len),
            offsets: Vec::with_capacity(len + 1),
            parts: Vec::with_capacity(len),
            len,
        }
    }

    /// The next row: `pieces(text)`, each a slice of `text`, a row of the input column, or a null element.
    fn row<'t, I>(&mut self, text: &'t str, pieces: impl FnOnce(&'t str) -> I)
    where
        I: Iterator<Item = Option<&'t str>>,
    {
        let i = self.offsets.len();
        if i == 0 {
            self.offsets.push(0);
        }
        self.validity.set_valid(i - usize::from(i > 0), true);
        let base = self.data.as_ptr() as usize;
        let end = base + self.data.len();
        for piece in pieces(text) {
            self.parts.push(match piece {
                Some(p) => {
                    let start = p.as_ptr() as usize - base;
                    debug_assert!(start + p.len() <= end - base, "a piece of the input column's row");
                    Span::Data(start as u32, (start + p.len()) as u32)
                }
                None => Span::Null,
            });
        }
        debug_assert!(
            self.parts.len() <= u32::MAX as usize,
            "more pieces than an offset addresses"
        );
        self.offsets.push(self.parts.len() as u32);
    }

    fn null_row(&mut self) {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        self.offsets.push(self.parts.len() as u32);
    }

    fn finish(mut self, ctx: &mut Ctx) -> Column {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        debug_assert_eq!(self.offsets.len(), self.len + 1);
        let total_bytes: usize = self
            .parts
            .iter()
            .map(|p| match *p {
                Span::Null => 0,
                Span::Data(s, e) => (e - s) as usize,
            })
            .sum();
        let mut b = StrBuilder::new(ctx.pool, VarType::Str, self.parts.len());
        b.reserve(ctx.pool, total_bytes);
        for p in &self.parts {
            match *p {
                Span::Null => b.push_null(),
                Span::Data(s, e) => b.push(&self.data[s as usize..e as usize]),
            }
        }
        let child = b.finish();
        let mut offsets_buf = ctx.pool.take((self.len + 1) * 4);
        offsets_buf.as_mut_slice::<u32>().copy_from_slice(&self.offsets);
        Column::list_from_parts(offsets_buf, child, self.validity, self.len)
    }
}

/// `row`'s element span in `list`; `None` for an absent array or a null row.
fn span_of(list: &Option<List>, row: usize) -> Option<(usize, usize)> {
    match list {
        Some(l) if l.validity.is_valid(row) => Some((l.offsets[row] as usize, l.offsets[row + 1] as usize)),
        _ => None,
    }
}

/// `push(array, value, ..)` -> `Array(T)`, and `insert(array, index, value, ..)`, which is the same write with the
/// split somewhere other than the end. A null value is a position, holding nothing.
///
/// `split(row, len)` is where the values go: `Some(i)` to put them before element `i`, `None` to null the row. Every
/// row is one pass and the whole column one allocation -- the reason `push` takes every value at once rather than
/// leaving an author to loop, which reallocates and copies the array on each turn.
fn insert_at<'a>(
    ctx: &mut Ctx,
    array: &'a Column,
    values: &[&'a Column],
    split: impl Fn(usize, usize) -> Option<usize>,
) -> Column {
    let len = array.len();
    let old = array.list().ok();
    // The builder is chosen by element type, as `stdfuncs::Ret::Pushed` decides it at compile time: the array's own
    // element type (when it is a real list) meets every value's. An unbound value (a literal `null`, or the unbound
    // array in a guarded `x = push(x, v)`) never enters the meet: skipped on binding, not on validity, since a value
    // column with no valid row in this batch is still bound and still has its say.
    let elem_ty = {
        let declared = match array.data_type() {
            DataType::Array(elem) => Some(*elem),
            _ => None,
        };
        // An unbound array holds no value to have a say beside a bound one, and stands only where no input carries
        // one. `[]`'s element type is `null`, which takes the values' type.
        let mut ty = if array.is_unbound() { None } else { declared.clone() };
        for v in values {
            if !v.is_unbound() {
                let vt = v.data_type();
                ty = Some(match &ty {
                    Some(t) => t.unify(&vt).unwrap_or_else(|| t.clone()),
                    None => vt,
                });
            }
        }
        ty.or(declared).unwrap_or(DataType::Null)
    };

    let mut validity = Validity::alloc(ctx.pool, len);
    let mut offsets = vec![0u32; len + 1];
    let mut cuts: Vec<Option<usize>> = Vec::with_capacity(len);
    let mut total = 0usize;
    for row in 0..len {
        let (base, n) = span_of(&old, row).map_or((0, 0), |(s, e)| (s, e - s));
        match split(row, n) {
            Some(at) => {
                cuts.push(Some(base + at));
                total += n + values.len();
            }
            None => {
                cuts.push(None);
                validity.set_valid(row, false);
            }
        }
        offsets[row + 1] = total as u32;
    }

    let old_child = old.as_ref().map(|l| l.child);
    let pieces = |row: usize, out: &mut Vec<Piece<'a>>| {
        let Some(cut) = cuts[row] else { return };
        let (base, end) = span_of(&old, row).unwrap_or((0, 0));
        if let Some(child) = old_child {
            out.push(Piece::Span(child, base, cut));
        }
        out.extend(values.iter().map(|v| Piece::One(v, row)));
        if let Some(child) = old_child {
            out.push(Piece::Span(child, cut, end));
        }
    };
    let child = build_from_pieces(ctx, &elem_ty, len, total, &pieces);

    let mut offsets_buf = ctx.pool.take((len + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
    Column::list_from_parts(offsets_buf, child, validity, len)
}

/// `push(array, value, ..)`: every value at the end. Every output row is a valid array, so a null array is an empty
/// one rather than a null result.
pub fn array_push(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    insert_at(ctx, ins[0], &ins[1..], |_, n| Some(n))
}

/// `insert(array, index, value, ..)`: every value before `index`, which counts from the end when negative and clamps
/// to the array at either end. A null index has no position to mean, so that row is null.
///
/// `-1` is the last element, as it is for `at` and `slice`, so `insert(xs, -1, v)` puts `v` *before* the last element.
/// Appending is `push`, which is this with the split at the end -- no negative index names the gap past it.
pub fn array_insert(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    // An index column with nothing in it is null on every row, so no row has a position.
    let Some(idx) = side::i64(ins[1]) else {
        return insert_at(ctx, ins[0], &ins[2..], |_, _| None);
    };
    let at = |row: usize, n: usize| idx.is_valid(row).then(|| crate::ops::helpers::clamped(idx.at(row), n));
    insert_at(ctx, ins[0], &ins[2..], at)
}

/// `remove(array, index)`: the array without element `index`, which counts from the end when negative, as it does for
/// `at` and `insert`. An index outside the array removes nothing. A null array or index is a null row.
pub fn array_remove(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let out_ty = ins[0].data_type();
    if let Some(e) = short_circuit_empty(ins, &out_ty, len) {
        return e;
    }
    let (Ok(list), DataType::Array(elem_ty)) = (ins[0].list(), out_ty.clone()) else {
        return Column::empty(out_ty, len);
    };
    let idx = side::i64(ins[1]).unwrap();
    let mut validity = Validity::alloc_null(ctx.pool, len);
    // Per row: the array's span, and the element it drops there.
    let mut kept: Vec<Option<(usize, usize, Option<usize>)>> = Vec::with_capacity(len);
    let mut offsets = vec![0u32; len + 1];
    for row in 0..len {
        if !list.validity.is_valid(row) || !idx.is_valid(row) {
            kept.push(None);
            offsets[row + 1] = offsets[row];
            continue;
        }
        let (base, end) = (list.offsets[row] as usize, list.offsets[row + 1] as usize);
        let cut = crate::ops::helpers::position(idx.at(row), end - base).map(|i| base + i);
        validity.set_valid(row, true);
        kept.push(Some((base, end, cut)));
        offsets[row + 1] = offsets[row] + (end - base - usize::from(cut.is_some())) as u32;
    }
    let total = offsets[len] as usize;
    let child = list.child;
    let built = build_from_pieces(
        ctx,
        &elem_ty,
        len,
        total,
        &|row, out: &mut Vec<Piece<'_>>| match kept[row] {
            Some((base, end, Some(cut))) => {
                out.push(Piece::Span(child, base, cut));
                out.push(Piece::Span(child, cut + 1, end));
            }
            Some((base, end, None)) => out.push(Piece::Span(child, base, end)),
            None => {}
        },
    );
    let mut offsets_buf = ctx.pool.take((len + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
    Column::list_from_parts(offsets_buf, built, validity, len)
}

/// `array` with position `index` set to `value`, per row: what `a[i] = x` writes. A negative index counts from the
/// end. A null value clears the position, which stays. An index outside the array writes nothing, and the row carries
/// an `IndexOutOfRange` warning; a null index writes nothing. A null array stays null. `ty` is the result's array type.
pub fn array_set(ctx: &mut Ctx, array: &Column, index: &Column, value: &Column, ty: &DataType) -> Column {
    let len = array.len();
    let Ok(list) = array.list() else {
        return Column::empty(ty.clone(), len);
    };
    let idx = side::i64(index);
    let index_valid = |row: usize| idx.as_ref().is_some_and(|i| i.is_valid(row));
    // The element type is the array's own met with the value's, as a push meets them: an unbound value has no say.
    let elem_ty = {
        let own = match array.data_type() {
            DataType::Array(e) => *e,
            _ => DataType::Null,
        };
        if value.is_unbound() {
            own
        } else {
            own.unify(&value.data_type()).unwrap_or(own)
        }
    };
    let cuts: Vec<Option<usize>> = (0..len)
        .map(|row| {
            let (base, end) = (list.offsets[row] as usize, list.offsets[row + 1] as usize);
            if !list.validity.is_valid(row) || !index_valid(row) {
                return None;
            }
            crate::ops::helpers::position(idx.as_ref().expect("valid").at(row), end - base).map(|i| base + i)
        })
        .collect();
    raise_outside(ctx, list.validity, index_valid, |row| cuts[row].is_none(), len);
    // A null row holds no elements, whatever span its offsets leave.
    let mut offsets = vec![0u32; len + 1];
    for row in 0..len {
        let n = if list.validity.is_valid(row) {
            list.offsets[row + 1] - list.offsets[row]
        } else {
            0
        };
        offsets[row + 1] = offsets[row] + n;
    }
    let child = list.child;
    let total = offsets[len] as usize;
    let built = build_from_pieces(ctx, &elem_ty, len, total, &|row, out: &mut Vec<Piece<'_>>| {
        let (base, end) = (list.offsets[row] as usize, list.offsets[row + 1] as usize);
        match cuts[row] {
            Some(cut) => {
                out.push(Piece::Span(child, base, cut));
                out.push(if value.is_valid(row) {
                    Piece::One(value, row)
                } else {
                    Piece::Null(1)
                });
                out.push(Piece::Span(child, cut + 1, end));
            }
            None if list.validity.is_valid(row) => out.push(Piece::Span(child, base, end)),
            None => {}
        }
    });
    let mut offsets_buf = ctx.pool.take((len + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
    let validity = list.validity.deep_copy(ctx.pool);
    Column::list_from_parts(offsets_buf, built, validity, len)
}

/// `resize(array, size)`: the array with exactly `size` positions -- its first `size` where it has more, and after its
/// own, positions that hold nothing where it has fewer. A negative size names no array, so that row is null, as is a
/// null array or size. More positions than a column can count is refused as an allocation over the memory budget.
pub fn array_resize(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let out_ty = ins[0].data_type();
    if let Some(e) = short_circuit_empty(ins, &out_ty, len) {
        return e;
    }
    let (Ok(list), DataType::Array(elem_ty)) = (ins[0].list(), out_ty.clone()) else {
        return Column::empty(out_ty, len);
    };
    let sizes = side::i64(ins[1]).unwrap();
    let mut validity = Validity::alloc_null(ctx.pool, len);
    // Per row: the run of the array it keeps, and how many positions that hold nothing follow it.
    let mut kept: Vec<Option<(usize, usize, usize)>> = Vec::with_capacity(len);
    let mut offsets = vec![0u32; len + 1];
    let mut total = 0u64;
    for row in 0..len {
        let size = sizes.at(row);
        if !list.validity.is_valid(row) || !sizes.is_valid(row) || size < 0 {
            kept.push(None);
            offsets[row + 1] = total as u32;
            continue;
        }
        let (base, end) = (list.offsets[row] as usize, list.offsets[row + 1] as usize);
        let size = size as u64;
        let keep = size.min((end - base) as u64) as usize;
        total += size;
        if total > u64::from(u32::MAX) {
            ctx.pool
                .refuse(usize::try_from(total.saturating_mul(4)).unwrap_or(usize::MAX));
        }
        validity.set_valid(row, true);
        kept.push(Some((base, base + keep, (size - keep as u64) as usize)));
        offsets[row + 1] = total as u32;
    }
    let child = list.child;
    let built = build_from_pieces(ctx, &elem_ty, len, total as usize, &|row, out: &mut Vec<Piece<'_>>| {
        if let Some((start, end, nothing)) = kept[row] {
            out.push(Piece::Span(child, start, end));
            if nothing > 0 {
                out.push(Piece::Null(nothing));
            }
        }
    });
    let mut offsets_buf = ctx.pool.take((len + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
    Column::list_from_parts(offsets_buf, built, validity, len)
}

/// `take` over arrays: row `r` is the whole array of the one source whose `takes` holds `r`, and null where none does
/// or where that source's own row is null. `takes` says which rows a source won; its list says what it holds there.
pub fn take_list<'a>(
    ctx: &mut Ctx,
    sources: &[&'a Column],
    takes: &[&[u64]],
    elem_ty: &DataType,
    rows: usize,
) -> Column {
    let lists: Vec<Option<List<'a>>> = sources.iter().map(|c| c.list().ok()).collect();
    let taker = bits::index_of_set(takes, rows);
    let source = |row: usize| taker[row].map(|k| &lists[k as usize]);

    let mut offsets = vec![0u32; rows + 1];
    let mut validity = Validity::alloc_null(ctx.pool, rows);
    let mut total = 0usize;
    for row in 0..rows {
        if let Some((s, e)) = source(row).and_then(|l| span_of(l, row)) {
            total += e - s;
            validity.set_valid(row, true);
        }
        offsets[row + 1] = total as u32;
    }

    let pieces = |row: usize, out: &mut Vec<Piece<'a>>| {
        if let Some(l) = source(row)
            && let Some((s, e)) = span_of(l, row)
        {
            out.push(Piece::Span(l.as_ref().unwrap().child, s, e));
        }
    };
    let child = build_from_pieces(ctx, elem_ty, rows, total, &pieces);
    let mut offsets_buf = ctx.pool.take((rows + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
    Column::list_from_parts(offsets_buf, child, validity, rows)
}

/// One piece of the elements being built: a run of rows of a column, one row of a column, or nulls.
#[derive(Clone, Copy)]
pub enum Piece<'a> {
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
fn build<'a>(pool: &mut BufferPool, ty: &DataType, pieces: &[Piece<'a>], total: usize) -> Column {
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
        if let Some(v) = src.and_then(|c| readers.of(c, |c| c.varlen().ok())) {
            total_bytes += (v.offsets[at.end] - v.offsets[at.start]) as usize;
        }
    }
    b.reserve(pool, total_bytes);
    for piece in pieces {
        let (src, at) = piece.source();
        let v = src.and_then(|c| readers.of(c, |c| c.varlen().ok()));
        for i in at {
            match v {
                Some(v) if v.validity.is_valid(i) => b.push(v.get(i)),
                _ => b.push_null(),
            }
        }
    }
    b.finish()
}

// -- the JS Array surface -----------------------------------------------------

/// `slice(array, start[, end])`: JS `Array.slice`, a negative index counting from the end, both clamped.
pub fn array_slice(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let out_ty = ins[0].data_type();
    if let Some(e) = short_circuit_empty(ins, &out_ty, len) {
        return e;
    }
    let (Ok(list), DataType::Array(elem_ty)) = (ins[0].list(), out_ty.clone()) else {
        return Column::empty(out_ty, len);
    };
    let starts = side::i64(ins[1]).unwrap();
    let ends = ins.get(2).and_then(|c| side::i64(c));
    let mut validity = Validity::alloc_null(ctx.pool, len);
    let mut ranges: Vec<Option<(usize, usize)>> = Vec::with_capacity(len);
    let mut offsets = vec![0u32; len + 1];
    for row in 0..len {
        let end_ok = ins.len() < 3 || ends.as_ref().is_some_and(|e| e.is_valid(row));
        if !list.validity.is_valid(row) || !starts.is_valid(row) || !end_ok {
            ranges.push(None);
            offsets[row + 1] = offsets[row];
            continue;
        }
        let base = list.offsets[row] as usize;
        let n = (list.offsets[row + 1] - list.offsets[row]) as usize;
        let s = crate::ops::helpers::clamped(starts.at(row), n);
        let e = ends.as_ref().map_or(n, |e| crate::ops::helpers::clamped(e.at(row), n));
        let (s, e) = if s < e { (base + s, base + e) } else { (base, base) };
        validity.set_valid(row, true);
        ranges.push(Some((s, e)));
        offsets[row + 1] = offsets[row] + (e - s) as u32;
    }
    let total = offsets[len] as usize;
    let child = list.child;
    let built = build_from_pieces(ctx, &elem_ty, len, total, &|row, out: &mut Vec<Piece<'_>>| {
        out.extend(ranges[row].map(|(s, e)| Piece::Span(child, s, e)))
    });
    let mut offsets_buf = ctx.pool.take((len + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
    Column::list_from_parts(offsets_buf, built, validity, len)
}

/// `concat(a, b)`: one array after the other. A null on either side is a null row.
pub fn array_concat<'a>(ctx: &mut Ctx, ins: &[&'a Column]) -> Column {
    let len = ins[0].len();
    // Every input's elements meet as one type: an `I64` array beside an `F64` one as `F64`, `[]` beside any as that.
    let out_ty = ins
        .iter()
        .map(|c| c.data_type())
        .reduce(|a, b| a.unify(&b).unwrap_or(a))
        .expect("concat takes an array");
    let DataType::Array(elem_ty) = out_ty.clone() else {
        return Column::empty(out_ty, len);
    };
    let Ok(lists) = ins.iter().map(|c| c.list()).collect::<Result<Vec<_>, _>>() else {
        return Column::empty(out_ty, len);
    };

    // A row is the concatenation of every input's span, or null if any input is null -- so the total is known before
    // a byte is written, whatever the number of inputs.
    let mut validity = Validity::alloc_null(ctx.pool, len);
    let mut offsets = vec![0u32; len + 1];
    for row in 0..len {
        let all = lists.iter().all(|l| l.validity.is_valid(row));
        let n = if all {
            lists.iter().map(|l| l.offsets[row + 1] - l.offsets[row]).sum()
        } else {
            0
        };
        if all {
            validity.set_valid(row, true);
        }
        offsets[row + 1] = offsets[row] + n;
    }
    let total = offsets[len] as usize;
    let spans = |row: usize, out: &mut Vec<Piece<'a>>| {
        if lists.iter().all(|l| l.validity.is_valid(row)) {
            out.extend(
                lists
                    .iter()
                    .map(|l| Piece::Span(l.child, l.offsets[row] as usize, l.offsets[row + 1] as usize)),
            );
        }
    };
    let built = build_from_pieces(ctx, &elem_ty, len, total, &spans);
    let mut offsets_buf = ctx.pool.take((len + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
    Column::list_from_parts(offsets_buf, built, validity, len)
}

/// `at(array, index)`: the element, a negative index counting from the end; out of range is null.
pub fn array_at(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let elem_ty = match ins[0].data_type() {
        DataType::Array(e) => *e,
        other => return Column::empty(other, len),
    };
    if let Some(e) = short_circuit_empty(ins, &elem_ty, len) {
        return e;
    }
    let Ok(list) = ins[0].list() else {
        return Column::empty(elem_ty, len);
    };
    let idx = side::i64(ins[1]).unwrap();
    let targets: Vec<Option<u32>> = (0..len)
        .map(|row| {
            if !list.validity.is_valid(row) || !idx.is_valid(row) {
                return None;
            }
            let n = (list.offsets[row + 1] - list.offsets[row]) as usize;
            crate::ops::helpers::position(idx.at(row), n).map(|i| list.offsets[row] + i as u32)
        })
        .collect();
    raise_outside(
        ctx,
        list.validity,
        |row| idx.is_valid(row),
        |row| targets[row].is_none(),
        len,
    );
    if list.child.is_empty() || targets.iter().all(Option::is_none) {
        return Column::empty(elem_ty, len);
    }
    let indices: Vec<u32> = targets.iter().map(|t| t.unwrap_or(0)).collect();
    let missing: Vec<bool> = targets.iter().map(Option::is_none).collect();
    gather_or_null(list.child, &indices, &missing, ctx)
}

/// An `IndexOutOfRange` warning on each of `rows` whose array and index hold a value and whose index names no position.
pub(crate) fn raise_outside(
    ctx: &mut Ctx,
    arrays: &Validity,
    index_valid: impl Fn(usize) -> bool,
    outside: impl Fn(usize) -> bool,
    rows: usize,
) {
    for row in (0..rows).filter(|&r| arrays.is_valid(r) && index_valid(r) && outside(r)) {
        ctx.raise(row, crate::core::error::ExceptionKind::IndexOutOfRange);
    }
}

/// `child` gathered at `indices`, null on every row `missing` marks. A struct carries no validity of its own, so a
/// missing row is null in each of its leaves; a value of a type that carries none is nothing.
fn gather_or_null(child: &Column, indices: &[u32], missing: &[bool], ctx: &mut Ctx) -> Column {
    let rows = indices.len();
    if let DataType::Struct(fields, _) = child.data_type() {
        let parts = fields
            .iter()
            .map(|(name, _)| {
                let field = child.field(name).expect("listed from the struct's own type");
                (name.clone(), gather_or_null(field, indices, missing, ctx))
            })
            .collect();
        let unclaimed_fields = child
            .unclaimed_fields()
            .map(|u| gather_or_null(u, indices, missing, ctx));
        return Column::struct_with_unclaimed_fields(parts, unclaimed_fields, rows);
    }
    let mut out = child.gather(ctx.pool, indices);
    let Some(v) = out.validity_mut() else {
        let ty = out.data_type();
        out.reclaim(ctx.pool);
        return Column::empty(ty, rows);
    };
    for (row, gone) in missing.iter().enumerate() {
        if *gone {
            v.set_valid(row, false);
        }
    }
    out
}

/// The value an element is compared against: one row of a column, or the one value a constant holds.
enum Elem<'a> {
    Text(&'a [u8]),
    Bool(bool),
    F64(f64),
    Int(i64),
}

/// Row `row` of `value`, or its one value. `None` for a null row, and for a type no element has.
fn elem_at(value: &Column, row: usize) -> Option<Elem<'_>> {
    if let Some(s) = value.constant_value() {
        return Some(match s {
            Scalar::Str(t) => Elem::Text(t.as_bytes()),
            Scalar::Bytes(b) => Elem::Text(b),
            Scalar::Bool(b) => Elem::Bool(*b),
            Scalar::F64(x) => Elem::F64(*x),
            Scalar::I64(x) | Scalar::DateTime(x) | Scalar::Duration(x) => Elem::Int(*x),
            Scalar::Variant(_) => return None,
        });
    }
    if !value.is_valid(row) {
        return None;
    }
    Some(match value.data_type() {
        DataType::Str | DataType::Bytes => Elem::Text(value.var().ok()?.get(row)),
        DataType::Bool => Elem::Bool(bits::get_row(value.bool().ok()?.values, row)),
        DataType::F64 => Elem::F64(value.f64().ok()?.values[row]),
        DataType::I64 | DataType::DateTime | DataType::Duration => Elem::Int(i64_values(value)?.0[row]),
        DataType::Null | DataType::Array(_) | DataType::Struct(..) | DataType::Variant => {
            return None;
        }
    })
}

/// The needle for `row`: `None` when the array row is null, or the needle has no value there.
fn needle_at<'a>(list: &List<'_>, needle: &'a Column, row: usize) -> Option<Elem<'a>> {
    list.validity.is_valid(row).then(|| elem_at(needle, row)).flatten()
}

/// Whether element `i` of `child` equals `value`; a null element is never equal. `F64` compares with `==`, so `NaN`
/// matches nothing, as JS `indexOf` (though not `includes`) has it.
fn element_eq(child: &Column, i: usize, value: &Elem<'_>) -> bool {
    if !child.is_valid(i) {
        return false;
    }
    match (child.data_type(), value) {
        (DataType::Str | DataType::Bytes, Elem::Text(v)) => child.var().is_ok_and(|c| c.get(i) == *v),
        (DataType::Bool, Elem::Bool(v)) => child.bool().is_ok_and(|c| bits::get_row(c.values, i) == *v),
        (DataType::F64, Elem::F64(v)) => child.f64().is_ok_and(|c| c.values[i] == *v),
        (DataType::I64 | DataType::DateTime | DataType::Duration, Elem::Int(v)) => {
            i64_values(child).is_some_and(|(vals, _)| vals[i] == *v)
        }
        // An `I64` array and an `F64` needle, or the reverse, meet by value.
        (DataType::F64, Elem::Int(v)) => child.f64().is_ok_and(|c| c.values[i] == *v as f64),
        (DataType::I64, Elem::F64(v)) => child.i64().is_ok_and(|c| c.values[i] as f64 == *v),
        _ => false,
    }
}

/// `sort(array)` and `sort(array, descending)`: the elements in order, text by its bytes, numbers by value and `false`
/// before `true`. Equal elements keep their order, and null elements go last either way. A null array or `descending`
/// is a null row.
pub fn array_sort(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let out_ty = ins[0].data_type();
    if let Some(e) = short_circuit_empty(ins, &out_ty, len) {
        return e;
    }
    let (Ok(list), DataType::Array(elem_ty)) = (ins[0].list(), out_ty.clone()) else {
        return Column::empty(out_ty, len);
    };
    let child = list.child;
    let mut order: Vec<Option<Vec<usize>>> = Vec::with_capacity(len);
    for row in 0..len {
        let descending = match ins.get(1) {
            Some(d) => match elem_at(d, row) {
                Some(Elem::Bool(b)) => b,
                _ => {
                    order.push(None);
                    continue;
                }
            },
            None => false,
        };
        if !list.validity.is_valid(row) {
            order.push(None);
            continue;
        }
        let mut idx: Vec<usize> = (list.offsets[row] as usize..list.offsets[row + 1] as usize).collect();
        idx.sort_by(|&a, &b| match (elem_at(child, a), elem_at(child, b)) {
            (Some(x), Some(y)) if descending => elem_cmp(&y, &x),
            (Some(x), Some(y)) => elem_cmp(&x, &y),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });
        order.push(Some(idx));
    }
    let mut validity = Validity::alloc_null(ctx.pool, len);
    let mut offsets = vec![0u32; len + 1];
    for (row, idx) in order.iter().enumerate() {
        let n = idx.as_ref().map_or(0, Vec::len);
        if idx.is_some() {
            validity.set_valid(row, true);
        }
        offsets[row + 1] = offsets[row] + n as u32;
    }
    let total = offsets[len] as usize;
    let built = build_from_pieces(ctx, &elem_ty, len, total, &|row, out: &mut Vec<Piece<'_>>| {
        if let Some(idx) = &order[row] {
            out.extend(idx.iter().map(|&i| Piece::One(child, i)));
        }
    });
    let mut offsets_buf = ctx.pool.take((len + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
    Column::list_from_parts(offsets_buf, built, validity, len)
}

/// How two elements of one array order. Elements of one array share a type, but for `I64` beside `F64`, which meet
/// by value.
fn elem_cmp(a: &Elem<'_>, b: &Elem<'_>) -> std::cmp::Ordering {
    match (a, b) {
        (Elem::Text(x), Elem::Text(y)) => x.cmp(y),
        (Elem::Bool(x), Elem::Bool(y)) => x.cmp(y),
        (Elem::Int(x), Elem::Int(y)) => x.cmp(y),
        (Elem::F64(x), Elem::F64(y)) => x.total_cmp(y),
        (Elem::Int(x), Elem::F64(y)) => (*x as f64).total_cmp(y),
        (Elem::F64(x), Elem::Int(y)) => x.total_cmp(&(*y as f64)),
        _ => std::cmp::Ordering::Equal,
    }
}

/// `indexOf(array, value)`: the first equal element's position, or `-1`.
pub fn array_index_of(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    let list = ins[0].list().unwrap();
    let mut out = Column::alloc(ctx.pool, &DataType::I64, len);
    {
        let o = out.i64_mut().unwrap();
        for row in 0..len {
            let Some(value) = needle_at(&list, ins[1], row) else {
                o.values[row] = -1;
                o.validity.set_valid(row, false);
                continue;
            };
            let (s, e) = (list.offsets[row] as usize, list.offsets[row + 1] as usize);
            o.values[row] = (s..e)
                .position(|i| element_eq(list.child, i, &value))
                .map_or(-1, |p| p as i64);
        }
        o.validity.recount();
    }
    out
}

/// `includes(array, value)`: whether any element equals `value`.
pub fn array_includes(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Bool, len) {
        return e;
    }
    let list = ins[0].list().unwrap();
    let mut out = Column::alloc(ctx.pool, &DataType::Bool, len);
    {
        let o = out.bool_mut().unwrap();
        for row in 0..len {
            let value = needle_at(&list, ins[1], row);
            let hit = value.as_ref().is_some_and(|v| {
                (list.offsets[row] as usize..list.offsets[row + 1] as usize).any(|i| element_eq(list.child, i, v))
            });
            bits::set_row(o.values, row, hit);
            o.validity.set_valid(row, value.is_some());
        }
        o.validity.recount();
    }
    out
}

/// `join(array, separator)`: every element rendered as `toString` would, a null element as nothing, as JS has it.
pub fn array_join(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let list = ins[0].list().unwrap();
    let sep = side::var(ins[1]).unwrap();
    let child = list.child;
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut buf: Vec<u8> = Vec::new();
    for row in 0..len {
        if !list.validity.is_valid(row) || !sep.is_valid(row) {
            b.push_null();
            continue;
        }
        buf.clear();
        for i in list.offsets[row] as usize..list.offsets[row + 1] as usize {
            if i > list.offsets[row] as usize {
                buf.extend_from_slice(sep.at(row));
            }
            render_element(child, i, &mut buf);
        }
        b.push_grow(ctx.pool, &buf);
    }
    b.finish()
}

/// `toString`'s rendering of one element; a null element renders as nothing.
fn render_element(child: &Column, i: usize, out: &mut Vec<u8>) {
    use std::io::Write;
    if !child.is_valid(i) {
        return;
    }
    match child.data_type() {
        DataType::Str => out.extend_from_slice(child.var().unwrap().get(i)),
        DataType::Bytes => crate::format::bytes_text::BytesForm::Base64.write(child.var().unwrap().get(i), out),
        DataType::Bool => out.extend_from_slice(if bits::get_row(child.bool().unwrap().values, i) {
            b"true"
        } else {
            b"false"
        }),
        DataType::I64 => write!(out, "{}", child.i64().unwrap().values[i]).unwrap(),
        DataType::F64 => write!(out, "{}", child.f64().unwrap().values[i]).unwrap(),
        DataType::DateTime => super::datetime::write_iso8601_micros_bare(child.datetime().unwrap().values[i], out),
        DataType::Duration => super::datetime::write_duration_micros_bare(child.duration().unwrap().values[i], out),
        DataType::Null | DataType::Array(_) | DataType::Struct(..) | DataType::Variant => {}
    }
}
