//! `TypedBuilder`: a declared type's column, written a row at a time as each value arrives, by the one table a
//! declared type means everywhere: at a parser's declared field, and at a declaration downstream of one
//! (`tag::decode_column`).
//!
//! A scalar is coerced as it is written, by the one table (`ops::convert`): lossy
//! narrowing (`1.9` under `i64`) is never a coercion, and widening (`i64` under `f64`) always is. An array's elements
//! are written into a child builder as the array arrives, and an object's declared fields into one builder each, its
//! other keys into its unclaimed fields. Nothing is staged as a variant and narrowed afterward.

use std::borrow::Cow;

use crate::core::bits;
use crate::core::buffer::{Buffer, BufferPool};
use crate::core::column::{Column, DataType, StrBuilder, Validity, VarType};
use crate::core::tally::TagCounts;
use crate::core::variant;
use crate::format::bytes_text::BytesForm;
use crate::format::tag::{self, Bytes, Emit, Tag};
use crate::ops::convert::{parse_bool, parse_f64, parse_i64, read_time, round_i64, timeparse_duration};
use crate::ops::timeparse::parse_epoch_digits;
use crate::plan::config::TimeSource;

/// One value on its way into a column: a text parser's field, a tag another parser typed already, or a value's
/// encoded bytes (`core::variant`).
#[derive(Clone, Copy)]
pub enum Cell<'a, 'd> {
    /// `quoted` is the author's "this is text". The column coerces it all the same; the tally counts it as text.
    Text {
        text: &'a str,
        quoted: bool,
    },
    Tag(&'a Tag<'d>),
    Variant(&'a [u8]),
}

/// The kinds `TagCounts` tallies.
#[derive(Clone, Copy)]
enum Kind {
    Bool,
    Int,
    Float,
    Str,
    Array,
    Object,
}

/// A row whose value did not fit, with the value as the pipeline language spells it where it arrived whole.
pub type Failed = (usize, Option<String>);

/// A column of one declared type, written in row order. A row never written is null; a row written twice keeps the
/// later value.
pub struct TypedBuilder {
    store: Store,
    /// Rows written so far, nulls included.
    filled: usize,
    /// Whether blank text is nothing here: true for a number, a bool, a time and a span. A blank under one is an empty
    /// cell, not a value that failed to be one, so the row stays null and clean, and the tally does not count it
    /// present. A declared `Str` keeps its `""`: it is text.
    blank_is_null: bool,
    /// Rows whose value did not fit the type, ascending.
    failed: Vec<Failed>,
    /// What the rows held, for the preview tally: the kind each row's value arrived as.
    counts: TagCounts,
    /// The kind counted for the row last written, taken back if that row is written again.
    last_kind: Option<Kind>,
    /// An object's unclaimed fields, or a container read whole, as a `Slot` writes them; reused row to row.
    bytes: Vec<u8>,
}

enum Store {
    Bool(Vec<Option<bool>>),
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    /// Microseconds since the epoch. Text reads in `format`, or by the automatic reading (`convert::read_time`),
    /// which tries first the shape that matched last (`hint`); a reading with no year takes it from `now`.
    DateTime {
        values: Vec<Option<i64>>,
        format: Option<String>,
        hint: usize,
        now: i64,
    },
    /// Microseconds.
    Duration(Vec<Option<i64>>),
    Str(Texts),
    /// Nothing is made bytes but bytes.
    Bytes(Texts),
    /// `offsets` has one entry more than the rows written: row `r`'s elements are the child's rows
    /// `offsets[r]..offsets[r + 1]`. A row that failed is null, its elements kept in the child.
    List {
        offsets: Vec<u32>,
        valid: Vec<bool>,
        child: Box<TypedBuilder>,
    },
    /// Each declared field's builder, and the keys it does not name, one variant object per row. A closed object has
    /// none: it drops each.
    Struct {
        fields: Vec<(String, TypedBuilder)>,
        unclaimed: Texts,
        open: bool,
    },
    /// Each value as it arrived, one encoded variant per row: a `variant` takes any value.
    Variant(Texts),
    /// A type no value is written as: every value present fails.
    Refused(DataType),
}

/// Variable-length rows: text, or a variant's bytes. The bytes stage in a pool buffer, so they are charged to the
/// budget as they arrive.
#[derive(Default)]
struct Texts {
    /// Taken from the pool at the first bytes pushed.
    data: Option<Buffer>,
    /// Bytes written so far.
    len: usize,
    /// One entry more than the rows written.
    offsets: Vec<u32>,
    valid: Vec<bool>,
}

impl Texts {
    fn new() -> Texts {
        Texts {
            data: None,
            len: 0,
            offsets: vec![0],
            valid: Vec::new(),
        }
    }

    /// Unwinds with `OverBudget` when the pool's budget refuses the bytes.
    fn push(&mut self, bytes: Option<&[u8]>, pool: &mut BufferPool) {
        if let Some(b) = bytes {
            let end = self.len + b.len();
            let data = match &mut self.data {
                Some(data) => {
                    data.grow(pool, end);
                    data
                }
                None => self.data.insert(pool.take(end)),
            };
            data.as_mut_slice::<u8>()[self.len..end].copy_from_slice(b);
            self.len = end;
        }
        self.valid.push(bytes.is_some());
        self.offsets
            .push(u32::try_from(self.len).expect("a column holds less than 4 GiB"));
    }

    fn push_null(&mut self) {
        self.valid.push(false);
        self.offsets
            .push(u32::try_from(self.len).expect("a column holds less than 4 GiB"));
    }

    fn truncate(&mut self, rows: usize) {
        self.len = self.offsets[rows] as usize;
        self.offsets.truncate(rows + 1);
        self.valid.truncate(rows);
    }

    fn finish(self, var_type: VarType, pool: &mut BufferPool) -> Column {
        let rows = self.valid.len();
        let mut b = StrBuilder::new(pool, var_type, rows);
        b.reserve(pool, self.len);
        let data = self.data.as_ref().map_or(&[][..], |d| &d.as_slice::<u8>()[..self.len]);
        for (row, &valid) in self.valid.iter().enumerate() {
            match valid {
                true => b.push(&data[self.offsets[row] as usize..self.offsets[row + 1] as usize]),
                false => b.push_null(),
            }
        }
        if let Some(d) = self.data {
            pool.give(d);
        }
        b.finish()
    }
}

impl TypedBuilder {
    /// A builder for `ty`. A `datetime` reads text as `time` says, by the automatic reading where it says nothing,
    /// and a reading with no year takes it from `now`.
    pub fn new(ty: &DataType, time: Option<&TimeSource>, now: i64) -> TypedBuilder {
        let store = match ty {
            DataType::Bool => Store::Bool(Vec::new()),
            DataType::I64 => Store::I64(Vec::new()),
            DataType::F64 => Store::F64(Vec::new()),
            DataType::DateTime => Store::DateTime {
                values: Vec::new(),
                format: match time {
                    Some(TimeSource::Format(f)) => Some(f.clone()),
                    _ => None,
                },
                hint: usize::MAX,
                now,
            },
            DataType::Duration => Store::Duration(Vec::new()),
            DataType::Str => Store::Str(Texts::new()),
            DataType::Bytes => Store::Bytes(Texts::new()),
            DataType::Array(elem) => Store::List {
                offsets: vec![0],
                valid: Vec::new(),
                child: Box::new(TypedBuilder::new(elem, None, now)),
            },
            DataType::Struct(fields, open) => Store::Struct {
                fields: fields
                    .iter()
                    .map(|(name, ty)| (name.clone(), TypedBuilder::new(ty, None, now)))
                    .collect(),
                unclaimed: Texts::new(),
                open: *open,
            },
            DataType::Variant => Store::Variant(Texts::new()),
            other => Store::Refused(other.clone()),
        };
        TypedBuilder {
            store,
            filled: 0,
            blank_is_null: matches!(
                ty,
                DataType::Bool | DataType::I64 | DataType::F64 | DataType::DateTime | DataType::Duration
            ),
            failed: Vec::new(),
            counts: TagCounts::default(),
            last_kind: None,
            bytes: Vec::new(),
        }
    }

    /// Write `cell` at `row`. Rows arrive in order; a row written twice keeps the later value. Unwinds with
    /// `OverBudget` when `pool`'s budget refuses what the cell stages.
    pub fn set(&mut self, row: usize, cell: Cell<'_, '_>, pool: &mut BufferPool) {
        match cell {
            // Bytes are walked into the column, as any walk writes it, unless the column keeps them as they are.
            Cell::Variant(b) if !matches!(self.store, Store::Variant(_)) => {
                tag::emit(b, &mut Slot::new(self, row, pool))
            }
            Cell::Tag(t @ (Tag::Array(_) | Tag::Object(_))) => {
                let mut bytes = Vec::new();
                tag::encode(t, &mut bytes);
                self.set(row, Cell::Variant(&bytes), pool);
            }
            cell => self.put(row, cell, pool),
        }
    }

    /// `set`, for a scalar, text, or a container the column reads whole.
    fn put(&mut self, row: usize, cell: Cell<'_, '_>, pool: &mut BufferPool) {
        if is_empty(cell, self.blank_is_null) {
            return self.set_null(row);
        }
        self.open_row(row);
        let (fits, kind) = self.write(row, cell, pool);
        self.close_row(row, fits, kind);
        if !fits {
            let mut bytes = Vec::new();
            match cell {
                Cell::Text { text, .. } => tag::encode(&Tag::Str(text.into()), &mut bytes),
                Cell::Tag(t) => tag::encode(t, &mut bytes),
                Cell::Variant(b) => bytes.extend_from_slice(b),
            }
            self.failed.last_mut().expect("close_row kept it").1 = Some(tag::json_text(&bytes));
        }
    }

    /// Write nothing at `row`: no kind, no count.
    fn set_null(&mut self, row: usize) {
        self.open_row(row);
        self.push_null();
        self.last_kind = None;
    }

    /// Ready row `row` to be written: the row last written is taken back, and the rows before it padded.
    fn open_row(&mut self, row: usize) {
        if row + 1 == self.filled {
            self.take_back(row);
        }
        debug_assert!(row >= self.filled, "a parser writes rows in order");
        self.pad(row);
    }

    /// Row `row` is written: whether it fit, and the kind it arrived as.
    fn close_row(&mut self, row: usize, fits: bool, kind: Kind) {
        self.filled = row + 1;
        if !fits {
            self.failed.push((row, None));
        }
        self.counts.present += 1;
        bump(&mut self.counts, kind, true);
        self.last_kind = Some(kind);
    }

    /// What the rows held.
    pub fn counts(&self) -> TagCounts {
        self.counts
    }

    /// The column, `rows` long, every row not written null, and the rows whose value did not fit.
    pub fn finish(mut self, rows: usize, pool: &mut BufferPool) -> (Column, Vec<Failed>) {
        self.pad(rows);
        let failed = std::mem::take(&mut self.failed);
        (self.into_column(pool), failed)
    }

    /// Append row `row` (`self.filled`) holding `cell`, null where it does not fit. Whether it fit, and the kind it
    /// arrived as.
    fn write(&mut self, row: usize, cell: Cell<'_, '_>, pool: &mut BufferPool) -> (bool, Kind) {
        self.filled = row + 1;
        match &mut self.store {
            Store::Bool(values) => {
                let parsed = match cell {
                    Cell::Text { text, .. } => parse_bool(text),
                    Cell::Tag(Tag::Str(s)) => parse_bool(s),
                    Cell::Tag(Tag::Bool(x)) => Some(*x),
                    _ => None,
                };
                values.push(parsed);
                (parsed.is_some(), arrived(cell, parsed.is_some(), Kind::Bool))
            }
            Store::I64(values) => {
                let parsed = match cell {
                    Cell::Text { text, .. } => parse_i64(text),
                    Cell::Tag(Tag::Str(s)) => parse_i64(s),
                    Cell::Tag(Tag::I64(x)) => Some(*x),
                    Cell::Tag(Tag::F64(x)) => round_i64(*x),
                    _ => None,
                };
                values.push(parsed);
                (parsed.is_some(), arrived(cell, parsed.is_some(), Kind::Int))
            }
            Store::F64(values) => {
                // Widening is always a coercion.
                let parsed = match cell {
                    Cell::Text { text, .. } => parse_f64(text),
                    Cell::Tag(Tag::Str(s)) => parse_f64(s),
                    Cell::Tag(Tag::F64(x)) => Some(*x),
                    Cell::Tag(Tag::I64(x)) => Some(*x as f64),
                    _ => None,
                };
                values.push(parsed);
                (parsed.is_some(), arrived(cell, parsed.is_some(), Kind::Float))
            }
            // A number is an epoch, its unit by magnitude.
            Store::DateTime {
                values,
                format,
                hint,
                now,
            } => {
                let parsed = match cell {
                    Cell::Text { text, .. } => read_time(text, format.as_deref(), hint, *now),
                    Cell::Tag(Tag::Str(s)) => read_time(s, format.as_deref(), hint, *now),
                    Cell::Tag(Tag::DateTime(x)) => Some(*x),
                    Cell::Tag(Tag::I64(x)) => parse_epoch_digits(&x.to_string()),
                    Cell::Tag(Tag::F64(x)) => parse_epoch_digits(&x.to_string()),
                    _ => None,
                };
                values.push(parsed);
                (parsed.is_some(), arrived(cell, false, Kind::Str))
            }
            Store::Duration(values) => {
                let parsed = match cell {
                    Cell::Text { text, .. } => timeparse_duration(text),
                    Cell::Tag(Tag::Str(s)) => timeparse_duration(s),
                    Cell::Tag(Tag::Duration(x) | Tag::I64(x)) => Some(*x),
                    Cell::Tag(Tag::F64(x)) => round_i64(*x),
                    _ => None,
                };
                values.push(parsed);
                (parsed.is_some(), arrived(cell, false, Kind::Str))
            }
            // A scalar renders as `toString` renders it, bytes in base64, and a container as its JSON text.
            Store::Str(texts) => {
                let text: Cow<'_, str> = match cell {
                    Cell::Text { text, .. } => (*text).into(),
                    Cell::Tag(t) => tag::stringify_scalar(t),
                    Cell::Variant(b) => tag::json_text(b).into(),
                };
                texts.push(Some(text.as_bytes()), pool);
                (true, arrived(cell, false, Kind::Str))
            }
            // Text reads as `parseBytes` reads it, in base64.
            Store::Bytes(texts) => {
                let mut read = Vec::new();
                let bytes = match cell {
                    Cell::Tag(Tag::Bytes(b)) => Some(b.as_ref()),
                    Cell::Text { text, .. } => BytesForm::Base64.read(text.as_bytes(), &mut read).then_some(&read[..]),
                    Cell::Tag(Tag::Str(s)) => BytesForm::Base64.read(s.as_bytes(), &mut read).then_some(&read[..]),
                    _ => None,
                };
                texts.push(bytes, pool);
                (bytes.is_some(), arrived(cell, false, Kind::Str))
            }
            // An array or an object the column takes apart arrives through `Slot`; anything else here does not fit.
            Store::List { offsets, valid, child } => {
                offsets.push(child.filled as u32);
                valid.push(false);
                (false, arrived(cell, false, Kind::Array))
            }
            Store::Struct { unclaimed, .. } => {
                unclaimed.push_null();
                (false, arrived(cell, false, Kind::Object))
            }
            Store::Variant(texts) => {
                let mut bytes = Vec::new();
                match cell {
                    Cell::Variant(b) => texts.push(Some(b), pool),
                    Cell::Tag(t) => {
                        tag::encode(t, &mut bytes);
                        texts.push(Some(&bytes), pool);
                    }
                    Cell::Text { text, .. } => {
                        variant::put_str(&mut bytes, text);
                        texts.push(Some(&bytes), pool);
                    }
                }
                (true, arrived(cell, false, Kind::Str))
            }
            Store::Refused(_) => (false, arrived(cell, false, Kind::Str)),
        }
    }

    /// Append a null row.
    fn push_null(&mut self) {
        match &mut self.store {
            Store::Bool(v) => v.push(None),
            Store::I64(v) | Store::Duration(v) | Store::DateTime { values: v, .. } => v.push(None),
            Store::F64(v) => v.push(None),
            Store::Str(t) | Store::Bytes(t) | Store::Variant(t) => t.push_null(),
            Store::List { offsets, valid, child } => {
                offsets.push(child.filled as u32);
                valid.push(false);
            }
            // A field not written on a row is null there, so the fields are padded when they are next written.
            Store::Struct { unclaimed, .. } => unclaimed.push_null(),
            Store::Refused(_) => {}
        }
        self.filled += 1;
    }

    /// Null rows up to `rows`.
    fn pad(&mut self, rows: usize) {
        while self.filled < rows {
            self.push_null();
        }
    }

    /// Undo the write at `row`, the row last written, so the next write replaces it.
    fn take_back(&mut self, row: usize) {
        if let Some(kind) = self.last_kind.take() {
            self.counts.present -= 1;
            bump(&mut self.counts, kind, false);
        }
        self.truncate(row);
    }

    /// Only the first `rows` rows.
    fn truncate(&mut self, rows: usize) {
        if self.filled <= rows {
            return;
        }
        while self.failed.last().is_some_and(|&(r, _)| r >= rows) {
            self.failed.pop();
        }
        match &mut self.store {
            Store::Bool(v) => v.truncate(rows),
            Store::I64(v) | Store::Duration(v) | Store::DateTime { values: v, .. } => v.truncate(rows),
            Store::F64(v) => v.truncate(rows),
            Store::Str(t) | Store::Bytes(t) | Store::Variant(t) => t.truncate(rows),
            Store::List { offsets, valid, child } => {
                child.truncate(offsets[rows] as usize);
                offsets.truncate(rows + 1);
                valid.truncate(rows);
            }
            Store::Struct { fields, unclaimed, .. } => {
                for (_, f) in fields {
                    f.truncate(rows);
                }
                unclaimed.truncate(rows);
            }
            Store::Refused(_) => {}
        }
        self.filled = rows;
    }

    fn into_column(self, pool: &mut BufferPool) -> Column {
        let rows = self.filled;
        match self.store {
            Store::Bool(values) => {
                let mut c = Column::alloc(pool, &DataType::Bool, rows);
                let b = c.bool_mut().expect("a Bool column");
                for (row, v) in values.into_iter().enumerate() {
                    bits::set_row(b.values, row, v.unwrap_or(false));
                    b.validity.set_valid(row, v.is_some());
                }
                c
            }
            Store::I64(values) => words(&DataType::I64, values, pool),
            Store::DateTime { values, .. } => words(&DataType::DateTime, values, pool),
            Store::Duration(values) => words(&DataType::Duration, values, pool),
            Store::F64(values) => {
                let mut c = Column::alloc(pool, &DataType::F64, rows);
                let f = c.f64_mut().expect("an F64 column");
                for (row, v) in values.into_iter().enumerate() {
                    f.values[row] = v.unwrap_or(0.0);
                    f.validity.set_valid(row, v.is_some());
                }
                c
            }
            Store::Str(texts) => texts.finish(VarType::Str, pool),
            Store::Bytes(texts) => texts.finish(VarType::Bytes, pool),
            Store::Variant(texts) => texts.finish(VarType::Variant, pool),
            Store::List { offsets, valid, child } => {
                let children = child.filled;
                let (child, _) = child.finish(children, pool);
                let mut validity = Validity::alloc_null(pool, rows);
                for (row, &v) in valid.iter().enumerate() {
                    validity.set_valid(row, v);
                }
                let mut buf = pool.take((rows + 1) * 4);
                buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
                Column::list_from_parts(buf, child, validity, rows)
            }
            Store::Struct { fields, unclaimed, .. } => {
                let columns = fields
                    .into_iter()
                    .map(|(name, f)| (name, f.finish(rows, pool).0))
                    .collect();
                let unclaimed = unclaimed
                    .valid
                    .iter()
                    .any(|&v| v)
                    .then(|| unclaimed.finish(VarType::Variant, pool));
                Column::struct_with_unclaimed_fields(columns, unclaimed, rows)
            }
            Store::Refused(ty) => Column::empty(ty, rows),
        }
    }
}

/// A declared column's value at one row, written as a walk reads it (`tag::Emit`). An array or an object the column
/// takes apart goes straight into its builders, element by element and field by field; any other container is
/// collected as bytes and read whole when it ends.
pub enum Slot<'b> {
    Row(Row<'b>),
    /// Inside a container read whole, or an open object's unclaimed field.
    Bytes(Bytes<&'b mut Vec<u8>>),
    /// A closed object's other keys.
    Skip,
}

/// One row of one builder, and what of its value has arrived.
pub struct Row<'b> {
    b: &'b mut TypedBuilder,
    row: usize,
    pool: &'b mut BufferPool,
    state: State,
}

/// The bytes a `Row` writes are the builder's `bytes`, taken while the row writes them, so each row reuses the last
/// one's buffer.
enum State {
    Value,
    /// An array going into a list's child: how many element rows had failed before it.
    List {
        before: usize,
    },
    /// An object going into a struct: its unclaimed fields so far, and whether the field opened last is one of them.
    Struct {
        rest: Bytes<Vec<u8>>,
        count: usize,
        unclaimed: bool,
    },
    /// A container the column reads whole.
    Whole(Bytes<Vec<u8>>),
}

impl<'b> Slot<'b> {
    pub fn new(b: &'b mut TypedBuilder, row: usize, pool: &'b mut BufferPool) -> Self {
        Slot::Row(Row {
            b,
            row,
            pool,
            state: State::Value,
        })
    }
}

impl Row<'_> {
    /// The builder's `bytes`, emptied, to write.
    fn take_bytes(&mut self) -> Bytes<Vec<u8>> {
        let mut out = std::mem::take(&mut self.b.bytes);
        out.clear();
        Bytes::new(out)
    }

    /// The container read whole ends: it is written now, and its buffer goes back to the builder.
    fn end_whole(&mut self) {
        if let State::Whole(bytes) = std::mem::replace(&mut self.state, State::Value) {
            self.b.put(self.row, Cell::Variant(&bytes.out), self.pool);
            self.b.bytes = bytes.out;
        }
    }
}

impl Emit for Slot<'_> {
    type Child<'a>
        = Slot<'a>
    where
        Self: 'a;

    fn scalar(&mut self, tag: &Tag<'_>) {
        match self {
            Slot::Row(c) => c.b.put(c.row, Cell::Tag(tag), c.pool),
            Slot::Bytes(b) => b.scalar(tag),
            Slot::Skip => {}
        }
    }

    fn null(&mut self) {
        match self {
            Slot::Row(c) => c.b.set_null(c.row),
            Slot::Bytes(b) => b.null(),
            Slot::Skip => {}
        }
    }

    fn begin_array(&mut self) {
        match self {
            Slot::Row(c) => match &c.b.store {
                Store::List { child, .. } => {
                    c.state = State::List {
                        before: child.failed.len(),
                    };
                    c.b.open_row(c.row);
                }
                _ => {
                    let mut bytes = c.take_bytes();
                    bytes.begin_array();
                    c.state = State::Whole(bytes);
                }
            },
            Slot::Bytes(b) => b.begin_array(),
            Slot::Skip => {}
        }
    }

    fn element(&mut self) -> Slot<'_> {
        match self {
            Slot::Row(c) => match (&mut c.state, &mut c.b.store) {
                (State::List { .. }, Store::List { child, .. }) => Slot::new(child, child.filled, c.pool),
                (State::Whole(bytes), _) => Slot::Bytes(bytes.element()),
                _ => unreachable!("an element is inside an array"),
            },
            Slot::Bytes(b) => Slot::Bytes(b.element()),
            Slot::Skip => Slot::Skip,
        }
    }

    // Every element must fit, or the row fails whole.
    fn end_array(&mut self, count: usize) {
        match self {
            Slot::Row(c) => match (&mut c.state, &mut c.b.store) {
                (State::List { before }, Store::List { offsets, valid, child }) => {
                    let fits = child.failed.len() == *before;
                    child.failed.truncate(*before);
                    offsets.push(child.filled as u32);
                    valid.push(fits);
                    c.b.close_row(c.row, fits, Kind::Array);
                }
                (State::Whole(bytes), _) => {
                    bytes.end_array(count);
                    c.end_whole();
                }
                _ => unreachable!("an array was begun"),
            },
            Slot::Bytes(b) => b.end_array(count),
            Slot::Skip => {}
        }
    }

    fn begin_object(&mut self) {
        match self {
            Slot::Row(c) => {
                let mut bytes = c.take_bytes();
                match &c.b.store {
                    Store::Struct { .. } => {
                        bytes.begin_object();
                        c.state = State::Struct {
                            rest: bytes,
                            count: 0,
                            unclaimed: false,
                        };
                        c.b.open_row(c.row);
                    }
                    _ => {
                        bytes.begin_object();
                        c.state = State::Whole(bytes);
                    }
                }
            }
            Slot::Bytes(b) => b.begin_object(),
            Slot::Skip => {}
        }
    }

    // Each declared field fits or fails on its own, so one failing field does not null its siblings. The keys the
    // declaration does not name are an open object's unclaimed fields, in the order the row had them; a closed object
    // drops each, and the row keeps its fields.
    fn field(&mut self, key: &str) -> Slot<'_> {
        match self {
            Slot::Row(c) => match (&mut c.state, &mut c.b.store) {
                (State::Struct { rest, count, unclaimed }, Store::Struct { fields, open, .. }) => {
                    *unclaimed = false;
                    if let Some((_, field)) = fields.iter_mut().find(|(n, _)| n == key) {
                        Slot::new(field, c.row, c.pool)
                    } else if *open {
                        *count += 1;
                        *unclaimed = true;
                        Slot::Bytes(rest.field(key))
                    } else {
                        Slot::Skip
                    }
                }
                (State::Whole(bytes), _) => Slot::Bytes(bytes.field(key)),
                _ => unreachable!("a field is inside an object"),
            },
            Slot::Bytes(b) => Slot::Bytes(b.field(key)),
            Slot::Skip => Slot::Skip,
        }
    }

    fn unfield(&mut self) {
        match self {
            Slot::Row(c) => match &mut c.state {
                State::Struct {
                    rest,
                    count,
                    unclaimed: true,
                } => {
                    rest.unfield();
                    *count -= 1;
                }
                State::Whole(bytes) => bytes.unfield(),
                _ => {}
            },
            Slot::Bytes(b) => b.unfield(),
            Slot::Skip => {}
        }
    }

    // A declared field written twice keeps the later value, as a row written twice does.
    fn end_object(&mut self, count: usize, repeats: bool) {
        match self {
            Slot::Row(c) => match (&mut c.state, &mut c.b.store) {
                (State::Struct { rest, count, .. }, Store::Struct { fields, unclaimed, .. }) => {
                    let row = c.row;
                    let fits = fields
                        .iter()
                        .all(|(_, f)| f.failed.last().is_none_or(|&(r, _)| r != row));
                    if *count > 0 {
                        rest.end_object(*count, repeats);
                        unclaimed.push(Some(&rest.out), c.pool);
                    } else {
                        unclaimed.push_null();
                    }
                    c.b.bytes = std::mem::take(&mut rest.out);
                    c.b.close_row(row, fits, Kind::Object);
                }
                (State::Whole(bytes), _) => {
                    bytes.end_object(count, repeats);
                    c.end_whole();
                }
                _ => unreachable!("an object was begun"),
            },
            Slot::Bytes(b) => b.end_object(count, repeats),
            Slot::Skip => {}
        }
    }
}

/// A column of `ty` holding one 64-bit word per row: an `i64`, a time or a span.
fn words(ty: &DataType, values: Vec<Option<i64>>, pool: &mut BufferPool) -> Column {
    let mut c = Column::alloc(pool, ty, values.len());
    let f = match ty {
        DataType::DateTime => c.datetime_mut(),
        DataType::Duration => c.duration_mut(),
        _ => c.i64_mut(),
    }
    .expect("a column of 64-bit words");
    for (row, v) in values.into_iter().enumerate() {
        f.values[row] = v.unwrap_or(0);
        f.validity.set_valid(row, v.is_some());
    }
    c
}

/// Whether `cell` holds nothing: a null, or, where `blank_is_null`, text with nothing in it, which under a typed column
/// is an empty cell rather than a value.
fn is_empty(cell: Cell<'_, '_>, blank_is_null: bool) -> bool {
    match cell {
        Cell::Text { text, .. } => blank_is_null && text.trim().is_empty(),
        Cell::Tag(Tag::Str(s)) => blank_is_null && s.trim().is_empty(),
        Cell::Tag(_) => false,
        Cell::Variant(b) => b[0] == variant::NULL,
    }
}

/// The kind a value arrived as, for the tally: a value's own kind; quoted text, a string; other text, the type it
/// parsed to, or a string where it did not parse.
fn arrived(cell: Cell<'_, '_>, parsed: bool, sink: Kind) -> Kind {
    match cell {
        Cell::Text { quoted: false, .. } if parsed => sink,
        Cell::Text { .. } => Kind::Str,
        Cell::Tag(t) => match t {
            Tag::Bool(_) => Kind::Bool,
            // A time, a span and bytes as JSON writes them.
            Tag::I64(_) | Tag::Duration(_) => Kind::Int,
            Tag::F64(_) => Kind::Float,
            Tag::Str(_) | Tag::DateTime(_) | Tag::Bytes(_) => Kind::Str,
            Tag::Array(_) => Kind::Array,
            Tag::Object(_) => Kind::Object,
        },
        // The tag byte alone, as `Tag`'s discriminant above.
        Cell::Variant(b) => match b[0] {
            variant::FALSE | variant::TRUE => Kind::Bool,
            variant::I64 | variant::DURATION => Kind::Int,
            variant::F64 => Kind::Float,
            variant::ARRAY => Kind::Array,
            variant::OBJECT => Kind::Object,
            _ => Kind::Str,
        },
    }
}

fn bump(counts: &mut TagCounts, kind: Kind, up: bool) {
    let slot = match kind {
        Kind::Bool => &mut counts.bools,
        Kind::Int => &mut counts.ints,
        Kind::Float => &mut counts.floats,
        Kind::Str => &mut counts.strings,
        Kind::Array => &mut counts.arrays,
        Kind::Object => &mut counts.objects,
    };
    if up {
        *slot += 1;
    } else {
        *slot -= 1;
    }
}

/// `values` written into a builder for `ty`, row by row: a column of `ty`, and the rows that did not fit.
pub fn build<'a, 'd: 'a>(
    values: impl Iterator<Item = Option<Cell<'a, 'd>>>,
    rows: usize,
    ty: &DataType,
    now: i64,
    pool: &mut BufferPool,
) -> (Column, Vec<Failed>) {
    let mut b = TypedBuilder::new(ty, None, now);
    for (row, v) in values.enumerate() {
        if let Some(v) = v {
            b.set(row, v, pool);
        }
    }
    b.finish(rows, pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    /// Rows of tags, the way a parser hands them over: `None` where a row holds nothing.
    struct Rows<'d>(Vec<Option<Tag<'d>>>);

    impl<'d> Rows<'d> {
        fn new(rows: usize) -> Rows<'d> {
            Rows(vec![None; rows])
        }

        fn set(&mut self, row: usize, tag: Tag<'d>) {
            self.0[row] = Some(tag);
        }
    }

    fn built(rows: Rows<'_>, ty: &DataType) -> (Column, Vec<usize>, BufferPool) {
        let mut pool = BufferPool::new();
        let n = rows.0.len();
        let (c, failed) = build(rows.0.iter().map(|t| t.as_ref().map(Cell::Tag)), n, ty, 0, &mut pool);
        (c, failed.into_iter().map(|(row, _)| row).collect(), pool)
    }

    fn object_tag<'d>(fields: Vec<(&'d str, Tag<'d>)>) -> Tag<'d> {
        Tag::Object(fields.into_iter().map(|(k, v)| (Cow::Borrowed(k), v)).collect())
    }

    fn text(s: &str) -> Cell<'_, 'static> {
        Cell::Text { text: s, quoted: false }
    }

    /// A quoted number under a declared `I64` still coerces, and the tally still calls it text.
    #[test]
    fn quoted_text_coerces_and_is_tallied_as_text() {
        let mut pool = BufferPool::new();
        let mut d = TypedBuilder::new(&DataType::I64, None, 0);
        d.set(
            0,
            Cell::Text {
                text: "42",
                quoted: true,
            },
            &mut pool,
        );
        assert_eq!((d.counts().ints, d.counts().strings), (0, 1));
        let (col, failed) = d.finish(1, &mut pool);
        assert!(failed.is_empty());
        assert_eq!(col.i64().unwrap().values, &[42]);
        col.reclaim(&mut pool);
    }

    #[test]
    fn a_string_column_fills_the_rows_it_skips_and_pads_the_end() {
        let mut pool = BufferPool::new();
        let mut d = TypedBuilder::new(&DataType::Str, None, 0);
        d.set(1, text("a"), &mut pool);
        d.set(3, text("b"), &mut pool);
        let (col, failed) = d.finish(5, &mut pool);
        let v = col.var().unwrap();
        assert!(failed.is_empty());
        let valid: Vec<bool> = (0..5).map(|r| v.validity.is_valid(r)).collect();
        assert_eq!(valid, [false, true, false, true, false]);
        assert_eq!(v.get(1), b"a");
        assert_eq!(v.get(3), b"b");
        col.reclaim(&mut pool);
    }

    #[test]
    fn the_last_write_to_a_row_wins() {
        let mut pool = BufferPool::new();
        let mut d = TypedBuilder::new(&DataType::Str, None, 0);
        d.set(0, text("first"), &mut pool);
        d.set(0, text("second"), &mut pool);
        d.set(1, Cell::Tag(&Tag::I64(7)), &mut pool);
        assert_eq!(d.counts().present, 2);
        assert_eq!(d.counts().strings, 1);
        assert_eq!(d.counts().ints, 1);
        let (col, failed) = d.finish(2, &mut pool);
        let v = col.var().unwrap();
        assert!(failed.is_empty());
        assert_eq!(v.get(0), b"second");
        assert_eq!(v.get(1), b"7");
        col.reclaim(&mut pool);
    }

    #[test]
    fn a_failure_is_taken_back_by_a_later_write_to_the_same_row() {
        let mut pool = BufferPool::new();
        let mut d = TypedBuilder::new(&DataType::I64, None, 0);
        d.set(0, text("abc"), &mut pool);
        d.set(0, text(" 42 "), &mut pool);
        d.set(1, Cell::Tag(&Tag::F64(1e300)), &mut pool);
        d.set(2, Cell::Tag(&Tag::Str(Cow::Borrowed("9"))), &mut pool);
        assert_eq!((d.counts().ints, d.counts().floats, d.counts().strings), (1, 1, 1));
        let (col, failed) = d.finish(3, &mut pool);
        let f = col.i64().unwrap();
        assert_eq!(failed, vec![(1, Some("1e300".into()))]);
        assert_eq!(f.values[0], 42);
        assert!(!f.validity.is_valid(1));
        assert_eq!(f.values[2], 9);
        col.reclaim(&mut pool);
    }

    #[test]
    fn a_float_column_widens_an_integer_and_a_bool_column_reads_text() {
        let mut pool = BufferPool::new();
        let mut f = TypedBuilder::new(&DataType::F64, None, 0);
        f.set(0, Cell::Tag(&Tag::I64(3)), &mut pool);
        f.set(1, text("2.5"), &mut pool);
        let (fc, failed) = f.finish(2, &mut pool);
        assert!(failed.is_empty());
        assert_eq!(fc.f64().unwrap().values, &[3.0, 2.5]);
        fc.reclaim(&mut pool);

        let mut b = TypedBuilder::new(&DataType::Bool, None, 0);
        b.set(0, text("true"), &mut pool);
        b.set(2, text("maybe"), &mut pool);
        let (bc, failed) = b.finish(3, &mut pool);
        assert_eq!(failed, vec![(2, Some("\"maybe\"".into()))]);
        let view = bc.bool().unwrap();
        assert!(bits::get_row(view.values, 0));
        assert!(!view.validity.is_valid(1));
        assert!(!view.validity.is_valid(2));
        bc.reclaim(&mut pool);
    }

    #[test]
    fn pinned_matching_tags_all_succeed() {
        let mut vb = Rows::new(2);
        vb.set(0, Tag::I64(1));
        vb.set(1, Tag::I64(2));
        let (c, failed, mut pool) = built(vb, &DataType::I64);
        assert!(failed.is_empty());
        assert_eq!(c.i64().unwrap().values, &[1, 2]);
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_mismatched_tag_fails_that_row_only() {
        let mut vb = Rows::new(2);
        vb.set(0, Tag::I64(1));
        vb.set(1, Tag::Str("nope".into()));
        let (c, failed, mut pool) = built(vb, &DataType::I64);
        assert_eq!(failed, vec![1]);
        {
            let f = c.i64().unwrap();
            assert_eq!(f.values[0], 1);
            assert!(!f.validity.is_valid(1));
        }
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_missing_row_is_null_not_a_failure() {
        let mut vb = Rows::new(2);
        vb.set(0, Tag::I64(1));
        let (c, failed, mut pool) = built(vb, &DataType::I64);
        assert!(failed.is_empty());
        assert!(!c.i64().unwrap().validity.is_valid(1));
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_f64_accepts_i64_tags_by_widening() {
        let mut vb = Rows::new(1);
        vb.set(0, Tag::I64(5));
        let (c, failed, mut pool) = built(vb, &DataType::F64);
        assert!(failed.is_empty());
        assert_eq!(c.f64().unwrap().values[0], 5.0);
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_i64_rounds_an_f64_tag_and_refuses_one_out_of_range() {
        let mut vb = Rows::new(2);
        vb.set(0, Tag::F64(5.5));
        vb.set(1, Tag::F64(1e300));
        let (c, failed, mut pool) = built(vb, &DataType::I64);
        assert_eq!(failed, vec![1]);
        assert_eq!(c.i64().unwrap().values[0], 6);
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_scalar_coerces_a_quoted_rendering_and_str_stringifies() {
        let mut vb = Rows::new(1);
        vb.set(0, Tag::Str("40".into()));
        let (c, failed, mut pool) = built(vb, &DataType::I64);
        assert!(failed.is_empty());
        assert_eq!(c.i64().unwrap().values, &[40]);
        c.reclaim(&mut pool);

        let mut vb = Rows::new(1);
        vb.set(0, Tag::I64(40));
        let (c, failed, mut pool) = built(vb, &DataType::Str);
        assert!(failed.is_empty());
        assert_eq!(c.var().unwrap().get(0), b"40");
        c.reclaim(&mut pool);
    }

    #[test]
    fn a_declared_datetime_reads_as_parse_time_does() {
        let mut vb = Rows::new(4);
        vb.set(0, Tag::Str("2024-01-01T00:00:00Z".into()));
        vb.set(1, Tag::I64(1_700_000_000));
        vb.set(2, Tag::Str("soon".into()));
        vb.set(3, Tag::Str(" ".into()));
        let (c, failed, mut pool) = built(vb, &DataType::DateTime);
        assert_eq!(failed, vec![2]);
        let t = c.datetime().unwrap();
        assert_eq!(t.values[0], 1_704_067_200_000_000);
        assert_eq!(t.values[1], 1_700_000_000_000_000);
        assert!(!t.validity.is_valid(2) && !t.validity.is_valid(3));
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_struct_coerces_declared_fields_and_leaves_missing_ones_null() {
        let mut vb = Rows::new(2);
        vb.set(0, object_tag(vec![("x", Tag::I64(1)), ("y", Tag::Bool(true))]));
        vb.set(1, object_tag(vec![("x", Tag::I64(2))]));
        let ty = DataType::Struct(vec![("x".into(), DataType::I64), ("y".into(), DataType::Bool)], false);
        let (c, failed, mut pool) = built(vb, &ty);
        assert!(failed.is_empty());
        assert_eq!(c.field("x").unwrap().i64().unwrap().values, &[1, 2]);
        let y = c.field("y").unwrap().bool().unwrap();
        assert!(y.validity.is_valid(0));
        assert!(!y.validity.is_valid(1));
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_struct_one_bad_field_fails_the_row_without_nulling_its_siblings() {
        let mut vb = Rows::new(1);
        vb.set(0, object_tag(vec![("x", Tag::I64(1)), ("y", Tag::Str("nope".into()))]));
        let ty = DataType::Struct(vec![("x".into(), DataType::I64), ("y".into(), DataType::I64)], false);
        let (c, failed, mut pool) = built(vb, &ty);
        assert_eq!(failed, vec![0]);
        assert!(c.field("x").unwrap().i64().unwrap().validity.is_valid(0));
        assert!(!c.field("y").unwrap().i64().unwrap().validity.is_valid(0));
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_struct_rejects_a_row_thats_not_an_object_at_all() {
        let mut vb = Rows::new(1);
        vb.set(0, Tag::I64(5));
        let ty = DataType::Struct(vec![("x".into(), DataType::I64)], false);
        let (c, failed, mut pool) = built(vb, &ty);
        assert_eq!(failed, vec![0]);
        c.reclaim(&mut pool);
    }

    #[test]
    fn pinned_array_missing_row_is_null_not_a_failure() {
        let vb = Rows::new(1);
        let (c, failed, mut pool) = built(vb, &DataType::Array(Box::new(DataType::I64)));
        assert!(failed.is_empty());
        assert!(!c.list().unwrap().validity.is_valid(0));
        c.reclaim(&mut pool);
    }

    /// Staged text is charged to the budget as it arrives, so a field past the limit is refused, not held.
    #[test]
    fn staged_text_past_the_budget_is_refused() {
        use crate::core::buffer::{MemoryBudget, catch_over_budget};
        let big = "x".repeat(1 << 16);
        for ty in [DataType::Str, DataType::Struct(vec![], true)] {
            let mut pool = BufferPool::with_budget(MemoryBudget::new(1 << 12));
            let mut b = TypedBuilder::new(&ty, None, 0);
            let tag = object_tag(vec![("k", Tag::Str(Cow::Borrowed(big.as_str())))]);
            let cell = match ty {
                DataType::Str => text(&big),
                _ => Cell::Tag(&tag),
            };
            assert!(catch_over_budget(|| b.set(0, cell, &mut pool)).is_err());
        }
    }

    /// A closed object keeps its fields and drops each other key, at any depth, and the row still fits.
    #[test]
    fn a_closed_object_drops_the_keys_it_does_not_name() {
        let elem = DataType::Struct(vec![("x".into(), DataType::I64)], false);
        let ty = DataType::Struct(vec![("a".into(), DataType::Array(Box::new(elem)))], false);
        let inner = object_tag(vec![("x", Tag::I64(1)), ("y", Tag::I64(2))]);
        let row = object_tag(vec![("a", Tag::Array(vec![Some(inner)])), ("z", Tag::I64(3))]);
        let mut pool = BufferPool::new();
        let mut b = TypedBuilder::new(&ty, None, 0);
        b.set(0, Cell::Tag(&row), &mut pool);
        b.set(1, Cell::Tag(&object_tag(vec![("z", Tag::I64(4))])), &mut pool);
        let (c, failed) = b.finish(2, &mut pool);
        assert!(failed.is_empty());
        let v = tag::to_variant(&c, &mut pool);
        let want = object_tag(vec![(
            "a",
            Tag::Array(vec![Some(object_tag(vec![("x", Tag::I64(1))]))]),
        )]);
        assert_eq!(tag::decode(v.variant().unwrap().get(0)), Some(want));
        v.reclaim(&mut pool);
        c.reclaim(&mut pool);
    }

    /// An array of open objects keeps each element's keys its declaration does not name, and a row written twice
    /// keeps only the later array: one failing element fails that row whole.
    #[test]
    fn an_array_of_objects_written_twice_keeps_the_later_one() {
        let ty = DataType::Array(Box::new(DataType::Struct(vec![("a".into(), DataType::I64)], true)));
        let first = Tag::Array(vec![Some(object_tag(vec![("a", Tag::I64(1)), ("z", Tag::I64(9))]))]);
        let second = Tag::Array(vec![
            Some(object_tag(vec![("a", Tag::I64(2))])),
            None,
            Some(object_tag(vec![("a", Tag::Str("x".into()))])),
        ]);
        let mut pool = BufferPool::new();
        let mut b = TypedBuilder::new(&ty, None, 0);
        b.set(0, Cell::Tag(&first), &mut pool);
        b.set(0, Cell::Tag(&second), &mut pool);
        b.set(1, Cell::Tag(&first), &mut pool);
        assert_eq!(b.counts().arrays, 2);
        let (c, failed) = b.finish(3, &mut pool);
        assert_eq!(failed, vec![(0, None)]);
        let v = tag::to_variant(&c, &mut pool);
        let rows = v.variant().unwrap();
        assert!(!rows.validity.is_valid(0));
        assert_eq!(tag::decode(rows.get(1)), Some(first));
        assert!(!rows.validity.is_valid(2));
        v.reclaim(&mut pool);
        c.reclaim(&mut pool);
    }
}
