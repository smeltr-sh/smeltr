//! Parquet object bodies, cut straight from a batch's columns.
//!
//! A `Schema` is a route's record (`plan::dictionary::record_type`) as Arrow columns, every one nullable. `Chunk::cut`
//! reads the rows a route sends to Parquet off the batch's record view (`format::record`), once per batch, while the
//! columns are still alive; each row leaves egress as its place in that chunk (`Columns`). `FileWriter` gathers rows
//! from their chunks into one Parquet file, a row group at a time; `write` is the whole file in one call.
//!
//! What a type is written as, where the engine and Parquet differ:
//! - `datetime` is `Timestamp(µs, UTC)`; `duration` is `Int64` microseconds.
//! - `bytes` is `Binary`, the bytes as they are.
//! - A variant is `Utf8` holding the value's JSON text, so a string variant keeps its quotes.
//! - An open struct has one more `Utf8` child, `rest`: a JSON object of the keys its fields do not name, null when
//!   there are none.
//! - A float JSON cannot hold is null, as the JSON writer leaves it out.
//!
//! Where the record types a place wider than the column holding it (the tag field, met with what the pipeline writes
//! there), each row is read into the record's type by the declared-type table (`tag::decode_column`). A row that does
//! not fit is marked in the chunk, and refused alone where it is delivered.

use std::sync::{Arc, Mutex};

use arrow_array::builder::StringBuilder;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, NullArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType as Arrow, Field, Fields, Schema as ArrowSchema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;

use crate::core::bits;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType, Scalar, StrBuilder, VarType};
use crate::core::variant::{self, Value};
use crate::format::json;
use crate::format::record::{self, RecordView};
use crate::format::tag;
use crate::plan::topology::Compression;

/// The column an open struct keeps its unclaimed keys in.
pub(crate) const REST: &str = "rest";

/// A record type as Parquet columns. Cloning shares it.
#[derive(Clone, Debug)]
pub struct Schema {
    root: Arc<Node>,
    arrow: Arc<ArrowSchema>,
    /// The schema a file is written with, when it differs from `arrow` in names and metadata alone: a table format's
    /// field ids or physical column names. Rows are cut to and matched against `arrow`.
    written: Option<Arc<ArrowSchema>>,
}

#[derive(Debug)]
struct Node {
    ty: DataType,
    kind: Kind,
    arrow: Arrow,
}

#[derive(Debug)]
enum Kind {
    Null,
    Bool,
    Int,
    Float,
    Time,
    Text,
    Binary,
    Variant,
    List(Box<Node>, Arc<Field>),
    Struct(Struct),
}

#[derive(Debug)]
struct Struct {
    fields: Vec<(String, Node)>,
    open: bool,
    arrow: Fields,
}

impl Struct {
    fn names(&self, key: &str) -> bool {
        self.fields.iter().any(|(name, _)| name == key)
    }

    /// The struct of `children`, valid on a row where any child is, `rest` included.
    fn array(&self, children: Vec<ArrayRef>) -> ArrayRef {
        let rows = children.first().map_or(0, |c| c.len());
        let valid: Vec<bool> = (0..rows).map(|i| children.iter().any(|c| c.is_valid(i))).collect();
        Arc::new(StructArray::new(self.arrow.clone(), children, nulls(valid)))
    }
}

/// A batch's rows as one route's record, in Parquet's columns: what a row bound for Parquet points into. Lives as
/// long as any row that points into it.
#[derive(Debug, PartialEq)]
pub struct Chunk {
    batch: RecordBatch,
    /// Why each row that does not fit the record is refused, by its place in the chunk, ascending.
    unfit: Vec<(u32, String)>,
    /// Each row's own bytes of the chunk, for an object's size.
    sizes: Vec<u32>,
    /// What the whole chunk holds in memory, for the queue's accounting: one row held holds all of it.
    bytes: usize,
}

/// One row as columns: its place in a chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct Columns {
    pub chunk: Arc<Chunk>,
    pub row: u32,
}

impl Schema {
    /// Err when the record cannot be written as Parquet: its root is no struct, a struct has no columns, or an open
    /// struct has a field named `rest`.
    pub fn new(record: &DataType) -> Result<Schema, String> {
        if !matches!(record, DataType::Struct(..)) {
            return Err(format!("the record is of type {record}, and Parquet writes a struct"));
        }
        let root = node(record, "")?;
        let Kind::Struct(s) = &root.kind else {
            unreachable!("checked above")
        };
        let arrow = Arc::new(ArrowSchema::new(s.arrow.clone()));
        Ok(Schema {
            root: Arc::new(root),
            arrow,
            written: None,
        })
    }

    /// This schema, its files written with `written`: the same fields, at any depth, renamed or with metadata of their
    /// own, such as Iceberg's field ids or Delta's physical names.
    pub fn with_written(self, written: Arc<ArrowSchema>) -> Schema {
        assert!(
            written.fields().len() == self.arrow.fields().len()
                && (written.fields().iter().zip(self.arrow.fields())).all(|(w, a)| alike(w.data_type(), a.data_type())),
            "a written schema is the record's own, renamed or with metadata added"
        );
        Schema {
            written: Some(written),
            ..self
        }
    }

    /// Whether `columns` were cut to this schema, so they can be written by a writer opened on it.
    pub fn matches(&self, columns: &Columns) -> bool {
        let theirs = columns.chunk.batch.schema_ref();
        Arc::ptr_eq(&self.arrow, theirs) || *self.arrow == **theirs
    }

    /// The Arrow schema, for a format that maps it to its own.
    pub fn arrow(&self) -> &Arc<ArrowSchema> {
        &self.arrow
    }

    /// The record these columns hold, for a format that tells a variant from text, which Arrow types alike.
    pub fn record(&self) -> &DataType {
        &self.root.ty
    }

    fn root(&self) -> &Struct {
        let Kind::Struct(s) = &self.root.kind else {
            unreachable!("Schema::new takes only a struct")
        };
        s
    }
}

impl Chunk {
    /// The rows `parts` name, each part a view of the batch and the batch rows read through it, ascending, in that
    /// order: a row's place in the chunk is its place in that sequence. A time read with no year takes it from `now`.
    pub fn cut(schema: &Schema, parts: &[(&RecordView<'_>, &[u32])], now: i64, pool: &mut BufferPool) -> Chunk {
        let mut batches = Vec::with_capacity(parts.len());
        let mut unfit = Vec::new();
        let mut at = 0;
        for (view, rows) in parts.iter().filter(|(_, rows)| !rows.is_empty()) {
            let mut cut = Cut {
                rows,
                now,
                unfit: Vec::new(),
            };
            let inherited = vec![None; rows.len()];
            let children = cut.object(&view.root, view, &inherited, schema.root(), "", pool);
            let batch = RecordBatch::try_new_with_options(
                schema.arrow.clone(),
                children,
                &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
            )
            .expect("the columns are cut to the schema they are checked against");
            unfit.extend(cut.unfit.into_iter().map(|(i, said)| (at + i, said)));
            at += rows.len() as u32;
            batches.push(batch);
        }
        let batch = match batches.len() {
            1 => batches.pop().expect("one"),
            _ => arrow_select::concat::concat_batches(&schema.arrow, &batches).expect("batches of one schema"),
        };
        Chunk::new(batch, unfit)
    }

    fn new(batch: RecordBatch, unfit: Vec<(u32, String)>) -> Chunk {
        let mut sizes = vec![0; batch.num_rows()];
        for column in batch.columns() {
            add_sizes(column, &mut sizes);
        }
        let bytes = batch.get_array_memory_size();
        Chunk {
            batch,
            unfit,
            sizes,
            bytes,
        }
    }

    /// The record of `{_raw: str}` for rows no pipeline ran on: each one's text.
    pub fn raw<'t>(schema: &Schema, texts: impl IntoIterator<Item = &'t str>) -> Chunk {
        let text: StringArray = texts.into_iter().map(Some).collect();
        let batch = RecordBatch::try_new(schema.arrow.clone(), vec![Arc::new(text)]).expect("the `{_raw: str}` record");
        Chunk::new(batch, Vec::new())
    }

    /// The chunk's columns, for a reader of one row's values.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Why row `row` is refused, if it is.
    pub fn unfit(&self, row: u32) -> Option<&str> {
        let at = self.unfit.binary_search_by_key(&row, |(r, _)| *r).ok()?;
        Some(&self.unfit[at].1)
    }

    /// Row `row`'s own bytes of this chunk.
    pub fn row_bytes(&self, row: u32) -> usize {
        self.sizes[row as usize] as usize
    }

    /// What the whole chunk holds in memory.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Columns {
    /// This row in a chunk of its own, so that holding it holds none of its batch's other rows.
    pub fn alone(&self) -> Columns {
        let batch = arrow_select::interleave::interleave_record_batch(&[&self.chunk.batch], &[(0, self.row as usize)])
            .expect("a row of its own chunk");
        let unfit = self
            .chunk
            .unfit(self.row)
            .map(|said| (0, said.to_string()))
            .into_iter()
            .collect();
        Columns {
            chunk: Arc::new(Chunk::new(batch, unfit)),
            row: 0,
        }
    }
}

/// Whether `a` and `b` are one type but for the names and metadata of the fields within.
fn alike(a: &Arrow, b: &Arrow) -> bool {
    match (a, b) {
        (Arrow::Struct(a), Arrow::Struct(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| alike(a.data_type(), b.data_type()))
        }
        (Arrow::List(a), Arrow::List(b)) => alike(a.data_type(), b.data_type()),
        (a, b) => a == b,
    }
}

/// `batch` under `schema`, which `alike` matches to its own. No value is copied.
fn relabel(batch: &RecordBatch, schema: &Arc<ArrowSchema>) -> RecordBatch {
    let columns = (batch.columns().iter())
        .zip(schema.fields())
        .map(|(column, field)| relabelled(column, field.data_type()))
        .collect();
    RecordBatch::try_new(schema.clone(), columns).expect("a written schema alike the rows' own")
}

fn relabelled(array: &ArrayRef, to: &Arrow) -> ArrayRef {
    use arrow_array::cast::AsArray;
    if array.data_type() == to {
        return array.clone();
    }
    match to {
        Arrow::Struct(fields) => {
            let parent = array.as_struct_opt().expect("a written schema alike the rows' own");
            let children = (parent.columns().iter())
                .zip(fields)
                .map(|(child, field)| relabelled(child, field.data_type()))
                .collect();
            Arc::new(StructArray::new(fields.clone(), children, parent.nulls().cloned()))
        }
        Arrow::List(field) => {
            let list = array
                .as_list_opt::<i32>()
                .expect("a written schema alike the rows' own");
            let values = relabelled(list.values(), field.data_type());
            Arc::new(ListArray::new(
                field.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            ))
        }
        _ => unreachable!("`alike` holds other types equal"),
    }
}

/// Adds each row's bytes of `array` to `sizes`: its fixed width, or a string's length and offset, and a list's items.
fn add_sizes(array: &ArrayRef, sizes: &mut [u32]) {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType as Arrow;
    match array.data_type() {
        Arrow::Struct(_) => {
            for child in array.as_struct().columns() {
                add_sizes(child, sizes);
            }
        }
        Arrow::List(_) => {
            let list = array.as_list::<i32>();
            let mut items = vec![0; list.values().len()];
            add_sizes(list.values(), &mut items);
            for (row, w) in list.offsets().windows(2).enumerate() {
                sizes[row] += 4 + items[w[0] as usize..w[1] as usize].iter().sum::<u32>();
            }
        }
        Arrow::Utf8 | Arrow::Binary => {
            let offsets = match array.data_type() {
                Arrow::Utf8 => array.as_string::<i32>().offsets().clone(),
                _ => array.as_binary::<i32>().offsets().clone(),
            };
            for (row, w) in offsets.windows(2).enumerate() {
                sizes[row] += 4 + (w[1] - w[0]) as u32;
            }
        }
        other => {
            let width = other.primitive_width().unwrap_or(1) as u32;
            sizes.iter_mut().for_each(|s| *s += width);
        }
    }
}

/// Writes one Parquet file a row group at a time, so a file larger than memory streams out as it grows.
pub struct FileWriter {
    writer: ArrowWriter<Sink>,
    sink: Sink,
    rows: u64,
    /// The schema each batch is relabelled with before it is written, when the file's differs from its rows'.
    written: Option<Arc<ArrowSchema>>,
}

/// What the Parquet writer writes into, shared with the `FileWriter` that drains it.
#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("no writer panics holding the sink")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Sink {
    fn drain(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0.lock().expect("no writer panics holding the sink"))
    }
}

impl FileWriter {
    /// A writer for rows of chunks cut to `schema`, compressed with `codec`.
    pub fn open(schema: &Schema, codec: Compression) -> FileWriter {
        match &schema.written {
            Some(written) => FileWriter {
                written: Some(written.clone()),
                ..FileWriter::on(written.clone(), codec)
            },
            None => FileWriter::on(schema.arrow.clone(), codec),
        }
    }

    fn on(schema: Arc<ArrowSchema>, codec: Compression) -> FileWriter {
        use parquet::basic::{Compression as Codec, GzipLevel, ZstdLevel};
        let compression = match codec {
            Compression::None => Codec::UNCOMPRESSED,
            Compression::Gzip => Codec::GZIP(GzipLevel::default()),
            Compression::Snappy => Codec::SNAPPY,
            Compression::Lz4 => Codec::LZ4_RAW,
            Compression::Zstd => Codec::ZSTD(ZstdLevel::default()),
        };
        // Row groups end where `take_row_group` says, not where the writer's own limits fall.
        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(None)
            .build();
        let sink = Sink::default();
        // Writing to memory fails only on a schema Parquet cannot hold, and `Schema::new` refuses those.
        let writer = ArrowWriter::try_new(sink.clone(), schema, Some(props)).expect("a writable schema");
        FileWriter {
            writer,
            sink,
            rows: 0,
            written: None,
        }
    }

    /// Appends `rows` to the row group in progress. Every row's chunk has the writer's schema (the caller checks
    /// `Schema::matches`).
    pub fn write<'a>(&mut self, rows: impl IntoIterator<Item = &'a Columns>) {
        let mut chunks: Vec<&Arc<Chunk>> = Vec::new();
        let mut at: Vec<(usize, usize)> = Vec::new();
        for c in rows {
            let i = match chunks.iter().rposition(|k| Arc::ptr_eq(k, &c.chunk)) {
                Some(i) => i,
                None => {
                    chunks.push(&c.chunk);
                    chunks.len() - 1
                }
            };
            at.push((i, c.row as usize));
        }
        if at.is_empty() {
            return;
        }
        let batches: Vec<&RecordBatch> = chunks.iter().map(|c| &c.batch).collect();
        let mut batch = arrow_select::interleave::interleave_record_batch(&batches, &at).expect("chunks of one schema");
        if let Some(written) = &self.written {
            batch = relabel(&batch, written);
        }
        self.writer.write(&batch).expect("a batch of the writer's own schema");
        self.rows += at.len() as u64;
    }

    /// Bytes of the row group in progress, uncompressed, as Arrow holds them.
    pub fn in_progress_bytes(&self) -> usize {
        self.writer.in_progress_size()
    }

    /// Ends the row group in progress and hands back every byte written so far and not yet taken. Empty if nothing is
    /// in progress and nothing is pending.
    pub fn take_row_group(&mut self) -> Vec<u8> {
        // Only the file's magic is pending before the first row, and it goes out with the first row group.
        if self.writer.in_progress_rows() == 0 {
            return Vec::new();
        }
        self.writer.flush().expect("writing to memory");
        // The row group's bytes sit in the writer's own buffer until it is synced.
        self.writer.sync().expect("writing to memory");
        self.sink.drain()
    }

    /// Bytes the finished file will have had so far: taken bytes plus what `take_row_group` would return now
    /// (an estimate for the row group in progress).
    pub fn bytes_written(&self) -> usize {
        // The writer counts every byte it has encoded, taken or still buffered on its way to the sink.
        self.writer.bytes_written() + self.writer.in_progress_size()
    }

    /// Row count written, in progress included.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Ends the file: the remaining bytes (the last row group and the footer) and the file's metadata, from which a
    /// format derives its statistics.
    pub fn finish(mut self) -> (Vec<u8>, ParquetMetaData) {
        let metadata = self.writer.finish().expect("writing to memory");
        self.writer.sync().expect("writing to memory");
        (self.sink.drain(), metadata)
    }
}

/// One Parquet file of `rows`, one row group, each column chunk compressed with `codec`. Every row's chunk has one
/// schema.
pub fn write<'a>(rows: impl IntoIterator<Item = &'a Columns>, codec: Compression) -> Vec<u8> {
    let mut rows = rows.into_iter().peekable();
    let Some(first) = rows.peek() else {
        return Vec::new();
    };
    let mut writer = FileWriter::on(first.chunk.batch.schema(), codec);
    writer.write(rows);
    writer.finish().0
}

fn node(ty: &DataType, path: &str) -> Result<Node, String> {
    let (kind, arrow) = match ty {
        DataType::Null => (Kind::Null, Arrow::Null),
        DataType::Bool => (Kind::Bool, Arrow::Boolean),
        DataType::I64 | DataType::Duration => (Kind::Int, Arrow::Int64),
        DataType::F64 => (Kind::Float, Arrow::Float64),
        DataType::DateTime => (Kind::Time, Arrow::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
        DataType::Str => (Kind::Text, Arrow::Utf8),
        DataType::Bytes => (Kind::Binary, Arrow::Binary),
        DataType::Variant => (Kind::Variant, Arrow::Utf8),
        DataType::Array(elem) => {
            let elem = node(elem, &format!("{path}[]"))?;
            let field = Arc::new(Field::new_list_field(elem.arrow.clone(), true));
            (Kind::List(Box::new(elem), field.clone()), Arrow::List(field))
        }
        DataType::Struct(fields, open) => {
            if *open && fields.iter().any(|(name, _)| name == REST) {
                return Err(format!(
                    "{} is a field of an open struct, whose unclaimed keys the column {REST} holds",
                    at(path, REST)
                ));
            }
            if fields.is_empty() && !open {
                let what = if path.is_empty() { "the record" } else { path };
                return Err(format!("{what} is a struct with no fields, which Parquet cannot write"));
            }
            let fields = fields
                .iter()
                .map(|(name, t)| Ok((name.clone(), node(t, &at(path, name))?)))
                .collect::<Result<Vec<_>, String>>()?;
            let mut arrow: Vec<Field> = fields
                .iter()
                .map(|(name, n)| Field::new(name, n.arrow.clone(), true))
                .collect();
            if *open {
                arrow.push(Field::new(REST, Arrow::Utf8, true));
            }
            let arrow = Fields::from(arrow);
            (
                Kind::Struct(Struct {
                    fields,
                    open: *open,
                    arrow: arrow.clone(),
                }),
                Arrow::Struct(arrow),
            )
        }
    };
    Ok(Node {
        ty: ty.clone(),
        kind,
        arrow,
    })
}

/// A struct's columns as the Parquet writer writes them: its fields, then `rest` when it is open. A field that
/// never holds a value is left out: Parquet writes it untyped, which no table type reads, and a column the record
/// does not write reads as null anyway.
pub(crate) fn columns(fields: &[(String, DataType)], open: bool) -> Vec<(String, DataType)> {
    let mut columns: Vec<_> = fields.iter().filter(|(_, t)| holds_values(t)).cloned().collect();
    if open {
        columns.push((REST.to_string(), DataType::Str));
    }
    columns
}

/// Whether a column of `t` can hold a value: it is not `Null`, nor a list or a closed struct of nothing else.
fn holds_values(t: &DataType) -> bool {
    match t {
        DataType::Null => false,
        DataType::Array(element) => holds_values(element),
        DataType::Struct(fields, open) => *open || fields.iter().any(|(_, t)| holds_values(t)),
        _ => true,
    }
}

/// `name` under `path`, as a refusal spells it.
pub(crate) fn at(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{path}.{name}")
    }
}

/// One part of a chunk being cut: the batch rows it reads, and the rows that did not fit, by their place in `rows`.
struct Cut<'r> {
    rows: &'r [u32],
    now: i64,
    unfit: Vec<(u32, String)>,
}

impl Cut<'_> {
    fn refuse(&mut self, at: usize, said: String) {
        if let Err(i) = self.unfit.binary_search_by_key(&(at as u32), |(r, _)| *r) {
            self.unfit.insert(i, (at as u32, said));
        }
    }

    /// The columns of `s` from the view's object `o`, the record's own at the root. `inherited` is, row by row, what
    /// the parent's unclaimed fields hold at this object's key.
    fn object(
        &mut self,
        o: &record::Object<'_>,
        view: &RecordView<'_>,
        inherited: &[Option<&[u8]>],
        s: &Struct,
        path: &str,
        pool: &mut BufferPool,
    ) -> Vec<ArrayRef> {
        let own: Vec<Option<&[u8]>> = self
            .rows
            .iter()
            .zip(inherited)
            .map(|(&row, inherited)| o.unclaimed_at(row as usize, *inherited))
            .collect();
        let own_object: Vec<Option<&[u8]>> = own.iter().map(|v| v.filter(|v| variant::is_object(v))).collect();
        let mut children: Vec<ArrayRef> = Vec::with_capacity(s.fields.len() + 1);
        for (name, node) in &s.fields {
            let path = at(path, name);
            let below = || own_object.iter().map(|v| o.member(*v, name)).collect::<Vec<_>>();
            children.push(match (o.field(name), &node.kind) {
                (Some(record::Field::Leaf(col, _)), _) => self.leaf(col, node, &path, pool),
                (Some(record::Field::Object(child)), Kind::Struct(cs)) => {
                    let columns = self.object(child, view, &below(), cs, &path, pool);
                    cs.array(columns)
                }
                // An object where the record holds a variant: its JSON text.
                (Some(record::Field::Object(child)), _) => {
                    json_text(self.rows.iter().zip(below()), |(&row, below), doc| {
                        json::write_object(child, below, &view.leaves, row as usize, 1, doc)
                    })
                }
                // A field only this record names: what the row's unclaimed fields hold at its key, if anything.
                (None, _) => self.members(&below(), node, &path, pool),
            });
        }
        if s.open {
            children.push(rest(own_object.iter().copied(), |key| o.claims(key) || s.names(key)));
        }
        // Where nothing is written and the unclaimed fields hold a value that is no object, the JSON writer writes that
        // value in the object's place, and no struct holds it.
        for (i, own) in own.iter().enumerate() {
            if let Some(v) = own.filter(|v| !variant::is_object(v))
                && !children.iter().any(|c| c.is_valid(i))
            {
                let mut doc = Vec::new();
                if json::write_variant(v, &mut doc) {
                    let what = if path.is_empty() { "the record" } else { path };
                    self.refuse(
                        i,
                        format!("{what} is {}, which is no struct", String::from_utf8_lossy(&doc)),
                    );
                }
            }
        }
        children
    }

    /// `col`'s rows as `node`. A column the record types wider is read into the record's type row by row.
    fn leaf(&mut self, col: &Column, node: &Node, path: &str, pool: &mut BufferPool) -> ArrayRef {
        if fits(&col.data_type(), &node.ty) {
            return direct(col, node, self.rows);
        }
        let variant = tag::to_variant(col, pool);
        let array = self.decoded(&variant, node, path, self.rows, pool);
        variant.reclaim(pool);
        array
    }

    /// `values`, one variant per row of the cut, as `node`.
    fn members(&mut self, values: &[Option<&[u8]>], node: &Node, path: &str, pool: &mut BufferPool) -> ArrayRef {
        if values.iter().all(Option::is_none) {
            return new_null_array(&node.arrow, values.len());
        }
        let mut b = StrBuilder::new(pool, VarType::Variant, values.len());
        for v in values {
            match v {
                Some(v) => b.push_grow(pool, v),
                None => b.push_null(),
            }
        }
        let variant = b.finish();
        let rows: Vec<u32> = (0..values.len() as u32).collect();
        let array = self.decoded(&variant, node, path, &rows, pool);
        variant.reclaim(pool);
        array
    }

    /// `variant`'s `rows` read into `node`'s type, each one that does not fit refused.
    fn decoded(&mut self, variant: &Column, node: &Node, path: &str, rows: &[u32], pool: &mut BufferPool) -> ArrayRef {
        let (typed, failed) = tag::decode_column(variant, &node.ty, self.now, pool);
        for (row, value) in failed {
            if let Ok(i) = rows.binary_search(&(row as u32)) {
                let said = match value {
                    Some(value) => format!("{path} is {value}, which is no {}", node.ty),
                    None => format!("{path} holds no {}", node.ty),
                };
                self.refuse(i, said);
            }
        }
        let array = direct(&typed, node, rows);
        typed.reclaim(pool);
        array
    }
}

/// Whether a column of `col` is written as `record` as it is: the same type, a struct's openness aside, which the
/// column does not hold.
fn fits(col: &DataType, record: &DataType) -> bool {
    match (col, record) {
        (DataType::Struct(a, _), DataType::Struct(b, _)) => {
            a.len() == b.len() && a.iter().zip(b).all(|((m, x), (n, y))| m == n && fits(x, y))
        }
        (DataType::Array(a), DataType::Array(b)) => fits(a, b),
        _ => col == record,
    }
}

/// `rows` of `col`, a column that `fits` `node`.
fn direct(col: &Column, node: &Node, rows: &[u32]) -> ArrayRef {
    if col.is_unbound() {
        return new_null_array(&node.arrow, rows.len());
    }
    if let Some(value) = col.constant_value() {
        return repeated(value, rows.len());
    }
    let valid = |r: &u32| col.is_valid(*r as usize);
    match &node.kind {
        Kind::Null => Arc::new(NullArray::new(rows.len())),
        Kind::Bool => {
            let b = col.bool().expect("a bool column");
            Arc::new(
                rows.iter()
                    .map(|r| valid(r).then(|| bits::get_row(b.values, *r as usize)))
                    .collect::<BooleanArray>(),
            )
        }
        Kind::Int | Kind::Time => {
            let f = match col.data_type() {
                DataType::Duration => col.duration(),
                DataType::DateTime => col.datetime(),
                _ => col.i64(),
            }
            .expect("an 8-byte integer column");
            let values = rows.iter().map(|r| valid(r).then(|| f.values[*r as usize]));
            match node.kind {
                Kind::Time => Arc::new(values.collect::<TimestampMicrosecondArray>().with_timezone("UTC")),
                _ => Arc::new(values.collect::<Int64Array>()),
            }
        }
        Kind::Float => {
            let f = col.f64().expect("an f64 column");
            Arc::new(
                rows.iter()
                    .map(|r| Some(f.values[*r as usize]).filter(|v| valid(r) && v.is_finite()))
                    .collect::<Float64Array>(),
            )
        }
        Kind::Text => {
            let v = col.var().expect("a text column");
            Arc::new(
                rows.iter()
                    .map(|r| valid(r).then(|| String::from_utf8_lossy(v.get(*r as usize))))
                    .collect::<StringArray>(),
            )
        }
        Kind::Binary => {
            let v = col.var().expect("a bytes column");
            Arc::new(
                rows.iter()
                    .map(|r| valid(r).then(|| v.get(*r as usize)))
                    .collect::<BinaryArray>(),
            )
        }
        Kind::Variant => {
            let v = col.variant().expect("a variant column");
            json_text(rows, |r, doc| valid(r) && json::write_variant(v.get(*r as usize), doc))
        }
        Kind::List(elem, field) => {
            let l = col.list().expect("a list column");
            let mut items = Vec::new();
            let mut lens = Vec::with_capacity(rows.len());
            for r in rows {
                let (lo, hi) = match valid(r) {
                    true => (l.offsets[*r as usize], l.offsets[*r as usize + 1]),
                    false => (0, 0),
                };
                items.extend(lo..hi);
                lens.push((hi - lo) as usize);
            }
            Arc::new(ListArray::new(
                field.clone(),
                OffsetBuffer::from_lengths(lens),
                direct(l.child, elem, &items),
                nulls(rows.iter().map(valid).collect()),
            ))
        }
        Kind::Struct(s) => {
            let mut children: Vec<ArrayRef> = s
                .fields
                .iter()
                .map(|(name, node)| match col.field(name) {
                    Some(c) => direct(c, node, rows),
                    None => new_null_array(&node.arrow, rows.len()),
                })
                .collect();
            if s.open {
                let unclaimed = col.unclaimed_fields().and_then(|u| u.variant().ok());
                let own = rows.iter().map(|r| {
                    let u = unclaimed.as_ref().filter(|u| u.validity.is_valid(*r as usize))?;
                    Some(u.get(*r as usize))
                });
                children.push(rest(own, |key| s.names(key)));
            }
            s.array(children)
        }
    }
}

/// One string row per item: the JSON `write` writes for it, null where it writes nothing.
fn json_text<T>(items: impl IntoIterator<Item = T>, mut write: impl FnMut(T, &mut Vec<u8>) -> bool) -> ArrayRef {
    let mut text = StringBuilder::new();
    let mut doc = Vec::new();
    for item in items {
        doc.clear();
        match write(item, &mut doc) {
            true => text.append_value(std::str::from_utf8(&doc).expect("JSON is UTF-8")),
            false => text.append_null(),
        }
    }
    Arc::new(text.finish())
}

/// An open struct's `rest` column: each row's unclaimed object minus the keys `claimed` names.
fn rest<'v>(own: impl IntoIterator<Item = Option<&'v [u8]>>, claimed: impl Fn(&str) -> bool) -> ArrayRef {
    json_text(own, |own, doc| match own.map(variant::read) {
        Some(Value::Object(entries)) => json::write_members_object(entries, &claimed, doc),
        _ => false,
    })
}

/// `value` on each of `n` rows.
fn repeated(value: &Scalar, n: usize) -> ArrayRef {
    match value {
        Scalar::Bool(v) => Arc::new(BooleanArray::from(vec![*v; n])),
        Scalar::I64(v) | Scalar::Duration(v) => Arc::new(Int64Array::from(vec![*v; n])),
        Scalar::DateTime(v) => Arc::new(TimestampMicrosecondArray::from(vec![*v; n]).with_timezone("UTC")),
        Scalar::F64(v) => Arc::new(Float64Array::from(vec![Some(*v).filter(|v| v.is_finite()); n])),
        Scalar::Str(v) => Arc::new(StringArray::from(vec![v.as_str(); n])),
        Scalar::Bytes(v) => Arc::new(BinaryArray::from(vec![v.as_slice(); n])),
        Scalar::Variant(v) => json_text(0..n, |_, doc| json::write_variant(v, doc)),
    }
}

fn nulls(valid: Vec<bool>) -> Option<NullBuffer> {
    valid.contains(&false).then(|| NullBuffer::from(valid))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(fields: &[(&str, DataType)], open: bool) -> DataType {
        DataType::Struct(fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect(), open)
    }

    #[test]
    fn a_record_parquet_cannot_hold_is_refused() {
        let clash = st(&[("o", st(&[(REST, DataType::Str)], true))], false);
        for (ty, said) in [
            (DataType::I64, "the record is of type i64, and Parquet writes a struct"),
            (
                clash,
                "o.rest is a field of an open struct, whose unclaimed keys the column rest holds",
            ),
            (
                st(&[], false),
                "the record is a struct with no fields, which Parquet cannot write",
            ),
        ] {
            assert_eq!(Schema::new(&ty).unwrap_err(), said);
        }
        // A closed struct may name a field `rest`: nothing else claims the name.
        Schema::new(&st(&[(REST, DataType::Str)], false)).unwrap();
    }

    fn raw_schema() -> Schema {
        Schema::new(&st(&[("_raw", DataType::Str)], false)).unwrap()
    }

    fn rows(schema: &Schema, texts: &[&str]) -> Vec<Columns> {
        let chunk = Arc::new(Chunk::raw(schema, texts.iter().copied()));
        (0..texts.len() as u32)
            .map(|row| Columns {
                chunk: chunk.clone(),
                row,
            })
            .collect()
    }

    /// The file's row groups and the `_raw` text of every row, read back through Parquet's reader.
    fn read_back(name: &str, file: &[u8]) -> (usize, Vec<String>) {
        use arrow_array::cast::AsArray;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let path = std::env::temp_dir().join(format!("smeltr-parquet-{}-{name}.parquet", std::process::id()));
        std::fs::write(&path, file).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap();
        let groups = builder.metadata().num_row_groups();
        let mut texts = Vec::new();
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            let col = batch.column_by_name("_raw").unwrap().as_string::<i32>();
            texts.extend(col.iter().map(|v| v.unwrap().to_string()));
        }
        std::fs::remove_file(&path).unwrap();
        (groups, texts)
    }

    #[test]
    fn a_file_streams_out_a_row_group_at_a_time() {
        let schema = raw_schema();
        let mut w = FileWriter::open(&schema, Compression::Zstd);
        assert!(w.take_row_group().is_empty());

        let first = rows(&schema, &["a", "b"]);
        w.write(&first);
        assert_eq!(w.rows(), 2);
        assert!(w.in_progress_bytes() > 0);
        let estimate = w.bytes_written();
        let mut file = w.take_row_group();
        assert!(!file.is_empty());
        assert_eq!(w.in_progress_bytes(), 0);
        assert_eq!(w.bytes_written(), file.len());
        assert!(estimate > 0);
        assert!(w.take_row_group().is_empty());

        let second = rows(&schema, &["c"]);
        w.write(second.iter().chain(&first));
        assert_eq!(w.rows(), 5);
        let (rest, metadata) = w.finish();
        file.extend(rest);
        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.file_metadata().num_rows(), 5);
        assert_eq!(
            read_back("two-groups", &file),
            (2, ["a", "b", "c", "a", "b"].map(String::from).to_vec())
        );
    }

    #[test]
    fn write_makes_one_file_of_one_row_group() {
        let schema = raw_schema();
        let a = rows(&schema, &["x", "y"]);
        let b = rows(&schema, &["z"]);
        let file = write(b.iter().chain(&a), Compression::Snappy);
        assert_eq!(
            read_back("one-group", &file),
            (1, ["z", "x", "y"].map(String::from).to_vec())
        );
        assert!(write(std::iter::empty(), Compression::None).is_empty());
    }

    #[test]
    fn a_file_written_with_field_ids_carries_them() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let schema = raw_schema();
        let mut field = schema.arrow().field(0).clone();
        field.set_metadata([(parquet::arrow::PARQUET_FIELD_ID_META_KEY, "7")]);
        let labelled = schema.clone().with_written(Arc::new(ArrowSchema::new(vec![field])));
        let own = rows(&schema, &["a", "b"]);
        assert!(labelled.matches(&own[0]));
        let mut w = FileWriter::open(&labelled, Compression::None);
        w.write(&own);
        let (file, _) = w.finish();
        let path = std::env::temp_dir().join(format!("smeltr-parquet-{}-ids.parquet", std::process::id()));
        std::fs::write(&path, &file).unwrap();
        let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let column = reader.metadata().file_metadata().schema_descr().column(0);
        assert_eq!(column.self_type().get_basic_info().id(), 7);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            read_back("field-ids", &file),
            (1, ["a", "b"].map(String::from).to_vec())
        );
    }

    #[test]
    fn nested_columns_and_list_elements_carry_their_field_ids() {
        use crate::io::iceberg::schema::{table_schema, with_field_ids};
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let record = st(
            &[
                ("o", st(&[("a", DataType::I64)], false)),
                ("l", DataType::Array(Box::new(DataType::Str))),
            ],
            false,
        );
        let schema = Schema::new(&record).unwrap();
        let table = table_schema(&record, None, 0).unwrap().schema;
        let labelled = schema.clone().with_written(with_field_ids(schema.arrow(), &table));

        let Arrow::Struct(o) = schema.arrow().field(0).data_type().clone() else {
            panic!("o is a struct")
        };
        let Arrow::List(item) = schema.arrow().field(1).data_type().clone() else {
            panic!("l is a list")
        };
        let o: ArrayRef = Arc::new(StructArray::new(o, vec![Arc::new(Int64Array::from(vec![1]))], None));
        let l: ArrayRef = Arc::new(ListArray::new(
            item,
            OffsetBuffer::from_lengths([2]),
            Arc::new(StringArray::from(vec!["x", "y"])),
            None,
        ));
        let batch = RecordBatch::try_new(schema.arrow().clone(), vec![o, l]).unwrap();
        let row = Columns {
            chunk: Arc::new(Chunk::new(batch, Vec::new())),
            row: 0,
        };
        assert!(labelled.matches(&row));
        let mut w = FileWriter::open(&labelled, Compression::None);
        w.write([&row]);
        let (file, _) = w.finish();

        let path = std::env::temp_dir().join(format!("smeltr-parquet-{}-nested-ids.parquet", std::process::id()));
        std::fs::write(&path, &file).unwrap();
        let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let descr = reader.metadata().file_metadata().schema_descr();
        let top: Vec<i32> = descr
            .root_schema()
            .get_fields()
            .iter()
            .map(|f| f.get_basic_info().id())
            .collect();
        // o is 1, l is 2; o.a is 3, l's element 4.
        assert_eq!(top, [1, 2]);
        let leaves: Vec<i32> = (0..descr.num_columns())
            .map(|i| descr.column(i).self_type().get_basic_info().id())
            .collect();
        assert_eq!(leaves, [3, 4]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn columns_match_the_schema_they_were_cut_to() {
        let schema = raw_schema();
        let own = rows(&schema, &["a"]);
        assert!(schema.matches(&own[0]));
        // The same record, a second schema: equal, not shared.
        assert!(raw_schema().matches(&own[0]));
        let other = Schema::new(&st(&[("text", DataType::Str)], false)).unwrap();
        assert!(!other.matches(&own[0]));
        assert!(!schema.matches(&rows(&other, &["b"])[0]));
    }

    #[test]
    fn a_file_written_under_physical_names_carries_them_at_every_depth() {
        use crate::io::delta::schema::{Mapping, physical, table_schema};
        use arrow_array::cast::AsArray;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let record = st(
            &[
                ("o", st(&[("a", DataType::I64)], false)),
                ("l", DataType::Array(Box::new(st(&[("b", DataType::Str)], false)))),
            ],
            false,
        );
        let schema = Schema::new(&record).unwrap();
        let table = table_schema(&record, None, Mapping::Name { max_column_id: 0 })
            .unwrap()
            .schema;
        let written = physical(schema.arrow(), &table);
        let labelled = schema.clone().with_written(written.clone());

        let Arrow::Struct(o) = schema.arrow().field(0).data_type().clone() else {
            panic!("o is a struct")
        };
        let Arrow::List(item) = schema.arrow().field(1).data_type().clone() else {
            panic!("l is a list")
        };
        let Arrow::Struct(element) = item.data_type().clone() else {
            panic!("l holds structs")
        };
        let o: ArrayRef = Arc::new(StructArray::new(o, vec![Arc::new(Int64Array::from(vec![7]))], None));
        let elements = StructArray::new(element, vec![Arc::new(StringArray::from(vec!["x", "y"]))], None);
        let l: ArrayRef = Arc::new(ListArray::new(
            item,
            OffsetBuffer::from_lengths([2]),
            Arc::new(elements),
            None,
        ));
        let batch = RecordBatch::try_new(schema.arrow().clone(), vec![o, l]).unwrap();
        let row = Columns {
            chunk: Arc::new(Chunk::new(batch, Vec::new())),
            row: 0,
        };
        assert!(labelled.matches(&row));
        let mut w = FileWriter::open(&labelled, Compression::None);
        w.write([&row]);
        let (file, _) = w.finish();

        let reader = ParquetRecordBatchReaderBuilder::try_new(tokio_util::bytes::Bytes::from(file)).unwrap();
        let read = reader.build().unwrap().next().unwrap().unwrap();
        // The file's schema is the physical one, field ids aside, which the reader carries as metadata.
        let names = |s: &ArrowSchema| {
            format!(
                "{:?}",
                s.fields().iter().map(|f| f.data_type().to_string()).collect::<Vec<_>>()
            )
        };
        assert_eq!(names(read.schema().as_ref()), names(written.as_ref()));
        let physical_o = table.fields[0].physical_name();
        assert_ne!(physical_o, "o");
        let o = read.column_by_name(physical_o).unwrap().as_struct();
        assert_eq!(o.column(0).as_primitive::<arrow_array::types::Int64Type>().value(0), 7);
        let l = read
            .column_by_name(table.fields[1].physical_name())
            .unwrap()
            .as_list::<i32>();
        let texts = l.value(0);
        assert_eq!(
            texts
                .as_struct()
                .column(0)
                .as_string::<i32>()
                .iter()
                .flatten()
                .collect::<Vec<_>>(),
            ["x", "y"]
        );
    }
}
