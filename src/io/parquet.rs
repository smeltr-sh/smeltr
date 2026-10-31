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
//! - A variant is `Utf8` holding the value's JSON text, so a string variant keeps its quotes. A chunk keeps each
//!   variant's own bytes beside it too, for a table that holds a variant as Parquet's variant (`parquet_variant`).
//! - An open struct has one more `Utf8` child, `rest`: a JSON object of the keys its fields do not name, null when
//!   there are none.
//! - A float JSON cannot hold is null, as the JSON writer leaves it out.
//!
//! Where the record types a place wider than the column holding it (the tag field, met with what the pipeline writes
//! there), each row is read into the record's type by the declared-type table (`tag::decode_column`). A row that does
//! not fit is marked in the chunk, and refused alone where it is delivered.
//!
//! A table may hold a leaf narrower than the record types it (`narrows`): an `i64` in an 8, 16 or 32-bit integer, an
//! `f64` in a 32-bit float, a `datetime` in a date (its UTC day) or a timestamp without a zone. A route's rows are cut
//! once, to the record's own types, for every destination on it; a table's file converts each value as it writes it,
//! a float rounded, and a row whose value the column cannot hold is refused alone where it is delivered
//! (`Schema::unfit`).

use std::sync::{Arc, Mutex};

use arrow_array::builder::StringBuilder;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, ListArray, NullArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType as Arrow, Field, Fields, Schema as ArrowSchema, TimeUnit};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use parquet::basic::LogicalType;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType, TypePtr};

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
    /// The schema a file is written with, when it differs from `arrow` in names and metadata, a table format's field
    /// ids or physical column names, or in a leaf a table holds narrower. Rows are cut to and matched against `arrow`.
    written: Option<Arc<ArrowSchema>>,
    /// Whether `written` holds a leaf narrower than `arrow`, so a row's values are checked against it.
    narrowed: bool,
    /// What each of `written`'s root fields takes where the record does not write it; null for one beyond.
    defaults: Arc<[WriteDefault]>,
    /// The Parquet logical type of each column of `written` Arrow has none for, by its field id.
    annotations: Arc<[(i32, LogicalType)]>,
    /// Columns the destination computes, written after the record's own.
    computed: Vec<Arc<dyn Computed>>,
    /// The record's columns that hold a partition's values, each by the partition field's name and its index.
    partition_columns: Vec<(String, usize)>,
    /// The record's variants as their own bytes, and their Arrow schema, when it has any (`variant_bytes`).
    variants: Option<(Arc<Node>, Arc<ArrowSchema>)>,
}

/// What a field of a written schema takes in each row where the record does not write it.
#[derive(Clone, Debug)]
pub enum WriteDefault {
    /// A field the record does not write: one row of the field's type, which every row takes.
    Value(ArrayRef),
    /// A struct the record writes: its fields', in the written schema's order.
    Struct(Vec<WriteDefault>),
    /// A list the record writes: its element's.
    List(Box<WriteDefault>),
    /// A leaf the record writes.
    Written,
}

/// A column a destination computes for each row it writes, after the record's own, such as a table's identity.
pub trait Computed: std::fmt::Debug + Send + Sync {
    /// The column as the file carries it.
    fn field(&self) -> Arc<Field>;
    /// Why `columns`' row can take no value. Each row admitted is filled at most once, after it is admitted.
    fn admit(&self, columns: &Columns) -> Result<(), String>;
    /// The column's values for `batch`'s rows, each admitted, `batch` under the record's own names.
    fn fill(&self, batch: &RecordBatch) -> ArrayRef;
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
    /// A variant as its own bytes (`core::variant`), in the tree of a record's variants (`variant_bytes`).
    VariantBytes,
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
    /// The record's variants as their own bytes, beside `batch`'s JSON text, for a table that holds them as a variant.
    variants: Option<RecordBatch>,
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
        let variants = variant_bytes(&root).map(|v| {
            let Arrow::Struct(fields) = &v.arrow else {
                unreachable!("the root is a struct")
            };
            let schema = Arc::new(ArrowSchema::new(fields.clone()));
            (Arc::new(v), schema)
        });
        Ok(Schema {
            root: Arc::new(root),
            arrow,
            written: None,
            narrowed: false,
            defaults: Arc::new([]),
            annotations: Arc::new([]),
            computed: Vec::new(),
            partition_columns: Vec::new(),
            variants,
        })
    }

    /// This schema, its files written with `written`: the same fields, at any depth, renamed or with metadata of their
    /// own, such as Iceberg's field ids or Delta's physical names, and a leaf a table holds narrower of that type
    /// (`narrows`). A struct may end with fields the record lacks, such as a table's columns it does not write, which
    /// every row writes null.
    pub fn with_written(self, written: Arc<ArrowSchema>) -> Schema {
        let Some(Arrow::Struct(fields)) = narrowed(&self.root, &Arrow::Struct(written.fields().clone())) else {
            panic!("a written schema is the record's own, renamed, with metadata added or a leaf narrowed");
        };
        Schema {
            narrowed: narrows_a_leaf(&Arrow::Struct(self.arrow.fields().clone()), &Arrow::Struct(fields)),
            written: Some(written),
            ..self
        }
    }

    /// This schema, a field its record does not write taking `defaults`, one for each of the written schema's root
    /// fields, rather than null.
    pub fn with_defaults(self, defaults: Vec<WriteDefault>) -> Schema {
        Schema {
            defaults: defaults.into(),
            ..self
        }
    }

    /// This schema, its files' column of each field id in `annotations` written as that Parquet logical type: one
    /// Arrow has none for, such as a geometry, or a variant's group.
    pub fn with_annotations(self, annotations: Vec<(i32, LogicalType)>) -> Schema {
        Schema {
            annotations: annotations.into(),
            ..self
        }
    }

    /// This schema, its chunks cut without their variants' own bytes, for a route on which no destination holds a
    /// variant as Parquet's variant.
    pub fn without_variant_bytes(self) -> Schema {
        Schema { variants: None, ..self }
    }

    /// Whether its chunks carry their variants' own bytes.
    pub fn cuts_variant_bytes(&self) -> bool {
        self.variants.is_some()
    }

    /// This schema, its files written with `computed` after the record's columns.
    pub fn with_computed(self, computed: Vec<Arc<dyn Computed>>) -> Schema {
        Schema { computed, ..self }
    }

    /// This schema, its record's columns `columns` holding the partition's values, each named by its partition field.
    /// A file of a partition whose value is empty, which a Hive-style table reads as null, holds that column null.
    pub fn with_partition_columns(self, columns: Vec<(String, usize)>) -> Schema {
        Schema {
            partition_columns: columns,
            ..self
        }
    }

    /// Admits `columns`' row to every computed column: Err is why one can give it no value.
    pub fn admit(&self, columns: &Columns) -> Result<(), String> {
        self.computed.iter().try_for_each(|c| c.admit(columns))
    }

    /// Why `columns`' row cannot be written, when a value of it is one a column its table holds narrower cannot hold.
    pub fn unfit(&self, columns: &Columns) -> Option<String> {
        let written = self.written.as_ref().filter(|_| self.narrowed)?;
        let batch = &columns.chunk.batch;
        (batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .zip(written.fields()))
        .find_map(|((column, own), to)| unfit_at(column, to.data_type(), columns.row as usize, &|| own.name().into()))
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
}

impl Chunk {
    /// The rows `parts` name, each part a view of the batch and the batch rows read through it, ascending, in that
    /// order: a row's place in the chunk is its place in that sequence. A time read with no year takes it from `now`.
    pub fn cut(schema: &Schema, parts: &[(&RecordView<'_>, &[u32])], now: i64, pool: &mut BufferPool) -> Chunk {
        let mut batches = Vec::with_capacity(parts.len());
        let mut variants = Vec::new();
        let mut unfit = Vec::new();
        let mut at = 0;
        for (view, rows) in parts.iter().filter(|(_, rows)| !rows.is_empty()) {
            let mut cut = Cut {
                rows,
                now,
                unfit: Vec::new(),
            };
            batches.push(cut.batch(view, &schema.root, &schema.arrow, pool));
            unfit.extend(cut.unfit.into_iter().map(|(i, said)| (at + i, said)));
            at += rows.len() as u32;
            if let Some((root, arrow)) = &schema.variants {
                // A row is refused by what the JSON cut said of it.
                let mut cut = Cut {
                    rows,
                    now,
                    unfit: Vec::new(),
                };
                variants.push(cut.batch(view, root, arrow, pool));
            }
        }
        let concat = |schema: &Arc<ArrowSchema>, mut batches: Vec<RecordBatch>| match batches.len() {
            1 => batches.pop().expect("one"),
            _ => arrow_select::concat::concat_batches(schema, &batches).expect("batches of one schema"),
        };
        let variants = (schema.variants.as_ref()).map(|(_, arrow)| concat(arrow, variants));
        Chunk::new(concat(&schema.arrow, batches), variants, unfit)
    }

    fn new(batch: RecordBatch, variants: Option<RecordBatch>, unfit: Vec<(u32, String)>) -> Chunk {
        let mut sizes = vec![0; batch.num_rows()];
        for column in batch.columns() {
            add_sizes(column, &mut sizes);
        }
        let bytes = batch.get_array_memory_size() + variants.as_ref().map_or(0, |v| v.get_array_memory_size());
        Chunk {
            batch,
            variants,
            unfit,
            sizes,
            bytes,
        }
    }

    /// The record of `{_raw: str}` for rows no pipeline ran on: each one's text.
    pub fn raw<'t>(schema: &Schema, texts: impl IntoIterator<Item = &'t str>) -> Chunk {
        let text: StringArray = texts.into_iter().map(Some).collect();
        let batch = RecordBatch::try_new(schema.arrow.clone(), vec![Arc::new(text)]).expect("the `{_raw: str}` record");
        Chunk::new(batch, None, Vec::new())
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
        let one = |batch: &RecordBatch| {
            arrow_select::interleave::interleave_record_batch(&[batch], &[(0, self.row as usize)])
                .expect("a row of its own chunk")
        };
        let unfit = self
            .chunk
            .unfit(self.row)
            .map(|said| (0, said.to_string()))
            .into_iter()
            .collect();
        Columns {
            chunk: Arc::new(Chunk::new(
                one(&self.chunk.batch),
                self.chunk.variants.as_ref().map(one),
                unfit,
            )),
            row: 0,
        }
    }
}

/// Whether a table may hold a leaf the record types `record` as `to`, each value converted as its file is written.
pub fn narrows(record: &DataType, to: &Arrow) -> bool {
    matches!(
        (record, to),
        (DataType::I64, Arrow::Int8 | Arrow::Int16 | Arrow::Int32)
            | (DataType::F64, Arrow::Float32)
            | (
                DataType::DateTime,
                Arrow::Date32 | Arrow::Timestamp(TimeUnit::Microsecond, None)
            )
    )
}

/// `node`'s type with each leaf `written` holds narrower (`narrows`) of that type. `None` when `written` is not the
/// node's type, renamed, with metadata added, a leaf narrowed or fields ending a struct that the node lacks.
fn narrowed(node: &Node, written: &Arrow) -> Option<Arrow> {
    match (&node.kind, written) {
        (Kind::Struct(s), Arrow::Struct(w)) if w.len() >= s.arrow.len() => {
            let fields = (s.arrow.iter().zip(w).enumerate())
                .map(|(i, (ours, w))| {
                    let t = match s.fields.get(i) {
                        Some((_, child)) => narrowed(child, w.data_type())?,
                        // `rest`
                        None => (w.data_type() == ours.data_type()).then(|| ours.data_type().clone())?,
                    };
                    Some(ours.as_ref().clone().with_data_type(t))
                })
                .collect::<Option<Fields>>()?;
            Some(Arrow::Struct(fields))
        }
        (Kind::List(element, field), Arrow::List(w)) => {
            let t = narrowed(element, w.data_type())?;
            Some(Arrow::List(Arc::new(field.as_ref().clone().with_data_type(t))))
        }
        (Kind::Struct(_) | Kind::List(..), _) => None,
        (Kind::Variant, w @ Arrow::Struct(fields)) if is_variant_group(fields) => Some(w.clone()),
        (_, w) if *w == node.arrow => Some(node.arrow.clone()),
        (_, w) if narrows(&node.ty, w) => Some(w.clone()),
        _ => None,
    }
}

/// Whether `written`, which `narrowed` made of `ours`, holds a leaf narrower than ours, which a row may not fit. A
/// variant held as Parquet's variant group always fits.
fn narrows_a_leaf(ours: &Arrow, written: &Arrow) -> bool {
    match (ours, written) {
        (Arrow::Utf8, Arrow::Struct(fields)) if is_variant_group(fields) => false,
        (Arrow::Struct(o), Arrow::Struct(w)) => {
            (o.iter().zip(w)).any(|(o, w)| narrows_a_leaf(o.data_type(), w.data_type()))
        }
        (Arrow::List(o), Arrow::List(w)) => narrows_a_leaf(o.data_type(), w.data_type()),
        (o, w) => o != w,
    }
}

/// Parquet's variant group, unshredded, which a table holds a variant in: its metadata and its value.
pub fn variant_group() -> Arrow {
    Arrow::Struct(Fields::from(vec![
        Field::new("metadata", Arrow::Binary, false),
        Field::new("value", Arrow::Binary, false),
    ]))
}

/// Whether `fields` are Parquet's variant group's (`variant_group`).
fn is_variant_group(fields: &Fields) -> bool {
    let names: Vec<(&str, &Arrow)> = fields.iter().map(|f| (f.name().as_str(), f.data_type())).collect();
    names == [("metadata", &Arrow::Binary), ("value", &Arrow::Binary)]
}

/// `batch` under `schema`, which `narrowed` matches to its own, a field it lacks taking its `defaults`, and a variant
/// it holds as Parquet's variant taking its bytes from `variants`, the chunks' own. No other value of the batch's is
/// copied.
fn relabel(
    batch: &RecordBatch,
    variants: Option<&RecordBatch>,
    schema: &Arc<ArrowSchema>,
    defaults: &[WriteDefault],
) -> RecordBatch {
    let variants = variants.map(|v| Arc::new(StructArray::from(v.clone())) as ArrayRef);
    let own = Own {
        columns: batch.columns(),
        names: batch.schema_ref().fields(),
        variants: variants.as_ref(),
    };
    let columns = relabelled_fields(own, schema.fields(), batch.num_rows(), defaults);
    RecordBatch::try_new(schema.clone(), columns)
        .expect("a written schema the rows' own but for names, metadata and narrowed leaves")
}

/// A struct's columns being relabelled, and the struct of its variants' bytes, when it holds any.
#[derive(Clone, Copy)]
struct Own<'a> {
    columns: &'a [ArrayRef],
    names: &'a Fields,
    variants: Option<&'a ArrayRef>,
}

impl<'a> Own<'a> {
    /// The bytes of the variants under column `i`.
    fn variants(&self, i: usize) -> Option<&'a ArrayRef> {
        use arrow_array::cast::AsArray;
        self.variants?.as_struct().column_by_name(self.names[i].name())
    }
}

fn relabelled(array: &ArrayRef, variants: Option<&ArrayRef>, to: &Arrow, default: Option<&WriteDefault>) -> ArrayRef {
    use arrow_array::cast::AsArray;
    if array.data_type() == to {
        return array.clone();
    }
    match to {
        // A variant, which the record writes as JSON text.
        Arrow::Struct(fields) if *array.data_type() == Arrow::Utf8 => {
            let variants = variants.expect("a chunk of a record with a variant keeps its bytes");
            parquet_variant(variants.as_binary::<i32>(), fields)
        }
        Arrow::Struct(fields) => {
            let parent = array
                .as_struct_opt()
                .expect("a written schema the rows' own but for names, metadata and narrowed leaves");
            let defaults = match default {
                Some(WriteDefault::Struct(defaults)) => &defaults[..],
                _ => &[],
            };
            let own = Own {
                columns: parent.columns(),
                names: parent.fields(),
                variants,
            };
            let children = relabelled_fields(own, fields, parent.len(), defaults);
            Arc::new(StructArray::new(fields.clone(), children, parent.nulls().cloned()))
        }
        Arrow::List(field) => {
            let list = array
                .as_list_opt::<i32>()
                .expect("a written schema the rows' own but for names, metadata and narrowed leaves");
            let element = match default {
                Some(WriteDefault::List(element)) => Some(&**element),
                _ => None,
            };
            let variants = variants.map(|v| v.as_list::<i32>().values());
            let values = relabelled(list.values(), variants, field.data_type(), element);
            Arc::new(ListArray::new(
                field.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            ))
        }
        // `Schema::unfit` refused every row whose value this column cannot hold.
        to => narrow_leaf(array, to),
    }
}

/// `columns` as `fields`, and for each field after theirs a column of `rows` of its default, or null.
fn relabelled_fields(own: Own<'_>, fields: &Fields, rows: usize, defaults: &[WriteDefault]) -> Vec<ArrayRef> {
    (fields.iter().enumerate())
        .map(|(i, field)| match (own.columns.get(i), defaults.get(i)) {
            (Some(column), default) => relabelled(column, own.variants(i), field.data_type(), default),
            (None, Some(WriteDefault::Value(value))) if value.is_valid(0) => {
                let first = arrow_array::UInt32Array::from(vec![0; rows]);
                arrow_select::take::take(value, &first, None).expect("a default's one row")
            }
            (None, _) => new_null_array(field.data_type(), rows),
        })
        .collect()
}

/// `variants`, each its own bytes, as Parquet's variant group of `fields`: null where a variant is null.
fn parquet_variant(variants: &BinaryArray, fields: &Fields) -> ArrayRef {
    let mut metadata = arrow_array::builder::BinaryBuilder::new();
    let mut value = arrow_array::builder::BinaryBuilder::new();
    let mut valid = Vec::with_capacity(variants.len());
    for v in variants {
        match v.filter(|v| !matches!(variant::read(v), Value::Null)) {
            Some(v) => {
                let (m, b) = crate::io::parquet_variant::encode(v);
                metadata.append_value(m);
                value.append_value(b);
                valid.push(true);
            }
            // Under a null, which no reader reads.
            None => {
                metadata.append_value([]);
                value.append_value([]);
                valid.push(false);
            }
        }
    }
    let children: Vec<ArrayRef> = vec![Arc::new(metadata.finish()), Arc::new(value.finish())];
    Arc::new(StructArray::new(fields.clone(), children, nulls(valid)))
}

/// Why value `i` of `array` cannot be written as `to`, `written`'s type for it, naming it by `path`, made only then.
fn unfit_at(array: &ArrayRef, to: &Arrow, i: usize, path: &dyn Fn() -> String) -> Option<String> {
    use arrow_array::cast::AsArray;
    if array.is_null(i) {
        return None;
    }
    match (array.data_type(), to) {
        (Arrow::Struct(own), Arrow::Struct(fields)) => (array.as_struct().columns().iter().zip(own).zip(fields))
            .find_map(|((child, own), to)| unfit_at(child, to.data_type(), i, &|| at(&path(), own.name()))),
        (Arrow::List(_), Arrow::List(to)) => {
            let list = array.as_list::<i32>();
            let (items, offsets) = (list.values(), list.value_offsets());
            let path = || format!("{}[]", path());
            (offsets[i] as usize..offsets[i + 1] as usize).find_map(|e| unfit_at(items, to.data_type(), e, &path))
        }
        (own, to) if own == to => None,
        _ => misfit(array, i, to).map(|why| format!("{} {why}", path())),
    }
}

/// Why value `i` of `array`, a leaf of the record's, cannot be held by a column a table holds as `to` (`narrows`):
/// `is 300, which an 8-bit integer column cannot hold`. None when it can, and for a time, which every one can.
pub fn misfit(array: &ArrayRef, i: usize, to: &Arrow) -> Option<String> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type};
    let int = |fits: bool, width: &str| {
        let v = array.as_primitive::<Int64Type>().value(i);
        (!fits).then(|| format!("is {v}, which {width} integer column cannot hold"))
    };
    match to {
        Arrow::Int8 => int(
            i8::try_from(array.as_primitive::<Int64Type>().value(i)).is_ok(),
            "an 8-bit",
        ),
        Arrow::Int16 => int(
            i16::try_from(array.as_primitive::<Int64Type>().value(i)).is_ok(),
            "a 16-bit",
        ),
        Arrow::Int32 => int(
            i32::try_from(array.as_primitive::<Int64Type>().value(i)).is_ok(),
            "a 32-bit",
        ),
        Arrow::Float32 => {
            let v = array.as_primitive::<Float64Type>().value(i);
            (v.is_finite() && !(v as f32).is_finite())
                .then(|| format!("is {v:?}, beyond a 32-bit float column's range"))
        }
        _ => None,
    }
}

/// `array`'s values as `to`, a type a table holds narrower (`narrows`), a float rounded; a value the column cannot
/// hold (`misfit`) is null.
fn narrow_leaf(array: &ArrayRef, to: &Arrow) -> ArrayRef {
    use crate::ops::civil::MICROS_PER_DAY;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type, TimestampMicrosecondType};
    let held = |i: usize| !array.is_null(i) && misfit(array, i, to).is_none();
    let ints = || (0..array.len()).map(|i| held(i).then(|| array.as_primitive::<Int64Type>().value(i)));
    match to {
        Arrow::Int8 => Arc::new(ints().map(|v| v.map(|v| v as i8)).collect::<Int8Array>()),
        Arrow::Int16 => Arc::new(ints().map(|v| v.map(|v| v as i16)).collect::<Int16Array>()),
        Arrow::Int32 => Arc::new(ints().map(|v| v.map(|v| v as i32)).collect::<Int32Array>()),
        // Rounded: what a float column is.
        Arrow::Float32 => Arc::new(
            (0..array.len())
                .map(|i| held(i).then(|| array.as_primitive::<Float64Type>().value(i) as f32))
                .collect::<Float32Array>(),
        ),
        // Every day an i64 of microseconds reaches is an i32.
        Arrow::Date32 => Arc::new(
            (array.as_primitive::<TimestampMicrosecondType>().iter())
                .map(|v| v.map(|v| v.div_euclid(MICROS_PER_DAY) as i32))
                .collect::<Date32Array>(),
        ),
        Arrow::Timestamp(TimeUnit::Microsecond, None) => Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .clone()
                .with_timezone_opt(None::<String>),
        ),
        to => unreachable!("`with_written` narrows a leaf to no {to}"),
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
    defaults: Arc<[WriteDefault]>,
    /// Columns computed for each batch, and the file's schema with them, when there are any.
    computed: Vec<Arc<dyn Computed>>,
    file: Arc<ArrowSchema>,
    /// The record's columns this file holds null: those of a partition value that is empty.
    nulled: Vec<usize>,
}

/// `node` with each node whose field id `annotations` names taking its logical type.
fn annotate(node: &TypePtr, annotations: &[(i32, LogicalType)]) -> TypePtr {
    let info = node.get_basic_info();
    let named = (info.has_id())
        .then(|| annotations.iter().find(|(id, _)| *id == info.id()))
        .flatten();
    let (logical, converted) = match named {
        Some((_, logical)) => (Some(logical.clone()), parquet::basic::ConvertedType::NONE),
        None => (info.logical_type_ref().cloned(), info.converted_type()),
    };
    let id = info.has_id().then(|| info.id());
    let built = match node.as_ref() {
        ParquetType::GroupType { fields, .. } => {
            let fields = fields.iter().map(|f| annotate(f, annotations)).collect();
            let builder = ParquetType::group_type_builder(info.name())
                .with_fields(fields)
                .with_logical_type(logical)
                .with_converted_type(converted)
                .with_id(id);
            match info.has_repetition() {
                true => builder.with_repetition(info.repetition()).build(),
                false => builder.build(),
            }
        }
        ParquetType::PrimitiveType {
            physical_type,
            type_length,
            scale,
            precision,
            ..
        } => ParquetType::primitive_type_builder(info.name(), *physical_type)
            .with_repetition(info.repetition())
            .with_logical_type(logical)
            .with_converted_type(converted)
            .with_length(*type_length)
            .with_precision(*precision)
            .with_scale(*scale)
            .with_id(id)
            .build(),
    };
    Arc::new(built.expect("the converter's own node, its logical type one Parquet allows there"))
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

/// How a file's bytes are written: what a destination's config asks, or a table's own properties.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteOptions {
    pub codec: Compression,
    /// The codec's level, where it takes one (gzip, zstd); its default when `None` or out of its range.
    pub level: Option<i32>,
    /// Leaf columns, by their names in the written file, that carry a bloom filter, each with its false-positive
    /// probability.
    pub bloom_filters: Vec<(Vec<String>, f64)>,
}

impl From<Compression> for WriteOptions {
    fn from(codec: Compression) -> WriteOptions {
        WriteOptions {
            codec,
            level: None,
            bloom_filters: Vec::new(),
        }
    }
}

impl WriteOptions {
    fn properties(&self) -> WriterProperties {
        use parquet::basic::{Compression as Codec, GzipLevel, ZstdLevel};
        use parquet::schema::types::ColumnPath;
        let compression = match self.codec {
            Compression::None => Codec::UNCOMPRESSED,
            Compression::Gzip => Codec::GZIP(
                (self.level.and_then(|l| u32::try_from(l).ok()))
                    .and_then(|l| GzipLevel::try_new(l).ok())
                    .unwrap_or_default(),
            ),
            Compression::Snappy => Codec::SNAPPY,
            Compression::Lz4 => Codec::LZ4_RAW,
            Compression::Zstd => Codec::ZSTD(self.level.and_then(|l| ZstdLevel::try_new(l).ok()).unwrap_or_default()),
        };
        // Row groups end where `take_row_group` says, not where the writer's own limits fall.
        let mut props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(None);
        for (path, fpp) in &self.bloom_filters {
            let column = ColumnPath::new(path.clone());
            props = props
                .set_column_bloom_filter_enabled(column.clone(), true)
                .set_column_bloom_filter_fpp(column, *fpp);
        }
        props.build()
    }
}

impl FileWriter {
    /// A writer for rows of chunks cut to `schema`, its bytes written as `options` say.
    pub fn open(schema: &Schema, options: &WriteOptions) -> FileWriter {
        let own = schema.written.as_ref().unwrap_or(&schema.arrow);
        let file = if schema.computed.is_empty() {
            own.clone()
        } else {
            let fields: Vec<Arc<Field>> = (own.fields().iter().cloned())
                .chain(schema.computed.iter().map(|c| c.field()))
                .collect();
            Arc::new(ArrowSchema::new_with_metadata(fields, own.metadata().clone()))
        };
        FileWriter {
            written: schema.written.clone(),
            defaults: schema.defaults.clone(),
            computed: schema.computed.clone(),
            ..FileWriter::annotated(file, options, &schema.annotations)
        }
    }

    /// A file of the partition whose values are `values`, by field name.
    pub fn of_partition(schema: &Schema, options: &WriteOptions, values: &[(String, String)]) -> FileWriter {
        let empty = |name: &String| values.iter().any(|(n, v)| n == name && v.is_empty());
        FileWriter {
            nulled: (schema.partition_columns.iter())
                .filter(|(name, _)| empty(name))
                .map(|(_, at)| *at)
                .collect(),
            ..FileWriter::open(schema, options)
        }
    }

    fn on(schema: Arc<ArrowSchema>, options: &WriteOptions) -> FileWriter {
        FileWriter::annotated(schema, options, &[])
    }

    fn annotated(schema: Arc<ArrowSchema>, options: &WriteOptions, annotations: &[(i32, LogicalType)]) -> FileWriter {
        let props = options.properties();
        let sink = Sink::default();
        let mut writer_options = ArrowWriterOptions::new();
        if !annotations.is_empty() {
            let converted = (ArrowSchemaConverter::new().with_coerce_types(props.coerce_types()))
                .convert(&schema)
                .expect("a writable schema");
            let root = annotate(&converted.root_schema_ptr(), annotations);
            writer_options = writer_options.with_parquet_schema(SchemaDescriptor::new(root));
        }
        // Writing to memory fails only on a schema Parquet cannot hold, and `Schema::new` refuses those.
        let writer =
            ArrowWriter::try_new_with_options(sink.clone(), schema.clone(), writer_options.with_properties(props))
                .expect("a writable schema");
        FileWriter {
            writer,
            sink,
            rows: 0,
            written: None,
            defaults: Arc::new([]),
            computed: Vec::new(),
            file: schema,
            nulled: Vec::new(),
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
        let computed: Vec<ArrayRef> = self.computed.iter().map(|c| c.fill(&batch)).collect();
        if let Some(written) = &self.written {
            let variants: Option<Vec<&RecordBatch>> = chunks.iter().map(|c| c.variants.as_ref()).collect();
            let variants = variants
                .map(|v| arrow_select::interleave::interleave_record_batch(&v, &at).expect("chunks of one schema"));
            batch = relabel(&batch, variants.as_ref(), written, &self.defaults);
        }
        if !self.nulled.is_empty() {
            let mut columns = batch.columns().to_vec();
            for &at in &self.nulled {
                columns[at] = new_null_array(columns[at].data_type(), batch.num_rows());
            }
            batch = RecordBatch::try_new(batch.schema(), columns).expect("null columns of their fields' types");
        }
        if !computed.is_empty() {
            let mut columns = batch.columns().to_vec();
            columns.extend(computed);
            batch = RecordBatch::try_new(self.file.clone(), columns).expect("computed columns of their fields' types");
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
    let mut writer = FileWriter::on(first.chunk.batch.schema(), &codec.into());
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
    /// The view's rows as `root`, a struct whose columns `arrow` holds.
    fn batch(
        &mut self,
        view: &RecordView<'_>,
        root: &Node,
        arrow: &Arc<ArrowSchema>,
        pool: &mut BufferPool,
    ) -> RecordBatch {
        let Kind::Struct(s) = &root.kind else {
            unreachable!("Schema::new takes only a struct")
        };
        let inherited = vec![None; self.rows.len()];
        let children = self.object(&view.root, view, &inherited, s, "", pool);
        RecordBatch::try_new_with_options(
            arrow.clone(),
            children,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(self.rows.len())),
        )
        .expect("the columns are cut to the schema they are checked against")
    }

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
                (Some(record::Field::Object(child)), Kind::VariantBytes) => {
                    let values: BinaryArray = (self.rows.iter().zip(below()))
                        .map(|(&row, below)| {
                            object_tag(child, below, row as usize).map(|value| {
                                let mut b = Vec::new();
                                tag::encode(&value, &mut b);
                                b
                            })
                        })
                        .collect();
                    Arc::new(values)
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
        return match (&node.kind, value) {
            (Kind::VariantBytes, Scalar::Variant(v)) => Arc::new(BinaryArray::from(vec![v.as_slice(); rows.len()])),
            _ => repeated(value, rows.len()),
        };
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
        Kind::VariantBytes => {
            let v = col.variant().expect("a variant column");
            Arc::new(
                rows.iter()
                    .map(|r| valid(r).then(|| v.get(*r as usize)))
                    .collect::<BinaryArray>(),
            )
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

/// `node`'s variants as their own bytes: its tree down to each variant, which is `Binary`, and nothing else. None
/// where it holds no variant. An open struct's `rest` is JSON text however it is written, so it is left out too.
fn variant_bytes(node: &Node) -> Option<Node> {
    let (kind, arrow) = match &node.kind {
        Kind::Variant => (Kind::VariantBytes, Arrow::Binary),
        Kind::List(element, _) => {
            let element = variant_bytes(element)?;
            let field = Arc::new(Field::new_list_field(element.arrow.clone(), true));
            (Kind::List(Box::new(element), field.clone()), Arrow::List(field))
        }
        Kind::Struct(s) => {
            let fields: Vec<(String, Node)> = (s.fields.iter())
                .filter_map(|(name, child)| Some((name.clone(), variant_bytes(child)?)))
                .collect();
            if fields.is_empty() {
                return None;
            }
            let arrow: Fields = fields
                .iter()
                .map(|(name, n)| Field::new(name, n.arrow.clone(), true))
                .collect();
            let s = Struct {
                fields,
                open: false,
                arrow: arrow.clone(),
            };
            (Kind::Struct(s), Arrow::Struct(arrow))
        }
        _ => return None,
    };
    Some(Node {
        ty: node.ty.clone(),
        kind,
        arrow,
    })
}

/// What `o` holds on `row`, as one value: what `json::write_object` writes of it, each value of its own type. None
/// where that writes nothing.
fn object_tag<'v>(o: &'v record::Object<'_>, inherited: Option<&'v [u8]>, row: usize) -> Option<tag::Tag<'v>> {
    use std::borrow::Cow;
    let own = o.unclaimed_at(row, inherited);
    let own_object = own.filter(|v| variant::is_object(v));
    let mut fields: Vec<(Cow<'v, str>, tag::Tag<'v>)> = Vec::new();
    for (key, field) in &o.fields {
        let value = match field {
            record::Field::Leaf(col, _) => tag::row_tag(col, row),
            record::Field::Object(child) => object_tag(child, o.member(own_object, &key.raw), row),
        };
        fields.extend(value.map(|v| (Cow::Borrowed(key.raw.as_str()), v)));
    }
    if let Some(Value::Object(entries)) = own_object.map(variant::read) {
        let unclaimed = entries.filter(|(key, _)| !o.claims(key));
        fields.extend(unclaimed.filter_map(|(key, v)| Some((Cow::Borrowed(key), tag::decode(v)?))));
    }
    if fields.is_empty() {
        return own.filter(|v| !variant::is_object(v)).and_then(tag::decode);
    }
    Some(tag::Tag::Object(fields))
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
    fn a_file_carries_the_codec_and_the_bloom_filters_asked() {
        let schema = raw_schema();
        let options = WriteOptions {
            codec: Compression::Gzip,
            level: Some(9),
            bloom_filters: vec![(vec!["_raw".into()], 0.05), (vec!["nowhere".into()], 0.05)],
        };
        let mut w = FileWriter::open(&schema, &options);
        w.write(&rows(&schema, &["a", "b"]));
        let (_, metadata) = w.finish();
        let column = metadata.row_group(0).column(0);
        assert!(matches!(column.compression(), parquet::basic::Compression::GZIP(_)));
        assert!(column.bloom_filter_offset().is_some(), "a bloom filter for _raw");
        let mut plain = FileWriter::open(&schema, &Compression::Gzip.into());
        plain.write(&rows(&schema, &["a", "b"]));
        assert_eq!(plain.finish().1.row_group(0).column(0).bloom_filter_offset(), None);
    }

    #[test]
    fn a_file_streams_out_a_row_group_at_a_time() {
        let schema = raw_schema();
        let mut w = FileWriter::open(&schema, &Compression::Zstd.into());
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

    /// A column a table holds narrower takes each value converted as the file is written, at any depth, and a row
    /// whose value it cannot hold is unfit.
    #[test]
    fn a_column_a_table_holds_narrower_is_written_as_its_type() {
        use crate::core::column::Column;
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Date32Type, Float32Type, Int8Type, Int16Type};
        let mut pool = BufferPool::new();
        let mut n = Column::alloc(&mut pool, &DataType::I64, 3);
        n.i64_mut().unwrap().values.copy_from_slice(&[1, 40_000, 0]);
        n.i64_mut().unwrap().validity.set_valid(2, false);
        let mut f = Column::alloc(&mut pool, &DataType::F64, 3);
        f.f64_mut().unwrap().values.copy_from_slice(&[1.1, 2.0, 1e300]);
        let mut t = Column::alloc(&mut pool, &DataType::DateTime, 3);
        t.datetime_mut()
            .unwrap()
            .values
            .copy_from_slice(&[86_400_000_000 + 1, -1, 0]);
        let mut m = Column::alloc(&mut pool, &DataType::I64, 3);
        m.i64_mut().unwrap().values.copy_from_slice(&[-128, 127, 5]);
        let record = st(
            &[
                ("n", DataType::I64),
                ("f", DataType::F64),
                ("t", DataType::DateTime),
                ("ctx", st(&[("m", DataType::I64)], false)),
            ],
            false,
        );
        let schema = Schema::new(&record).unwrap();
        let ctx = Fields::from(vec![Field::new("m", Arrow::Int8, true)]);
        let written = Arc::new(ArrowSchema::new(vec![
            Field::new("n", Arrow::Int16, true),
            Field::new("f", Arrow::Float32, true),
            Field::new("t", Arrow::Date32, true),
            Field::new("ctx", Arrow::Struct(ctx), true),
        ]));
        let narrow = schema.clone().with_written(written.clone());
        let view = RecordView::from_named([("n", &n), ("f", &f), ("t", &t), ("ctx.m", &m)]);
        let chunk = Arc::new(Chunk::cut(&schema, &[(&view, &[0, 1, 2])], 0, &mut pool));
        for c in [n, f, t, m] {
            c.reclaim(&mut pool);
        }
        let rows: Vec<Columns> = (0..3)
            .map(|row| Columns {
                chunk: chunk.clone(),
                row,
            })
            .collect();

        assert!(narrow.matches(&rows[0]), "rows are cut to the record's own types");
        assert_eq!(narrow.unfit(&rows[0]), None);
        assert_eq!(
            narrow.unfit(&rows[1]).as_deref(),
            Some("n is 40000, which a 16-bit integer column cannot hold")
        );
        assert_eq!(
            narrow.unfit(&rows[2]).as_deref(),
            Some("f is 1e300, beyond a 32-bit float column's range")
        );
        assert_eq!(
            schema.unfit(&rows[1]),
            None,
            "a schema that narrows nothing checks nothing"
        );

        let mut w = FileWriter::open(&narrow, &Compression::None.into());
        w.write([&rows[0]]);
        let (file, _) = w.finish();
        let batch = ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            tokio_util::bytes::Bytes::from(file),
        )
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
        assert_eq!(batch.schema().fields(), written.fields());
        assert_eq!(batch.column(0).as_primitive::<Int16Type>().value(0), 1);
        assert_eq!(
            batch.column(1).as_primitive::<Float32Type>().value(0),
            1.1f32,
            "rounded"
        );
        assert_eq!(batch.column(2).as_primitive::<Date32Type>().value(0), 1, "its UTC day");
        let m = batch
            .column(3)
            .as_struct()
            .column(0)
            .as_primitive::<Int8Type>()
            .value(0);
        assert_eq!(m, -128);
    }

    /// A list's items a table holds narrower: a row is unfit for any one the column cannot hold, and written each one
    /// converted.
    #[test]
    fn a_list_a_table_holds_narrower_is_checked_and_written_item_by_item() {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Int32Type, Int64Type};
        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1), None, Some(2)]),
            Some(vec![Some(3), Some(1 << 40)]),
            None,
        ]);
        let lists: ArrayRef = Arc::new(lists);
        let to = Arrow::List(Arc::new(Field::new_list_field(Arrow::Int32, true)));
        assert_eq!(unfit_at(&lists, &to, 0, &|| "n".into()), None);
        assert_eq!(
            unfit_at(&lists, &to, 1, &|| "n".into()).as_deref(),
            Some("n[] is 1099511627776, which a 32-bit integer column cannot hold")
        );
        assert_eq!(
            unfit_at(&lists, &to, 2, &|| "n".into()),
            None,
            "a null list holds nothing"
        );
        let written = relabelled(&lists.slice(0, 1), None, &to, None);
        let items = written.as_list::<i32>().value(0);
        let items = items.as_primitive::<Int32Type>();
        assert_eq!(items.iter().collect::<Vec<_>>(), vec![Some(1), None, Some(2)]);
    }

    /// A written schema may narrow only a leaf the record types wider.
    #[test]
    #[should_panic(expected = "a leaf narrowed")]
    fn a_written_schema_narrows_no_leaf_the_record_types_otherwise() {
        let schema = Schema::new(&st(&[("s", DataType::Str)], false)).unwrap();
        let _ = schema.with_written(Arc::new(ArrowSchema::new(vec![Field::new("s", Arrow::Int32, true)])));
    }

    #[test]
    fn a_file_of_a_partition_whose_value_is_empty_holds_its_column_null() {
        use arrow_array::cast::AsArray;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let schema = raw_schema().with_partition_columns(vec![("p".into(), 0)]);
        let own = rows(&schema, &["", ""]);
        let read = |values: &[(String, String)]| {
            let mut w = FileWriter::of_partition(&schema, &Compression::None.into(), values);
            w.write(&own);
            let (file, _) = w.finish();
            let reader = ParquetRecordBatchReaderBuilder::try_new(tokio_util::bytes::Bytes::from(file))
                .unwrap()
                .build()
                .unwrap();
            (reader.map(Result::unwrap))
                .flat_map(|b| {
                    b.column(0)
                        .as_string::<i32>()
                        .iter()
                        .map(|v| v.map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(read(&[("p".into(), String::new())]), [None, None]);
        assert_eq!(
            read(&[("q".into(), String::new())]),
            [Some(String::new()), Some(String::new())]
        );
        assert_eq!(read(&[]), [Some(String::new()), Some(String::new())]);
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
        let mut w = FileWriter::open(&labelled, &Compression::None.into());
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
        let table = table_schema(&record, None, 0, 2).unwrap().schema;
        let labelled = schema
            .clone()
            .with_written(with_field_ids(schema.arrow(), &table).unwrap().0);

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
            chunk: Arc::new(Chunk::new(batch, None, Vec::new())),
            row: 0,
        };
        assert!(labelled.matches(&row));
        let mut w = FileWriter::open(&labelled, &Compression::None.into());
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
    fn a_table_column_the_record_lacks_is_written_null_at_every_depth() {
        use crate::io::iceberg::metadata::{ListType, NestedField, Primitive, StructType, Type};
        use crate::io::iceberg::schema::{table_schema, with_field_ids};
        use arrow_array::cast::AsArray;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::basic::LogicalType;

        let record = st(&[("o", st(&[("a", DataType::I64)], false))], false);
        let schema = Schema::new(&record).unwrap();
        let mut table = table_schema(&record, None, 0, 2).unwrap().schema;
        let field = |id: i32, name: &str, field_type: Type| NestedField {
            id,
            name: name.into(),
            required: false,
            field_type,
            doc: None,
            other: Default::default(),
        };
        let other = |name: &str| Type::Primitive(Primitive::Other(name.into()));
        let Type::Struct(o) = &mut table.fields[0].field_type else {
            panic!("o is a struct")
        };
        o.fields.push(field(3, "b", Type::Primitive(Primitive::String)));
        table.fields.extend([
            field(4, "d", other("decimal(10, 2)")),
            field(5, "u", other("uuid")),
            field(6, "f", other("fixed[4]")),
            field(7, "t", other("time")),
            field(
                8,
                "l",
                Type::List(ListType {
                    element_id: 9,
                    element_required: false,
                    element: Box::new(Type::Primitive(Primitive::Int)),
                }),
            ),
            field(
                10,
                "m",
                Type::Map(
                    serde_json::json!({"type": "map", "key-id": 11, "key": "string", "value-id": 12,
                    "value": "long", "value-required": false}),
                ),
            ),
            field(
                13,
                "s",
                Type::Struct(StructType {
                    fields: vec![field(14, "x", Type::Primitive(Primitive::Timestamptz))],
                }),
            ),
        ]);
        let labelled = schema
            .clone()
            .with_written(with_field_ids(schema.arrow(), &table).unwrap().0);

        let Arrow::Struct(fields) = schema.arrow().field(0).data_type().clone() else {
            panic!("o is a struct")
        };
        let o: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
            None,
        ));
        let batch = RecordBatch::try_new(schema.arrow().clone(), vec![o]).unwrap();
        let chunk = Arc::new(Chunk::new(batch, None, Vec::new()));
        let rows: Vec<Columns> = (0..2)
            .map(|row| Columns {
                chunk: chunk.clone(),
                row,
            })
            .collect();
        assert!(labelled.matches(&rows[0]));
        let mut w = FileWriter::open(&labelled, &Compression::None.into());
        w.write(&rows);
        let (file, _) = w.finish();

        let path = std::env::temp_dir().join(format!("smeltr-parquet-{}-all-columns.parquet", std::process::id()));
        std::fs::write(&path, &file).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap();
        let descr = builder.metadata().file_metadata().schema_descr();
        let leaves: Vec<(i32, Option<LogicalType>)> = (0..descr.num_columns())
            .map(|i| {
                let column = descr.column(i);
                (
                    column.self_type().get_basic_info().id(),
                    column.logical_type_ref().cloned(),
                )
            })
            .collect();
        let ids: Vec<i32> = leaves.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [2, 3, 4, 5, 6, 7, 9, 11, 12, 14]);
        assert_eq!(leaves[2].1, Some(LogicalType::decimal(2, 10)));
        assert_eq!(leaves[3].1, Some(LogicalType::Uuid));
        assert_eq!(
            leaves[5].1,
            Some(LogicalType::time(false, parquet::basic::TimeUnit::MICROS))
        );

        let read: Vec<RecordBatch> = builder.build().unwrap().map(Result::unwrap).collect();
        std::fs::remove_file(&path).unwrap();
        let batch = &read[0];
        let names: Vec<&str> = batch.schema_ref().fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["o", "d", "u", "f", "t", "l", "m", "s"]);
        let o = batch.column(0).as_struct();
        assert_eq!(o.column(0).null_count(), 0);
        assert_eq!(o.column(1).null_count(), 2, "o.b");
        for (column, name) in batch.columns().iter().zip(names).skip(1) {
            assert_eq!(column.null_count(), 2, "{name}");
        }
    }

    #[test]
    fn a_table_column_of_a_version_3_type_is_written_as_the_spec_has_parquet_hold_it() {
        use crate::io::iceberg::metadata::{NestedField, Primitive, StructType, Type};
        use crate::io::iceberg::schema::{parquet_annotations, table_schema, with_field_ids};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::basic::{EdgeInterpolationAlgorithm, LogicalType, TimeUnit as Unit};

        let record = st(&[("a", DataType::I64)], false);
        let schema = Schema::new(&record).unwrap();
        let mut table = table_schema(&record, None, 0, 2).unwrap().schema;
        let field = |id: i32, name: &str, ty: &str| NestedField {
            id,
            name: name.into(),
            required: false,
            field_type: Type::Primitive(Primitive::Other(ty.into())),
            doc: None,
            other: Default::default(),
        };
        table.fields.extend([
            field(2, "ns", "timestamp_ns"),
            field(3, "nstz", "timestamptz_ns"),
            field(4, "geom", "geometry(srid:4326)"),
            field(5, "geog", "geography(OGC:CRS84, karney)"),
            field(6, "plain", "geometry"),
            field(7, "v", "variant"),
            field(8, "gone", "unknown"),
            NestedField {
                field_type: Type::Struct(StructType {
                    fields: vec![field(13, "gone", "unknown")],
                }),
                ..field(12, "empty", "")
            },
            NestedField {
                field_type: Type::Struct(StructType {
                    fields: vec![field(10, "gone", "unknown"), field(11, "v", "variant")],
                }),
                ..field(9, "s", "")
            },
        ]);
        let (written, defaults) = with_field_ids(schema.arrow(), &table).unwrap();
        let labelled = (schema.clone().with_written(written).with_defaults(defaults))
            .with_annotations(parquet_annotations(&table));

        let batch = RecordBatch::try_new(schema.arrow().clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let chunk = Arc::new(Chunk::new(batch, None, Vec::new()));
        let rows: Vec<Columns> = (0..2)
            .map(|row| Columns {
                chunk: chunk.clone(),
                row,
            })
            .collect();
        let mut w = FileWriter::open(&labelled, &Compression::None.into());
        w.write(&rows);
        let (file, _) = w.finish();

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file)).unwrap();
        let root = builder.metadata().file_metadata().schema_descr().root_schema();
        let fields: Vec<(&str, Option<LogicalType>)> = (root.get_fields().iter())
            .map(|f| (f.name(), f.get_basic_info().logical_type_ref().cloned()))
            .collect();
        assert_eq!(
            fields,
            [
                ("a", None),
                ("ns", Some(LogicalType::timestamp(false, Unit::NANOS))),
                ("nstz", Some(LogicalType::timestamp(true, Unit::NANOS))),
                ("geom", Some(LogicalType::geometry(Some("srid:4326".into())))),
                (
                    "geog",
                    Some(LogicalType::geography(
                        Some("OGC:CRS84".into()),
                        Some(EdgeInterpolationAlgorithm::KARNEY)
                    ))
                ),
                ("plain", Some(LogicalType::geometry(None))),
                ("v", Some(LogicalType::variant(Some(1)))),
                ("s", None),
            ],
            "an unknown column is not stored"
        );
        let v = &root.get_fields()[6];
        let parts: Vec<(&str, bool)> = (v.get_fields().iter())
            .map(|f| (f.name(), f.get_basic_info().has_id()))
            .collect();
        assert_eq!(parts, [("metadata", false), ("value", false)]);
        let s = &root.get_fields()[7];
        assert_eq!(s.get_fields().len(), 1);
        assert_eq!(
            s.get_fields()[0].get_basic_info().logical_type_ref(),
            Some(&LogicalType::variant(Some(1)))
        );

        let read: Vec<RecordBatch> = builder.build().unwrap().map(Result::unwrap).collect();
        for column in read[0].columns().iter().skip(1) {
            assert_eq!(column.null_count(), 2);
        }
    }

    #[test]
    fn a_variant_a_table_holds_as_one_is_written_as_parquets_variant_each_value_of_its_own_type() {
        use crate::format::tag::{self, Tag};
        use crate::io::iceberg::schema::{parquet_annotations, table_schema, with_field_ids};
        use crate::io::parquet_variant::tests::read;
        use crate::plan::tree::UNCLAIMED;
        use arrow_array::cast::AsArray;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::borrow::Cow;

        let record = st(
            &[
                ("v", DataType::Variant),
                ("o", DataType::Variant),
                ("s", st(&[("w", DataType::Variant), ("n", DataType::I64)], false)),
                ("l", DataType::Array(Box::new(DataType::Variant))),
            ],
            false,
        );
        let schema = Schema::new(&record).unwrap();
        let table = table_schema(&record, None, 0, 3).unwrap().schema;
        let (written, defaults) = with_field_ids(schema.arrow(), &table).unwrap();
        let labelled = (schema.clone().with_written(written).with_defaults(defaults))
            .with_annotations(parquet_annotations(&table));

        let mut pool = BufferPool::default();
        let obj = |fields: Vec<(&'static str, Tag<'static>)>| {
            Tag::Object(fields.into_iter().map(|(k, v)| (Cow::Borrowed(k), v)).collect())
        };
        let v = tag::encode_column(
            &[
                Some(obj(vec![
                    ("t", Tag::DateTime(5)),
                    ("b", Tag::Bytes(Cow::Borrowed(&[1, 2]))),
                    ("d", Tag::Duration(7)),
                    (
                        "a",
                        Tag::Array(vec![
                            Some(Tag::I64(1)),
                            Some(Tag::F64(2.5)),
                            Some(Tag::Bool(true)),
                            None,
                        ]),
                    ),
                ])),
                None,
            ],
            &mut pool,
        );
        // `o` is an object of the view's, under which the unclaimed fields hold more, or a value that is no object.
        fn decoded(values: &[Option<Tag<'_>>], ty: &DataType, pool: &mut BufferPool) -> Column {
            let variant = tag::encode_column(values, pool);
            let column = tag::decode_column(&variant, ty, 0, pool).0;
            variant.reclaim(pool);
            column
        }
        let k = decoded(&[Some(Tag::Str("hi".into())), None], &DataType::Str, &mut pool);
        let unclaimed = tag::encode_column(
            &[
                Some(obj(vec![("o", obj(vec![("z", Tag::I64(1))]))])),
                Some(obj(vec![("o", Tag::I64(5))])),
            ],
            &mut pool,
        );
        // A text column where the record holds a variant.
        let w = decoded(&[Some(Tag::Str("s".into())), None], &DataType::Str, &mut pool);
        let l = decoded(
            &[
                Some(Tag::Array(vec![
                    Some(Tag::Str("x".into())),
                    Some(obj(vec![("q", Tag::I64(1))])),
                ])),
                None,
            ],
            &DataType::Array(Box::new(DataType::Variant)),
            &mut pool,
        );
        let view = RecordView::from_named([("v", &v), ("o.k", &k), (UNCLAIMED, &unclaimed), ("s.w", &w), ("l", &l)]);
        let chunk = Arc::new(Chunk::cut(&schema, &[(&view, &[0, 1])], 0, &mut pool));
        drop(view);
        for column in [v, k, unclaimed, w, l] {
            column.reclaim(&mut pool);
        }
        assert_eq!(chunk.unfit(0), None);
        let rows: Vec<Columns> = (0..2)
            .map(|row| Columns {
                chunk: chunk.clone(),
                row,
            })
            .collect();
        let mut writer = FileWriter::open(&labelled, &Compression::None.into());
        // Each row alone, as a chunk of its own, and both together, as one.
        writer.write(&[rows[0].alone(), rows[1].alone()]);
        writer.write(&rows);
        let (file, _) = writer.finish();

        let read_back: Vec<RecordBatch> = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file))
            .unwrap()
            .build()
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let batch = arrow_select::concat::concat_batches(&read_back[0].schema(), &read_back).unwrap();
        let text = |v: &StructArray, i: usize| {
            v.is_valid(i).then(|| {
                let (m, b) = (v.column(0).as_binary::<i32>(), v.column(1).as_binary::<i32>());
                read(m.value(i), b.value(i))
            })
        };
        let list = batch.column(3).as_list::<i32>();
        for row in [0, 2] {
            assert_eq!(
                text(batch.column(0).as_struct(), row).as_deref(),
                Some("{a:[1,2.5,true,null],b:bin:[1, 2],d:7,t:ts:5}")
            );
            assert_eq!(
                text(batch.column(1).as_struct(), row).as_deref(),
                Some("{k:\"hi\",z:1}")
            );
            let s = batch.column(2).as_struct();
            assert_eq!(text(s.column(0).as_struct(), row).as_deref(), Some("\"s\""));
            let items = list.value(row);
            let items = items.as_struct();
            assert_eq!(text(items, 0).as_deref(), Some("\"x\""));
            assert_eq!(text(items, 1).as_deref(), Some("{q:1}"));
        }
        for row in [1, 3] {
            assert_eq!(text(batch.column(0).as_struct(), row), None);
            assert_eq!(text(batch.column(1).as_struct(), row).as_deref(), Some("5"));
            assert_eq!(text(batch.column(2).as_struct().column(0).as_struct(), row), None);
            assert!(list.is_null(row));
        }
    }

    #[test]
    fn a_table_column_the_record_lacks_takes_its_write_default_at_every_depth() {
        use crate::io::iceberg::metadata::{ListType, NestedField, Primitive, StructType, Type};
        use crate::io::iceberg::schema::{table_schema, with_field_ids};
        use arrow_array::cast::AsArray;
        use arrow_array::types::{
            Date32Type, Decimal128Type, Int32Type, Int64Type, Time64MicrosecondType, TimestampMicrosecondType,
        };
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use serde_json::json;

        let record = st(&[("o", st(&[("a", DataType::I64)], false))], false);
        let schema = Schema::new(&record).unwrap();
        let mut table = table_schema(&record, None, 0, 2).unwrap().schema;
        let field = |id: i32, name: &str, field_type: Type, default: serde_json::Value| NestedField {
            id,
            name: name.into(),
            required: false,
            field_type,
            doc: None,
            other: [("write-default".to_string(), default)].into(),
        };
        let other = |name: &str| Type::Primitive(Primitive::Other(name.into()));
        let Type::Struct(o) = &mut table.fields[0].field_type else {
            panic!("o is a struct")
        };
        o.fields
            .push(field(3, "b", Type::Primitive(Primitive::String), json!("dflt")));
        let mut required = field(16, "r", Type::Primitive(Primitive::Long), json!(7));
        required.required = true;
        table.fields.extend([
            field(4, "d", other("decimal(10, 2)"), json!("-14.2")),
            field(5, "u", other("uuid"), json!("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
            field(6, "f", other("fixed[2]"), json!("0a0B")),
            field(7, "t", other("time"), json!("22:31:08.000123")),
            field(
                8,
                "l",
                Type::List(ListType {
                    element_id: 9,
                    element_required: false,
                    element: Box::new(Type::Primitive(Primitive::Int)),
                }),
                json!([1, 2]),
            ),
            field(
                10,
                "m",
                Type::Map(json!({"type": "map", "key-id": 11, "key": "string", "value-id": 12,
                    "value": "long", "value-required": false})),
                json!({"keys": ["k"], "values": [5]}),
            ),
            field(
                13,
                "s",
                Type::Struct(StructType {
                    fields: vec![field(
                        14,
                        "x",
                        Type::Primitive(Primitive::Timestamptz),
                        json!("1970-01-01T00:00:01+00:00"),
                    )],
                }),
                json!({}),
            ),
            field(15, "n", Type::Primitive(Primitive::Date), json!("1970-01-03")),
            required,
            field(17, "z", Type::Primitive(Primitive::Boolean), serde_json::Value::Null),
        ]);
        let (written, defaults) = with_field_ids(schema.arrow(), &table).unwrap();
        let labelled = schema.clone().with_written(written).with_defaults(defaults);

        let Arrow::Struct(fields) = schema.arrow().field(0).data_type().clone() else {
            panic!("o is a struct")
        };
        // The second row's o is null, and so is the o.b it holds.
        let o: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
            Some(NullBuffer::from(vec![true, false])),
        ));
        let batch = RecordBatch::try_new(schema.arrow().clone(), vec![o]).unwrap();
        let chunk = Arc::new(Chunk::new(batch, None, Vec::new()));
        let rows: Vec<Columns> = (0..2)
            .map(|row| Columns {
                chunk: chunk.clone(),
                row,
            })
            .collect();
        let mut w = FileWriter::open(&labelled, &Compression::None.into());
        w.write(&rows);
        let (file, _) = w.finish();

        let read: Vec<RecordBatch> = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file))
            .unwrap()
            .build()
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let batch = &read[0];
        let column = |name: &str| batch.column_by_name(name).unwrap().clone();
        let o = column("o");
        let b = o.as_struct().column(1).as_string::<i32>();
        assert_eq!((b.value(0), o.is_null(1)), ("dflt", true));
        for row in 0..2 {
            assert_eq!(column("d").as_primitive::<Decimal128Type>().value(row), -1420);
            assert_eq!(
                column("u").as_fixed_size_binary().value(row),
                [
                    0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7
                ]
            );
            assert_eq!(column("f").as_fixed_size_binary().value(row), [10, 11]);
            assert_eq!(
                column("t").as_primitive::<Time64MicrosecondType>().value(row),
                ((22 * 60 + 31) * 60 + 8) * 1_000_000 + 123
            );
            let l = column("l");
            assert_eq!(
                l.as_list::<i32>().value(row).as_primitive::<Int32Type>().values(),
                &[1, 2]
            );
            let m = column("m");
            let entries = m.as_map().value(row);
            assert_eq!(entries.column(0).as_string::<i32>().value(0), "k");
            assert_eq!(entries.column(1).as_primitive::<Int64Type>().value(0), 5);
            let x = column("s").as_struct().column(0).clone();
            assert_eq!(x.as_primitive::<TimestampMicrosecondType>().value(row), 1_000_000);
            assert_eq!(column("n").as_primitive::<Date32Type>().value(row), 2);
            assert_eq!(column("r").as_primitive::<Int64Type>().value(row), 7);
        }
        assert_eq!(column("z").null_count(), 2);
    }

    #[test]
    fn a_write_default_that_is_no_value_of_its_column_is_refused() {
        use crate::io::iceberg::metadata::{NestedField, Primitive, StructType, Type};
        use crate::io::iceberg::schema::{table_schema, with_field_ids};
        use serde_json::json;

        let record = st(&[("a", DataType::I64)], false);
        let schema = Schema::new(&record).unwrap();
        let field = |name: &str, field_type: Type, default: serde_json::Value| NestedField {
            id: 9,
            name: name.into(),
            required: false,
            field_type,
            doc: None,
            other: [("write-default".to_string(), default)].into(),
        };
        let other = |name: &str| Type::Primitive(Primitive::Other(name.into()));
        let refused = |column: NestedField| {
            let mut table = table_schema(&record, None, 0, 2).unwrap().schema;
            table.fields.push(column);
            with_field_ids(schema.arrow(), &table).unwrap_err()
        };
        assert_eq!(
            refused(field("i", Type::Primitive(Primitive::Int), json!(3_000_000_000_i64))),
            "the table's column i has the write default 3000000000, which is not of type int"
        );
        for (ty, default) in [
            (other("decimal(3, 1)"), json!("100.0")),
            (other("decimal(3, 1)"), json!("1.25")),
            (other("uuid"), json!("f79c3e09677c4bbda4793f349cb785e7")),
            (other("fixed[2]"), json!("0a")),
            (Type::Primitive(Primitive::Timestamptz), json!("1970-01-01T00:00:00")),
            (Type::Primitive(Primitive::Date), json!(2)),
        ] {
            assert!(
                refused(field("v", ty, default.clone())).contains("which is not of type"),
                "{default}"
            );
        }
        let mut x = field("x", Type::Primitive(Primitive::Long), serde_json::Value::Null);
        x.required = true;
        let s = field("s", Type::Struct(StructType { fields: vec![x] }), json!({}));
        assert_eq!(
            refused(s),
            "the table's column s.x is required, and has no write default for a row that lacks it"
        );
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
        let table = table_schema(&record, None, Mapping::name(0), None).unwrap().schema;
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
            chunk: Arc::new(Chunk::new(batch, None, Vec::new())),
            row: 0,
        };
        assert!(labelled.matches(&row));
        let mut w = FileWriter::open(&labelled, &Compression::None.into());
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
