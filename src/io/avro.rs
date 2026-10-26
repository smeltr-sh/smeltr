//! Avro on Kafka, as Confluent frames it: a zero byte, the id of the schema it was written with as four big-endian
//! bytes, then the value in Avro's binary encoding. The schema is the registry's, by that id (`registry`).
//!
//! Reading, a payload is read with the schema its writer named, and each field its source declares is taken from it
//! by name, as the declared type (`io::values::column`): what a payload holds is the writer's to say, and what a
//! pipeline reads is the bundle's. A schema is asked for once per id and kept for as long as the source.
//!
//! Writing, a route's record, as Parquet's columns (`io::parquet::Schema`), is an Avro record. It is registered under
//! `<topic>-value` the first time a row of it goes to that topic, and each row is its columns written with it. The
//! types are written as Parquet writes them:
//!
//! | record | Avro |
//! |---|---|
//! | `bool`, `i64`, `f64`, `str`, `bytes` | `boolean`, `long`, `double`, `string`, `bytes` |
//! | `datetime` | `long`, `timestamp-micros` |
//! | `duration` | `long`, its microseconds |
//! | a variant | `string`, its JSON text |
//! | an array, a struct | an `array`, a `record`; an open struct's unclaimed fields its `rest`, a JSON object |
//!
//! Every field is a union with `null`, defaulting to it. A record field's name must be one Avro allows.

pub mod container;
pub mod registry;

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::{NamesRef, ResolvedSchema};
use apache_avro::types::Value as Avro;
use apache_avro::writer::datum::GenericDatumWriter;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType as Arrow, Fields, Schema as ArrowSchema, SchemaRef};

use self::registry::{Registry, Unread};
use crate::core::column::DataType;
use crate::core::variant;
use crate::io::values::{self, Value};
use crate::io::{IoError, RecordChunk, SourceRecord};
use crate::plan::topology::{AvroRead, HeldSecrets, SchemaRegistry, Tls};

/// The byte a framed payload starts with.
const MAGIC: u8 = 0;

/// What a framed payload carries before its value: the magic byte and the schema's id.
const HEADER: usize = 5;

/// `payload`'s schema id and its value's bytes. Err says why it is not Confluent-framed Avro.
pub fn unframe(payload: &[u8]) -> Result<(u32, &[u8]), String> {
    match payload {
        [MAGIC, a, b, c, d, value @ ..] => Ok((u32::from_be_bytes([*a, *b, *c, *d]), value)),
        [] => Err("an empty payload, which holds no Avro".to_string()),
        [first, ..] if *first != MAGIC => Err(format!(
            "a payload starting with byte {first:#04x}, not the zero byte Confluent's Avro starts with"
        )),
        _ => Err(format!(
            "a payload of {} bytes, shorter than the 5 Avro's header takes",
            payload.len()
        )),
    }
}

/// `value` written with schema `id`, framed.
pub fn frame(id: u32, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + value.len());
    out.push(MAGIC);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// A writer's schema: the schema itself and every named schema it references.
pub struct Parsed {
    schema: Schema,
    referenced: Vec<Schema>,
}

impl Parsed {
    /// `text` and the texts of what it references, which may reference each other.
    pub fn new(text: &str, referenced: &[String]) -> Result<Parsed, String> {
        let (schema, referenced) = Schema::parse_str_with_list(text, referenced).map_err(|e| e.to_string())?;
        Ok(Parsed { schema, referenced })
    }

    /// The schema itself, without what it references.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn resolved(&self) -> Result<ResolvedSchema<'_>, String> {
        // What it references first: each is resolved against those before it.
        let all = self.referenced.iter().chain(std::iter::once(&self.schema)).collect();
        ResolvedSchema::new_with_schemata(all).map_err(|e| e.to_string())
    }
}

/// The schema `id` names, with every schema it references, asked of `registry`.
pub fn fetch(registry: &Registry, id: u32) -> Result<Parsed, Unread> {
    let text = registry.schema(id)?;
    let mut seen: Vec<(String, i64)> = Vec::new();
    let mut referenced: Vec<String> = Vec::new();
    for r in &text.references {
        fetch_reference(registry, id, r, &mut seen, &mut referenced)?;
    }
    Parsed::new(&text.schema, &referenced).map_err(|e| Unread::Refused(format!("schema {id} does not parse: {e}")))
}

/// Adds `r`'s schema to `referenced` after every schema it references, each subject and version once: the parser
/// resolves each against those before it.
fn fetch_reference(
    registry: &Registry,
    id: u32,
    r: &registry::Reference,
    seen: &mut Vec<(String, i64)>,
    referenced: &mut Vec<String>,
) -> Result<(), Unread> {
    if seen.iter().any(|(s, v)| *s == r.subject && *v == r.version) {
        return Ok(());
    }
    // A bound on the references also bounds the depth.
    if seen.len() == registry::MAX_REFERENCES {
        return Err(Unread::Refused(format!(
            "schema {id} references more than {} schemas",
            registry::MAX_REFERENCES
        )));
    }
    seen.push((r.subject.clone(), r.version));
    let got = registry.version(&r.subject, r.version)?;
    for inner in &got.references {
        fetch_reference(registry, id, inner, seen, referenced)?;
    }
    referenced.push(got.schema);
    Ok(())
}

/// How a source reads its payloads as Avro: the registry it asks for each writer's schema, the record it reads each
/// payload into, and every schema it has been answered or told.
pub struct AvroReader {
    /// `None` for a reader every schema is told to (`AvroReader::tell`), as a container file's is.
    registry: Option<Registry>,
    /// Each declared field, by its path's keys.
    fields: Vec<(Vec<String>, DataType)>,
    /// Whether the record keeps the root's fields no declared field claims, as its last column (`tree::UNCLAIMED`).
    open: bool,
    /// By id: the schema, or why the registry will never give one.
    schemas: HashMap<u32, Result<Arc<Parsed>, String>>,
}

impl AvroReader {
    pub fn new(config: &AvroRead, tls: Option<&Tls>, secrets: &HeldSecrets) -> Result<AvroReader, IoError> {
        Ok(AvroReader {
            registry: (config.registry.as_ref())
                .map(|r| Registry::new(r, tls, secrets))
                .transpose()?,
            fields: (config.meta.record_fields().into_iter())
                .filter(|(n, _)| *n != crate::plan::tree::UNCLAIMED)
                .map(|(n, t)| (crate::plan::tree::segments(n).map(|k| k.into_owned()).collect(), t))
                .collect(),
            open: config.meta.is_open(),
            schemas: HashMap::new(),
        })
    }

    /// The schema id `payload` names that this reader has not asked for yet. `None` for a payload whose schema is
    /// known, or that names none.
    pub fn unasked(&self, payload: &[u8]) -> Option<u32> {
        let (id, _) = unframe(payload).ok()?;
        (!self.schemas.contains_key(&id)).then_some(id)
    }

    /// Asks the registry for schema `id`. Err is a registry that may answer later, and leaves the schema unasked; a
    /// refusal is kept, and every payload naming it is read as one that does not read.
    pub fn ask(&mut self, id: u32) -> Result<(), IoError> {
        let answer = match self.registry.as_ref().map(|r| fetch(r, id)) {
            Some(Ok(parsed)) => Ok(Arc::new(parsed)),
            Some(Err(Unread::Refused(why))) => Err(why),
            Some(Err(Unread::Transient(e))) => return Err(e),
            None => Err(format!("schema {id}, which no registry holds")),
        };
        self.schemas.insert(id, answer);
        Ok(())
    }

    /// Takes `schema` as schema `id`, for payloads framed with it that no registry was asked about.
    pub fn tell(&mut self, id: u32, schema: Arc<Parsed>) {
        self.schemas.insert(id, Ok(schema));
    }

    /// Forgets schema `id`, told or answered.
    pub fn forget(&mut self, id: u32) {
        self.schemas.remove(&id);
    }

    /// `records`, a poll of payloads, as a chunk of the declared fields. A payload that does not read is a row of
    /// nulls with the reason (`RecordChunk::unread`); a value that is not its field's type is null and failed.
    pub fn read(&self, records: Vec<SourceRecord>) -> Vec<SourceRecord> {
        let width = self.fields.len() + usize::from(self.open);
        let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(records.len()); width];
        let mut unread = Vec::new();
        let mut time = Vec::with_capacity(records.len());
        // The schema each read row was written with, newest last: what a capture of the source declares from.
        let mut writers: Vec<u32> = Vec::new();
        // One datum reader per schema per poll: building one resolves the schema's names.
        let mut readers: HashMap<u32, Result<(GenericDatumReader<'_>, NamesRef<'_>), String>> = HashMap::new();
        for (row, record) in records.iter().enumerate() {
            time.push(record.chunk.time(record.row as usize));
            let payload = record.bytes().unwrap_or_default();
            let read = self.read_one(payload, &mut readers);
            match read {
                Ok(fields) => {
                    if let Ok((id, _)) = unframe(payload)
                        && writers.last() != Some(&id)
                    {
                        writers.retain(|w| *w != id);
                        writers.push(id);
                    }
                    for (column, value) in columns.iter_mut().zip(fields) {
                        column.push(value);
                    }
                }
                Err(why) => {
                    unread.push((row as u32, why));
                    columns.iter_mut().for_each(|c| c.push(Value::Null));
                }
            }
        }
        let mut pool = crate::core::buffer::BufferPool::new();
        let mut failed = Vec::new();
        let mut built = Vec::with_capacity(width);
        let types = (self.fields.iter().map(|(_, ty)| ty)).chain(self.open.then_some(&DataType::Variant));
        for (f, (ty, values)) in types.zip(&columns).enumerate() {
            let (column, refused) =
                values::column(&mut pool, ty, values).expect("no declaration names a type no column is built as");
            failed.extend(
                refused
                    .into_iter()
                    .map(|row| (row as u32, f, values::spell(&values[row]))),
            );
            built.push(column);
        }
        let writers = (writers.iter())
            .filter_map(|id| self.schemas.get(id)?.as_ref().ok().cloned())
            .collect();
        let chunk = Arc::new(
            RecordChunk::new(built, failed)
                .stamped(time)
                .with_unread(unread)
                .read_with(writers),
        );
        records
            .into_iter()
            .enumerate()
            .map(|(row, r)| SourceRecord {
                chunk: chunk.clone(),
                row: row as u32,
                ..r
            })
            .collect()
    }

    /// The declared fields of one payload, in their order, then the root's fields none claims where the record is
    /// open.
    fn read_one<'s>(
        &'s self,
        payload: &[u8],
        readers: &mut HashMap<u32, Result<(GenericDatumReader<'s>, NamesRef<'s>), String>>,
    ) -> Result<Vec<Value>, String> {
        let (id, mut body) = unframe(payload)?;
        let reader = readers.entry(id).or_insert_with(|| {
            let parsed = match self.schemas.get(&id) {
                Some(Ok(parsed)) => parsed,
                Some(Err(why)) => return Err(why.clone()),
                None => return Err(format!("schema {id}, which the registry has not been asked for")),
            };
            let resolved = parsed.resolved()?;
            let names = resolved.get_names().clone();
            let reader = GenericDatumReader::builder(&parsed.schema)
                .resolved_writer_schemata(resolved)
                .build()
                .map_err(|e| e.to_string())?;
            Ok((reader, names))
        });
        let (reader, names) = reader.as_ref().map_err(String::clone)?;
        let value = reader
            .read_value(&mut body)
            .map_err(|e| format!("an Avro value that does not read with schema {id}: {e}"))?;
        if !body.is_empty() {
            return Err(format!("an Avro value with {} bytes after it", body.len()));
        }
        let parsed = self.schemas[&id].as_ref().expect("read only with a schema");
        if !matches!(resolve(&parsed.schema, names), Schema::Record(_)) {
            return Err(format!("schema {id} is no record, so it has no fields to read"));
        }
        let mut out: Vec<Value> = (self.fields.iter())
            .map(|(path, _)| match member(&value, &parsed.schema, names, path) {
                Some((value, schema)) => to_value(value, schema, names),
                None => Value::Null,
            })
            .collect();
        if self.open {
            out.push(self.unclaimed(&value, &parsed.schema, names));
        }
        Ok(out)
    }

    /// The members of `value`, a record, that no declared field claims by its first key, as one object. Null where
    /// there are none.
    fn unclaimed(&self, value: &Avro, schema: &Schema, names: &NamesRef<'_>) -> Value {
        let (Avro::Record(members), Schema::Record(record)) = unwrapped(value, schema, names) else {
            return Value::Null;
        };
        let claimed = |name: &str| {
            self.fields
                .iter()
                .any(|(path, _)| path.first().is_some_and(|k| k == name))
        };
        let mut out = Vec::new();
        let open = variant::begin_object(&mut out);
        let mut count = 0;
        for ((name, v), field) in members.iter().zip(&record.fields) {
            if claimed(name) || is_null(v) {
                continue;
            }
            variant::put_key(&mut out, name);
            put(v, &field.schema, names, &mut out);
            count += 1;
        }
        variant::end(&mut out, open, count);
        match count {
            0 => Value::Null,
            _ => values::from_variant(&out),
        }
    }
}

/// The member of `value` at `path`, a record field's name at each step, with its schema. `None` where a step's value
/// is null or no record, or holds no field of that name.
fn member<'v, 's>(
    mut value: &'v Avro,
    mut schema: &'s Schema,
    names: &NamesRef<'s>,
    path: &[String],
) -> Option<(&'v Avro, &'s Schema)> {
    for key in path {
        (value, schema) = unwrapped(value, schema, names);
        let (Avro::Record(members), Schema::Record(record)) = (value, schema) else {
            return None;
        };
        let at = *record.lookup.get(key)?;
        (value, schema) = (&members.get(at)?.1, &record.fields[at].schema);
    }
    Some((value, schema))
}

/// `value` and its schema, through any union and named reference: what the value is.
fn unwrapped<'v, 's>(mut value: &'v Avro, schema: &'s Schema, names: &NamesRef<'s>) -> (&'v Avro, &'s Schema) {
    let mut schema = resolve(schema, names);
    while let (Avro::Union(i, inner), Schema::Union(u)) = (value, schema) {
        let Some(s) = u.variants().get(*i as usize) else {
            break;
        };
        (value, schema) = (inner, resolve(s, names));
    }
    (value, schema)
}

/// `schema`, or the schema it names.
fn resolve<'s>(schema: &'s Schema, names: &NamesRef<'s>) -> &'s Schema {
    match schema {
        Schema::Ref { name } => names.get(name).copied().unwrap_or(schema),
        other => other,
    }
}

/// An Avro value as a source's value: a record or a map is an object, a union the value it holds.
fn to_value(value: &Avro, schema: &Schema, names: &NamesRef<'_>) -> Value {
    let schema = resolve(schema, names);
    match value {
        Avro::Union(i, inner) => match schema {
            Schema::Union(u) => u
                .variants()
                .get(*i as usize)
                .map_or(Value::Null, |s| to_value(inner, s, names)),
            _ => to_value(inner, &Schema::Null, names),
        },
        Avro::Array(items) => {
            let item = match schema {
                Schema::Array(a) => &*a.items,
                _ => &Schema::Null,
            };
            Value::Array(items.iter().map(|v| to_value(v, item, names)).collect())
        }
        Avro::Record(_) | Avro::Map(_) | Avro::Duration(_) => {
            let mut out = Vec::new();
            put(value, schema, names, &mut out);
            values::from_variant(&out)
        }
        Avro::Null => Value::Null,
        Avro::Boolean(b) => Value::Bool(*b),
        Avro::Int(i) => Value::Int(*i as i64),
        Avro::Long(i) => Value::Int(*i),
        Avro::Float(f) => Value::Float(*f as f64),
        Avro::Double(f) => Value::Float(*f),
        Avro::Bytes(b) | Avro::Fixed(_, b) => Value::Bytes(b.clone()),
        Avro::String(s) | Avro::Enum(_, s) => Value::Str(s.clone()),
        Avro::Uuid(u) => Value::Str(u.as_hyphenated().to_string()),
        Avro::Decimal(d) => {
            let scale = match schema {
                Schema::Decimal(s) => s.scale,
                _ => 0,
            };
            Value::Str(decimal(&Vec::<u8>::try_from(d).unwrap_or_default(), scale))
        }
        Avro::BigDecimal(d) => Value::Str(d.to_string()),
        Avro::Date(days) => Value::Time(*days as i64 * MICROS_PER_DAY),
        Avro::TimeMillis(ms) => Value::Duration(*ms as i64 * 1000),
        Avro::TimeMicros(us) => Value::Duration(*us),
        Avro::TimestampMillis(ms) | Avro::LocalTimestampMillis(ms) => Value::Time(ms.saturating_mul(1000)),
        Avro::TimestampMicros(us) | Avro::LocalTimestampMicros(us) => Value::Time(*us),
        Avro::TimestampNanos(ns) | Avro::LocalTimestampNanos(ns) => Value::Time(ns.div_euclid(1000)),
    }
}

const MICROS_PER_DAY: i64 = 86_400 * 1_000_000;

/// How deep `fields_of` follows nested records before it declares one whole, as a variant.
const NESTED_RECORDS: usize = 64;

/// The fields `writer`'s schema declares, as a source declares them: each leaf of its record, a nested record's
/// named with dots, each typed as `to_value` reads it. A record nested in itself, or past `NESTED_RECORDS`, is one
/// variant field. Empty for a schema that is no record.
pub fn fields_of(writer: &Parsed) -> Vec<crate::heartbeat::AvroField> {
    let Ok(resolved) = writer.resolved() else {
        return Vec::new();
    };
    let names = resolved.get_names();
    let mut out = Vec::new();
    if let Schema::Record(record) = resolve(&writer.schema, names) {
        leaves(record, "", names, &mut vec![&record.name], &mut out);
    }
    out
}

/// `record`'s leaves into `out`; `within` is every record from the root down to it.
fn leaves<'s>(
    record: &'s apache_avro::schema::RecordSchema,
    prefix: &str,
    names: &NamesRef<'s>,
    within: &mut Vec<&'s apache_avro::schema::Name>,
    out: &mut Vec<crate::heartbeat::AvroField>,
) {
    for field in &record.fields {
        let name = format!("{prefix}{}", field.name);
        match present(&field.schema, names) {
            Some(Schema::Record(inner)) if within.len() <= NESTED_RECORDS && !within.contains(&&inner.name) => {
                within.push(&inner.name);
                leaves(inner, &format!("{name}."), names, within, out);
                within.pop();
            }
            _ => out.push(crate::heartbeat::AvroField {
                name,
                avro_type: spelled(&field.schema),
                suggested: suggested(&field.schema, names).spell(),
            }),
        }
    }
}

/// What `schema` holds where it holds anything: itself, or the one schema a union holds beside `null`.
fn present<'s>(schema: &'s Schema, names: &NamesRef<'s>) -> Option<&'s Schema> {
    match resolve(schema, names) {
        Schema::Null => None,
        Schema::Union(u) => match u
            .variants()
            .iter()
            .filter(|v| !matches!(v, Schema::Null))
            .collect::<Vec<_>>()[..]
        {
            [only] => present(only, names),
            _ => Some(resolve(schema, names)),
        },
        other => Some(other),
    }
}

/// The type a value of `schema` reads as (`to_value`).
fn suggested(schema: &Schema, names: &NamesRef<'_>) -> DataType {
    match present(schema, names) {
        Some(Schema::Boolean) => DataType::Bool,
        Some(Schema::Int | Schema::Long) => DataType::I64,
        Some(Schema::Float | Schema::Double) => DataType::F64,
        Some(Schema::Bytes | Schema::Fixed(_)) => DataType::Bytes,
        Some(Schema::String | Schema::Enum(_) | Schema::Uuid(_) | Schema::Decimal(_) | Schema::BigDecimal) => {
            DataType::Str
        }
        Some(
            Schema::Date
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos,
        ) => DataType::DateTime,
        Some(Schema::TimeMillis | Schema::TimeMicros) => DataType::Duration,
        Some(Schema::Array(a)) => DataType::Array(Box::new(suggested(&a.items, names))),
        // A map, a record `fields_of` declares whole, a union of several, a duration: an object, or a shape only
        // the value knows.
        _ => DataType::Variant,
    }
}

/// `schema` as its schema spells it, briefly: `long`, `long (timestamp-millis)`, `union[null, string]`.
fn spelled(schema: &Schema) -> String {
    let logical = |base: &str, logical: &str| format!("{base} ({logical})");
    match schema {
        Schema::Null => "null".into(),
        Schema::Boolean => "boolean".into(),
        Schema::Int => "int".into(),
        Schema::Long => "long".into(),
        Schema::Float => "float".into(),
        Schema::Double => "double".into(),
        Schema::Bytes => "bytes".into(),
        Schema::String => "string".into(),
        Schema::Array(a) => format!("array<{}>", spelled(&a.items)),
        Schema::Map(m) => format!("map<{}>", spelled(&m.types)),
        Schema::Union(u) => {
            let each: Vec<String> = u.variants().iter().map(spelled).collect();
            format!("union[{}]", each.join(", "))
        }
        Schema::Record(r) => r.name.name().to_string(),
        Schema::Enum(e) => e.name.name().to_string(),
        Schema::Fixed(f) => f.name.name().to_string(),
        Schema::Ref { name } => name.name().to_string(),
        Schema::Decimal(_) => logical("bytes", "decimal"),
        Schema::BigDecimal => logical("bytes", "big-decimal"),
        Schema::Uuid(_) => logical("string", "uuid"),
        Schema::Date => logical("int", "date"),
        Schema::TimeMillis => logical("int", "time-millis"),
        Schema::TimeMicros => logical("long", "time-micros"),
        Schema::TimestampMillis => logical("long", "timestamp-millis"),
        Schema::TimestampMicros => logical("long", "timestamp-micros"),
        Schema::TimestampNanos => logical("long", "timestamp-nanos"),
        Schema::LocalTimestampMillis => logical("long", "local-timestamp-millis"),
        Schema::LocalTimestampMicros => logical("long", "local-timestamp-micros"),
        Schema::LocalTimestampNanos => logical("long", "local-timestamp-nanos"),
        Schema::Duration(_) => logical("fixed", "duration"),
    }
}

/// An Avro value in `core::variant`'s encoding, as `to_value` reads each kind. A null member of a record or map is
/// no entry.
fn put(value: &Avro, schema: &Schema, names: &NamesRef<'_>, out: &mut Vec<u8>) {
    // What a part of the value is read under where the schema does not say: nothing a decoded value can meet, since
    // it was decoded with this schema, but a mismatch reads on rather than failing the row.
    let any = Schema::Null;
    let schema = resolve(schema, names);
    match value {
        Avro::Union(i, inner) => {
            let s = match schema {
                Schema::Union(u) => u.variants().get(*i as usize).unwrap_or(&any),
                _ => &any,
            };
            put(inner, s, names, out);
        }
        Avro::Record(members) => {
            let open = variant::begin_object(out);
            let mut count = 0;
            for (i, (name, v)) in members.iter().enumerate() {
                if is_null(v) {
                    continue;
                }
                let s = match schema {
                    Schema::Record(r) => r.fields.get(i).map_or(&any, |f| &f.schema),
                    _ => &any,
                };
                variant::put_key(out, name);
                put(v, s, names, out);
                count += 1;
            }
            variant::end(out, open, count);
        }
        Avro::Map(entries) => {
            // In key order: a map's own is its encoder's, and the same value is then always the same bytes.
            let mut sorted: Vec<_> = entries.iter().filter(|(_, v)| !is_null(v)).collect();
            sorted.sort_by(|a, b| a.0.cmp(b.0));
            let s = match schema {
                Schema::Map(m) => &*m.types,
                _ => &any,
            };
            let open = variant::begin_object(out);
            for (key, v) in &sorted {
                variant::put_key(out, key);
                put(v, s, names, out);
            }
            variant::end(out, open, sorted.len());
        }
        Avro::Array(items) => {
            let s = match schema {
                Schema::Array(a) => &*a.items,
                _ => &any,
            };
            let open = variant::begin_array(out);
            for item in items {
                put(item, s, names, out);
            }
            variant::end(out, open, items.len());
        }
        Avro::Duration(d) => {
            let open = variant::begin_object(out);
            for (key, n) in [
                ("months", u32::from(d.months())),
                ("days", u32::from(d.days())),
                ("millis", u32::from(d.millis())),
            ] {
                variant::put_key(out, key);
                variant::put_i64(out, n as i64);
            }
            variant::end(out, open, 3);
        }
        scalar => match to_value(scalar, schema, names) {
            Value::Null => variant::put_null(out),
            Value::Bool(b) => variant::put_bool(out, b),
            Value::Int(i) => variant::put_i64(out, i),
            Value::Float(f) => variant::put_f64(out, f),
            Value::Str(s) => variant::put_str(out, &s),
            Value::Bytes(b) => variant::put_bytes(out, &b),
            Value::Time(t) => variant::put_datetime(out, t),
            Value::Duration(d) => variant::put_duration(out, d),
            Value::Json(_) | Value::Array(_) => unreachable!("a container is put above"),
        },
    }
}

fn is_null(value: &Avro) -> bool {
    match value {
        Avro::Null => true,
        Avro::Union(_, inner) => is_null(inner),
        _ => false,
    }
}

/// A decimal's text, exact: `unscaled`, its two's complement digits big-endian, over ten to the `scale`.
pub fn decimal(unscaled: &[u8], scale: usize) -> String {
    let negative = unscaled.first().is_some_and(|b| b & 0x80 != 0);
    // The magnitude, base 256, most significant first.
    let mut magnitude: Vec<u8> = if negative {
        let mut m: Vec<u8> = unscaled.iter().map(|b| !b).collect();
        for b in m.iter_mut().rev() {
            let (sum, carry) = b.overflowing_add(1);
            *b = sum;
            if !carry {
                break;
            }
        }
        m
    } else {
        unscaled.to_vec()
    };
    // Base 10, least significant first, by long division.
    let mut digits = Vec::new();
    while magnitude.iter().any(|b| *b != 0) {
        let mut rest = 0u32;
        for b in magnitude.iter_mut() {
            let n = (rest << 8) | *b as u32;
            *b = (n / 10) as u8;
            rest = n % 10;
        }
        digits.push(b'0' + rest as u8);
    }
    while digits.len() <= scale {
        digits.push(b'0');
    }
    let mut text = String::with_capacity(digits.len() + 2);
    if negative {
        text.push('-');
    }
    for (i, d) in digits.iter().rev().enumerate() {
        if i == digits.len() - scale {
            text.push('.');
        }
        text.push(*d as char);
    }
    text
}

/// Why `name` cannot name an Avro field, if it cannot: Avro's names are a letter or `_`, then letters, digits and `_`.
pub fn refuse_name(name: &str) -> Option<String> {
    let mut chars = name.chars();
    let starts = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    (!starts || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')).then(|| {
        format!("`{name}` is no Avro name: it starts with a letter or `_`, and holds only letters, digits and `_`")
    })
}

/// `schema_of`'s text, and that text parsed.
pub fn parsed_schema_of(arrow: &ArrowSchema) -> Result<(String, Schema), String> {
    let text = schema_of(arrow)?;
    let parsed = Schema::parse_str(&text).map_err(|e| e.to_string())?;
    Ok((text, parsed))
}

/// A route's record, as Parquet's columns, as an Avro record schema's text. Err names a field Avro cannot name.
pub fn schema_of(arrow: &ArrowSchema) -> Result<String, String> {
    let mut taken = Vec::new();
    let record = record_of(arrow.fields(), "Row", &mut taken)?;
    Ok(record.to_string())
}

fn record_of(fields: &Fields, name: &str, taken: &mut Vec<String>) -> Result<serde_json::Value, String> {
    let mut unique = name.to_string();
    let mut n = 1;
    while taken.contains(&unique) {
        n += 1;
        unique = format!("{name}{n}");
    }
    taken.push(unique.clone());
    let mut members = Vec::with_capacity(fields.len());
    for f in fields {
        if let Some(why) = refuse_name(f.name()) {
            return Err(why);
        }
        let ty = type_of(f.data_type(), &format!("{unique}_{}", f.name()), taken)?;
        members.push(serde_json::json!({ "name": f.name(), "type": nullable(ty), "default": null }));
    }
    Ok(serde_json::json!({ "type": "record", "name": unique, "fields": members }))
}

fn nullable(ty: serde_json::Value) -> serde_json::Value {
    if ty == "null" {
        ty
    } else {
        serde_json::json!(["null", ty])
    }
}

fn type_of(arrow: &Arrow, name: &str, taken: &mut Vec<String>) -> Result<serde_json::Value, String> {
    Ok(match arrow {
        Arrow::Null => "null".into(),
        Arrow::Boolean => "boolean".into(),
        Arrow::Int64 => "long".into(),
        Arrow::Float64 => "double".into(),
        Arrow::Utf8 => "string".into(),
        Arrow::Binary => "bytes".into(),
        Arrow::Timestamp(..) => serde_json::json!({ "type": "long", "logicalType": "timestamp-micros" }),
        Arrow::List(item) => {
            serde_json::json!({ "type": "array", "items": nullable(type_of(item.data_type(), name, taken)?) })
        }
        Arrow::Struct(fields) => record_of(fields, name, taken)?,
        other => unreachable!("`io::parquet::Schema` cuts no {other} column"),
    })
}

/// Row `row` of `batch`, as a value of the record schema `schema_of` made of its schema.
pub fn row_of(batch: &RecordBatch, row: usize) -> Avro {
    Avro::Record(
        (batch.schema().fields().iter().zip(batch.columns()))
            .map(|(f, column)| (f.name().clone(), field_value(column.as_ref(), row)))
            .collect(),
    )
}

/// Row `row` of `array`, as its field's union with `null`.
fn field_value(array: &dyn Array, row: usize) -> Avro {
    match array.data_type() {
        Arrow::Null => Avro::Null,
        _ if array.is_null(row) => Avro::Union(0, Box::new(Avro::Null)),
        _ => Avro::Union(1, Box::new(value_at(array, row))),
    }
}

fn value_at(array: &dyn Array, row: usize) -> Avro {
    match array.data_type() {
        Arrow::Null => Avro::Null,
        Arrow::Boolean => Avro::Boolean(array.as_boolean().value(row)),
        Arrow::Int64 => Avro::Long(array.as_primitive::<Int64Type>().value(row)),
        Arrow::Float64 => Avro::Double(array.as_primitive::<Float64Type>().value(row)),
        Arrow::Utf8 => Avro::String(array.as_string::<i32>().value(row).to_string()),
        Arrow::Binary => Avro::Bytes(array.as_binary::<i32>().value(row).to_vec()),
        Arrow::Timestamp(..) => Avro::TimestampMicros(array.as_primitive::<TimestampMicrosecondType>().value(row)),
        Arrow::List(_) => {
            let items = array.as_list::<i32>().value(row);
            Avro::Array((0..items.len()).map(|i| field_value(items.as_ref(), i)).collect())
        }
        Arrow::Struct(fields) => {
            let s = array.as_struct();
            Avro::Record(
                (fields.iter().zip(s.columns()))
                    .map(|(f, column)| (f.name().clone(), field_value(column.as_ref(), row)))
                    .collect(),
            )
        }
        other => unreachable!("`io::parquet::Schema` cuts no {other} column"),
    }
}

/// One record's schema as a Kafka destination writes it: parsed, and its id under each topic it was registered for.
struct Written {
    arrow: SchemaRef,
    /// Its text, as registered, and parsed.
    schema: Result<(String, Schema), String>,
    /// By topic: the id, or why the registry refused it there.
    ids: HashMap<String, Result<u32, String>>,
}

/// How a Kafka destination writes its rows as Avro: the registry it registers each record's schema with, and every
/// schema it has registered.
pub struct AvroWriter {
    registry: Registry,
    written: Vec<Written>,
}

impl AvroWriter {
    pub fn new(config: &SchemaRegistry, tls: Option<&Tls>, secrets: &HeldSecrets) -> Result<AvroWriter, IoError> {
        Ok(AvroWriter {
            registry: Registry::new(config, tls, secrets)?,
            written: Vec::new(),
        })
    }

    /// The place in `written` of `arrow`'s schema, if it has one.
    fn find(&self, arrow: &SchemaRef) -> Option<usize> {
        // By pointer first: every row of a route shares its record's schema.
        (self.written.iter()).position(|w| Arc::ptr_eq(&w.arrow, arrow) || *w.arrow == **arrow)
    }

    /// The place in `written` of `arrow`'s schema, made the first time it is asked.
    fn place(&mut self, arrow: &SchemaRef) -> usize {
        if let Some(at) = self.find(arrow) {
            return at;
        }
        let schema = parsed_schema_of(arrow);
        self.written.push(Written {
            arrow: arrow.clone(),
            schema,
            ids: HashMap::new(),
        });
        self.written.len() - 1
    }

    /// Makes sure `arrow`'s schema is registered for `topic`. Err is a registry that may answer later; a refusal is
    /// kept, and every row of that schema to that topic is refused for it.
    pub fn register(&mut self, topic: &str, arrow: &SchemaRef) -> Result<(), IoError> {
        let at = self.place(arrow);
        if self.written[at].ids.contains_key(topic) {
            return Ok(());
        }
        let id = match &self.written[at].schema {
            Err(why) => Err(why.clone()),
            // Its own text, not its canonical form, which leaves out the logical types and defaults.
            Ok((text, _)) => match self.registry.register(&subject(topic), text) {
                Ok(id) => Ok(id),
                Err(Unread::Refused(why)) => Err(why),
                Err(Unread::Transient(e)) => return Err(e),
            },
        };
        self.written[at].ids.insert(topic.to_string(), id);
        Ok(())
    }

    /// An encoder for the rows of one delivery, each of whose schemas `register` has registered for its topic.
    pub fn encoder(&self) -> Encoder<'_> {
        Encoder {
            writer: self,
            datum: HashMap::new(),
        }
    }
}

/// Encodes rows for one delivery: a datum writer per schema, built the first time a row needs it.
pub struct Encoder<'w> {
    writer: &'w AvroWriter,
    datum: HashMap<usize, Result<GenericDatumWriter<'w>, String>>,
}

impl Encoder<'_> {
    /// Row `row` of `batch`, framed for `topic`. Err is why the row is refused.
    pub fn encode(&mut self, topic: &str, batch: &RecordBatch, row: usize) -> Result<Vec<u8>, String> {
        let arrow = batch.schema_ref();
        let at = self.writer.find(arrow).expect("registered before it is encoded");
        let written = &self.writer.written[at];
        let id = match written.ids.get(topic) {
            Some(Ok(id)) => *id,
            Some(Err(why)) => return Err(why.clone()),
            None => unreachable!("registered for its topic before it is encoded"),
        };
        let datum = self.datum.entry(at).or_insert_with(|| {
            let (_, schema) = written.schema.as_ref().map_err(String::clone)?;
            GenericDatumWriter::builder(schema).build().map_err(|e| e.to_string())
        });
        let datum = datum.as_ref().map_err(String::clone)?;
        let value = datum
            .write_value_to_vec(row_of(batch, row))
            .map_err(|e| format!("a row that does not write as its Avro schema: {e}"))?;
        Ok(frame(id, &value))
    }
}

/// The subject a topic's values are registered under: Confluent's topic name strategy.
pub fn subject(topic: &str) -> String {
    format!("{topic}-value")
}

#[cfg(test)]
mod tests;
