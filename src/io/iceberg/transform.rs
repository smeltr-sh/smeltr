//! Partition transforms, and the partitioner that follows the table's spec.
//!
//! A row's partition is each spec field's transform applied to its source column. The partition travels as text,
//! the Hive-style path segment Iceberg's own writer makes (`path_text`); a committer reads the typed value back from
//! that text (`literal_of`) for the manifest.
//!
//! ```mermaid
//! flowchart LR
//!   C[source column<br/>of the row] -->|apply| L[Literal or null]
//!   L -->|path_text| T["name=text<br/>Partition"]
//!   T -->|literal_of| M[Literal<br/>for the manifest]
//! ```

use base64::Engine as _;

use crate::io::OwnedRecord;
use crate::io::files::{Partition, Partitioner};
use arrow_schema::{DataType as Arrow, Schema as ArrowSchema};

use super::schema::{primitive_arrow, write_default};
use crate::io::iceberg::metadata::{Literal, NestedField, PartitionSpec, Primitive, Schema, Transform, Type};
use crate::io::table::{
    Kind, column_at, date_days, iso_date, parse_date, parse_timestamp, read, signed_parts, url_encode,
};
use crate::ops::civil::{MICROS_PER_DAY, civil_from_days};

const MICROS_PER_HOUR: i64 = 3_600_000_000;

/// One `partitionBy` term: `ts`, `day(ts)`, `bucket(16, id)`, `truncate(4, name)`. A nested column is named by its
/// path, `http.host`. Gives the transform and the column's path. Err says what the term should look like.
pub fn partition_term(term: &str) -> Result<(Transform, Vec<String>), String> {
    let refused = || {
        format!(
            "{term:?} is no partition term: write a column, or year, month, day or hour of one (day(ts)), or \
             bucket(n, column) or truncate(width, column)"
        )
    };
    let term = term.trim();
    let (transform, column) = match term.split_once('(') {
        None => (Transform::Identity, term),
        Some((name, rest)) => {
            let inner = rest.strip_suffix(')').ok_or_else(refused)?;
            let width = |n: &str| n.trim().parse::<u32>().ok().filter(|&n| n > 0).ok_or_else(refused);
            match (name.trim(), inner.split_once(',')) {
                ("year", None) => (Transform::Year, inner),
                ("month", None) => (Transform::Month, inner),
                ("day", None) => (Transform::Day, inner),
                ("hour", None) => (Transform::Hour, inner),
                ("bucket", Some((n, column))) => (Transform::Bucket(width(n)?), column),
                ("truncate", Some((w, column))) => (Transform::Truncate(width(w)?), column),
                _ => return Err(refused()),
            }
        }
    };
    let path: Vec<String> = column.trim().split('.').map(str::to_string).collect();
    if path
        .iter()
        .any(|name| name.is_empty() || name.contains(['(', ')', ',']))
    {
        return Err(refused());
    }
    Ok((transform, path))
}

/// The partition field's name Iceberg gives a term: the column's for identity, else the column's with the
/// transform's suffix (ts_day, id_bucket, name_trunc).
pub fn partition_name(transform: &Transform, path: &[String]) -> String {
    let column = path.join(".");
    match transform {
        Transform::Identity | Transform::Other(_) => column,
        Transform::Year => format!("{column}_year"),
        Transform::Month => format!("{column}_month"),
        Transform::Day => format!("{column}_day"),
        Transform::Hour => format!("{column}_hour"),
        Transform::Bucket(_) => format!("{column}_bucket"),
        Transform::Truncate(_) => format!("{column}_trunc"),
        Transform::Void => format!("{column}_null"),
    }
}

/// The type `transform` makes of a `source` value. Err when the spec does not define the pair.
pub fn result_type(transform: &Transform, source: &Primitive) -> Result<Primitive, String> {
    use Primitive as P;
    let supported = match transform {
        Transform::Identity => !matches!(source, P::Other(_)),
        Transform::Year | Transform::Month | Transform::Day => {
            matches!(source, P::Date | P::Timestamp | P::Timestamptz)
        }
        Transform::Hour => matches!(source, P::Timestamp | P::Timestamptz),
        Transform::Bucket(n) => {
            *n > 0
                && matches!(
                    source,
                    P::Int | P::Long | P::Date | P::Timestamp | P::Timestamptz | P::String | P::Binary
                )
        }
        Transform::Truncate(w) => *w > 0 && matches!(source, P::Int | P::Long | P::String | P::Binary),
        Transform::Void => true,
        Transform::Other(_) => false,
    };
    if !supported {
        return Err(format!(
            "the transform {transform} cannot be applied to a {source} column"
        ));
    }
    Ok(match transform {
        Transform::Year | Transform::Month | Transform::Hour | Transform::Bucket(_) => P::Int,
        Transform::Day => P::Date,
        _ => source.clone(),
    })
}

/// `value` transformed, as the spec defines it. None for void, and for a pair `result_type` refuses. A time too far
/// from 1970 for an `int` of hours is None too.
pub fn apply(transform: &Transform, value: &Literal) -> Option<Literal> {
    match transform {
        Transform::Identity => Some(value.clone()),
        Transform::Year => Some(Literal::Int(
            i32::try_from(civil_from_days(days_of(value)?).0 - 1970).ok()?,
        )),
        Transform::Month => {
            let (y, m, _) = civil_from_days(days_of(value)?);
            Some(Literal::Int(i32::try_from((y - 1970) * 12 + m as i64 - 1).ok()?))
        }
        Transform::Day => Some(Literal::Date(i32::try_from(days_of(value)?).ok()?)),
        Transform::Hour => match value {
            Literal::Micros(us) => Some(Literal::Int(i32::try_from(us.div_euclid(MICROS_PER_HOUR)).ok()?)),
            _ => None,
        },
        Transform::Bucket(n) if *n > 0 => {
            let hash = match value {
                Literal::Int(v) | Literal::Date(v) => murmur3_x86_32(&(*v as i64).to_le_bytes()),
                Literal::Long(v) | Literal::Micros(v) => murmur3_x86_32(&v.to_le_bytes()),
                Literal::String(s) => murmur3_x86_32(s.as_bytes()),
                Literal::Binary(b) => murmur3_x86_32(b),
                _ => return None,
            };
            Some(Literal::Int(((hash & i32::MAX) as i64 % *n as i64) as i32))
        }
        Transform::Truncate(w) if *w > 0 => match value {
            Literal::Int(v) => {
                let (v, w) = (*v as i64, *w as i64);
                Some(Literal::Int(i32::try_from(v - (v % w + w) % w).ok()?))
            }
            Literal::Long(v) => {
                let (v, w) = (*v as i128, *w as i128);
                Some(Literal::Long(i64::try_from(v - (v % w + w) % w).ok()?))
            }
            Literal::String(s) => Some(Literal::String(s.chars().take(*w as usize).collect())),
            Literal::Binary(b) => Some(Literal::Binary(b[..b.len().min(*w as usize)].to_vec())),
            _ => None,
        },
        _ => None,
    }
}

/// Days since 1970 of a date or a time.
fn days_of(value: &Literal) -> Option<i64> {
    match value {
        Literal::Date(d) => Some(*d as i64),
        Literal::Micros(us) => Some(us.div_euclid(MICROS_PER_DAY)),
        _ => None,
    }
}

/// MurmurHash3, x86 32-bit, seed 0: the hash the spec buckets by.
pub fn murmur3_x86_32(data: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mix = |k: u32| k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
    let mut h: u32 = 0;
    let (blocks, tail) = data.as_chunks::<4>();
    for block in blocks {
        h ^= mix(u32::from_le_bytes(*block));
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let k = tail.iter().rev().fold(0u32, |k, &b| (k << 8) | b as u32);
        h ^= mix(k);
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

/// A partition value of the `result` type `transform` makes, as it reads in a data file's path, escaped as Java's
/// URLEncoder escapes it, as Iceberg's own writer does: `2026-10-10` for a day, `2026-10-10-13` for an hour, `null`
/// for null. A string or a binary's Base64 that reads `null` escapes its first letter, so that `literal_of` can tell
/// it from null.
pub fn path_text(value: Option<&Literal>, result: &Primitive, transform: &Transform) -> String {
    let Some(value) = value else {
        return NULL.to_string();
    };
    let human = match (value, transform) {
        (Literal::Int(v), Transform::Year) => format!("{:04}", 1970 + *v as i64),
        (Literal::Int(v), Transform::Month) => {
            let (y, m) = ((*v as i64).div_euclid(12), (*v as i64).rem_euclid(12));
            format!("{:04}-{:02}", 1970 + y, m + 1)
        }
        (Literal::Int(v), Transform::Hour) => {
            let hours = *v as i64;
            let (y, m, d) = civil_from_days(hours.div_euclid(24));
            format!("{y:04}-{m:02}-{d:02}-{:02}", hours.rem_euclid(24))
        }
        (Literal::Boolean(b), _) => b.to_string(),
        (Literal::Int(v), _) => v.to_string(),
        (Literal::Long(v), _) => v.to_string(),
        (Literal::Double(v), _) if *result == Primitive::Float => java_float(*v as f32),
        (Literal::Double(v), _) => java_double(*v),
        (Literal::Date(d), _) => iso_date(*d as i64),
        (Literal::Micros(us), _) => {
            let (day, time) = (us.div_euclid(MICROS_PER_DAY), us.rem_euclid(MICROS_PER_DAY));
            let secs = time / 1_000_000;
            let zone = if *result == Primitive::Timestamp { "" } else { "+00:00" };
            format!(
                "{}T{:02}:{:02}:{:02}.{:06}{zone}",
                iso_date(day),
                secs / 3600,
                secs / 60 % 60,
                secs % 60,
                time % 1_000_000
            )
        }
        (Literal::String(s), _) => s.clone(),
        (Literal::Binary(b), _) => base64::engine::general_purpose::STANDARD.encode(b),
    };
    if human == NULL {
        return "%6Eull".to_string();
    }
    url_encode(&human)
}

/// What a null partition value reads as.
const NULL: &str = "null";

/// The value `path_text` wrote as `text`, of the `result` type `transform` makes. None for null.
pub fn literal_of(result: &Primitive, transform: &Transform, text: &str) -> Result<Option<Literal>, String> {
    if text == NULL {
        return Ok(None);
    }
    let human = url_decode(text).ok_or_else(|| format!("{text:?} is not an escaped partition value"))?;
    let bad = || format!("{text:?} is no {transform} partition value of type {result}");
    let int = |v: i64| i32::try_from(v).map(Literal::Int).map_err(|_| bad());
    let value = match (result, transform) {
        (Primitive::Int, Transform::Year) => int(signed_parts(&human, 1).ok_or_else(bad)?[0] - 1970)?,
        (Primitive::Int, Transform::Month) => {
            let [y, m] = signed_parts(&human, 2).ok_or_else(bad)?[..] else {
                return Err(bad());
            };
            if !(1..=12).contains(&m) {
                return Err(bad());
            }
            int((y - 1970) * 12 + m - 1)?
        }
        (Primitive::Int, Transform::Hour) => {
            let [y, m, d, h] = signed_parts(&human, 4).ok_or_else(bad)?[..] else {
                return Err(bad());
            };
            if !(0..24).contains(&h) {
                return Err(bad());
            }
            int(date_days(y, m, d).ok_or_else(bad)? * 24 + h)?
        }
        (Primitive::Boolean, _) => Literal::Boolean(human.parse().map_err(|_| bad())?),
        (Primitive::Int, _) => Literal::Int(human.parse().map_err(|_| bad())?),
        (Primitive::Long, _) => Literal::Long(human.parse().map_err(|_| bad())?),
        (Primitive::Float, _) => Literal::Double(human.parse::<f32>().map_err(|_| bad())? as f64),
        (Primitive::Double, _) => Literal::Double(human.parse().map_err(|_| bad())?),
        (Primitive::Date, _) => {
            let days = parse_date(&human).ok_or_else(bad)?;
            Literal::Date(i32::try_from(days).map_err(|_| bad())?)
        }
        (Primitive::Timestamp, _) => Literal::Micros(parse_timestamp(&human).ok_or_else(bad)?),
        (Primitive::Timestamptz, _) => {
            let time = human.strip_suffix("+00:00").or_else(|| human.strip_suffix('Z'));
            Literal::Micros(time.and_then(parse_timestamp).ok_or_else(bad)?)
        }
        (Primitive::String, _) => Literal::String(human),
        (Primitive::Binary, _) => Literal::Binary(
            base64::engine::general_purpose::STANDARD
                .decode(&human)
                .map_err(|_| bad())?,
        ),
        _ => return Err(bad()),
    };
    Ok(Some(value))
}

/// Java's `Double.toString`, which Iceberg writes a double's partition value with: plain from 10⁻³ up to 10⁷,
/// `1.0E10` beyond.
fn java_double(v: f64) -> String {
    java_number(v, format!("{v:?}"), format!("{v:e}"))
}

/// Java's `Float.toString`, as `java_double` for a float: the fewest digits that read back as `v`.
fn java_float(v: f32) -> String {
    java_number(v as f64, format!("{v:?}"), format!("{v:e}"))
}

/// `v` as Java writes it, from its shortest digits as `{:?}` and `{:e}` write them.
fn java_number(v: f64, plain: String, sci: String) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    let magnitude = v.abs();
    if magnitude == 0.0 || (1e-3..1e7).contains(&magnitude) {
        return plain;
    }
    let (mantissa, exponent) = sci.split_once('e').expect("{:e} writes an exponent");
    let dot = if mantissa.contains('.') { "" } else { ".0" };
    format!("{mantissa}{dot}E{exponent}")
}

fn url_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

/// Partitions rows by a table's spec: one path segment per spec field, in the spec's order.
pub struct TablePartitioner {
    fields: Vec<SpecField>,
}

struct SpecField {
    /// The partition field's name.
    name: String,
    /// The source column's names from the record's root, its type, and the type the transform makes of it. `None` for
    /// void, which reads no source: its column may since have been dropped.
    source: Option<(Vec<String>, Kind, Primitive)>,
    /// The source's value in a row whose record lacks its names from the `k`th on, for each `k`.
    lacked: Vec<Option<Literal>>,
    transform: Transform,
}

impl TablePartitioner {
    /// Err when a spec field's source is not a primitive column of `schema` outside any list, or its transform cannot
    /// be applied to it.
    pub fn new(spec: &PartitionSpec, schema: &Schema) -> Result<TablePartitioner, String> {
        let fields = spec
            .fields
            .iter()
            .map(|field| {
                if field.transform == Transform::Void {
                    return Ok(SpecField {
                        name: field.name.clone(),
                        source: None,
                        lacked: Vec::new(),
                        transform: Transform::Void,
                    });
                }
                let (source, ty) = source_path(&schema.fields, field.source_id, &mut Vec::new())?.ok_or_else(|| {
                    format!(
                        "the partition field {} names column id {}, which the table's schema lacks",
                        field.name, field.source_id
                    )
                })?;
                let Type::Primitive(source_type) = ty else {
                    return Err(format!(
                        "the partition field {} is of {}, a {ty} column, and a partition's source is a primitive",
                        field.name,
                        source.join(".")
                    ));
                };
                let result = result_type(&field.transform, source_type)
                    .map_err(|e| format!("the partition field {}: {e}", field.name))?;
                let kind = source_type.kind().expect("result_type refuses Other");
                let lacked = lacked_values(&schema.fields, &source, &kind)?;
                Ok(SpecField {
                    name: field.name.clone(),
                    source: Some((source, kind, result)),
                    lacked,
                    transform: field.transform.clone(),
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(TablePartitioner { fields })
    }
}

/// What a row whose record lacks `names` from the `k`th on holds at them, for each `k`: the leaf's write default where
/// every struct from the `k`th on takes a struct by default, null otherwise.
fn lacked_values(fields: &[NestedField], names: &[String], kind: &Kind) -> Result<Vec<Option<Literal>>, String> {
    let mut path: Vec<&NestedField> = Vec::with_capacity(names.len());
    for name in names {
        let fields = match path.last().map(|f| &f.field_type) {
            Some(Type::Struct(s)) => &s.fields,
            _ => fields,
        };
        path.push(fields.iter().find(|f| f.name == *name).expect("a source's path"));
    }
    let (leaf, structs) = path.split_last().expect("a source has a name");
    let Type::Primitive(p) = &leaf.field_type else {
        unreachable!("a source is a primitive")
    };
    let arrow = primitive_arrow(p).expect("a primitive of a kind");
    let value = read(&write_default(leaf, &arrow, &names.join("."))?, 0, kind, names)?;
    let defaulted = |f: &&NestedField| f.other.get("write-default").is_some_and(|d| !d.is_null());
    Ok((0..names.len())
        .map(|k| structs[k..].iter().all(defaulted).then(|| value.clone()).flatten())
        .collect())
}

/// The first of `names` that `schema`, a row's, lacks along them.
fn lacked_from(schema: &ArrowSchema, names: &[String]) -> Option<usize> {
    let mut fields = schema.fields();
    for (k, name) in names.iter().enumerate() {
        let Some((_, field)) = fields.find(name) else {
            return Some(k);
        };
        match field.data_type() {
            Arrow::Struct(children) => fields = children,
            _ => return None,
        }
    }
    None
}

/// The names leading to column `id` among `fields`, and its type. Err when it is inside a list, which has no one
/// value per row.
fn source_path<'s>(
    fields: &'s [NestedField],
    id: i32,
    at: &mut Vec<String>,
) -> Result<Option<(Vec<String>, &'s Type)>, String> {
    for field in fields {
        at.push(field.name.clone());
        if field.id == id {
            return Ok(Some((at.clone(), &field.field_type)));
        }
        let inner = match &field.field_type {
            Type::Struct(s) => source_path(&s.fields, id, at)?,
            Type::List(l) => match &*l.element {
                Type::Struct(s) => source_path(&s.fields, id, &mut Vec::new())?
                    .map(|_| Err(format!("column id {id} is inside the list {}", at.join("."))))
                    .transpose()?,
                _ => None,
            },
            _ => None,
        };
        if inner.is_some() {
            return Ok(inner);
        }
        at.pop();
    }
    Ok(None)
}

impl Partitioner for TablePartitioner {
    fn partition(&self, row: &OwnedRecord) -> Result<Partition, String> {
        let columns = row
            .columns
            .as_ref()
            .ok_or("the row has no columns, and a table is partitioned by its columns")?;
        let batch = columns.chunk.batch();
        let at = columns.row as usize;
        let mut path = Vec::with_capacity(self.fields.len());
        let mut values = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let text = match &field.source {
                None => NULL.to_string(),
                Some((source, source_type, result)) => {
                    let value = match lacked_from(batch.schema_ref(), source) {
                        None => (column_at(batch, source, at)?)
                            .map(|c| read(&c, at, source_type, source))
                            .transpose()?
                            .flatten(),
                        // Where the record lacks the source, the file holds its default, under structs a row holds.
                        Some(k) => (k == 0 || column_at(batch, &source[..k], at)?.is_some_and(|p| p.is_valid(at)))
                            .then(|| field.lacked[k].clone())
                            .flatten(),
                    };
                    let value = value.and_then(|v| apply(&field.transform, &v));
                    path_text(value.as_ref(), result, &field.transform)
                }
            };
            path.push(format!("{}={text}", url_encode(&field.name)));
            values.push((field.name.clone(), text));
        }
        Ok(Partition { part: 0, path, values })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::core::buffer::BufferPool;
    use crate::core::column::{Column, DataType, StrBuilder, VarType};
    use crate::format::record::RecordView;
    use crate::io::iceberg::metadata::{ListType, PartitionField, StructType};
    use crate::io::parquet::{self, Chunk, Columns};

    /// 2017-11-16T22:31:08 UTC, the spec's appendix time.
    const SPEC_TIME: i64 = 1_510_871_468_000_000;

    #[test]
    fn partition_terms_read_as_written() {
        let path = |p: &str| p.split('.').map(str::to_string).collect::<Vec<_>>();
        assert_eq!(partition_term("host"), Ok((Transform::Identity, path("host"))));
        assert_eq!(partition_term("day(ts)"), Ok((Transform::Day, path("ts"))));
        assert_eq!(
            partition_term(" bucket(16, user.id) "),
            Ok((Transform::Bucket(16), path("user.id")))
        );
        assert_eq!(
            partition_term("truncate(4,name)"),
            Ok((Transform::Truncate(4), path("name")))
        );
        for bad in [
            "",
            "day()",
            "bucket(0, id)",
            "bucket(id)",
            "week(ts)",
            "day(ts",
            "a..b",
            "day(a, b)",
        ] {
            assert!(partition_term(bad).is_err(), "{bad:?} should be refused");
        }
        assert_eq!(partition_name(&Transform::Day, &path("ts")), "ts_day");
        assert_eq!(partition_name(&Transform::Identity, &path("http.host")), "http.host");
    }

    #[test]
    fn hashes_match_the_spec_appendix() {
        let long = |v: i64| murmur3_x86_32(&v.to_le_bytes());
        assert_eq!(long(34), 2_017_239_379);
        assert_eq!(long(17_486), -653_330_422);
        assert_eq!(long(SPEC_TIME), -2_047_944_441);
        assert_eq!(murmur3_x86_32(b"iceberg"), 1_210_000_089);
        assert_eq!(murmur3_x86_32(&[0, 1, 2, 3]), -188_683_207);
        // An int and a date hash as the long of the same value.
        let bucket = |v: Literal| apply(&Transform::Bucket(i32::MAX as u32), &v);
        assert_eq!(bucket(Literal::Int(34)), Some(Literal::Int(2_017_239_379)));
        assert_eq!(bucket(Literal::Long(34)), Some(Literal::Int(2_017_239_379)));
        assert_eq!(
            bucket(Literal::Date(17_486)),
            Some(Literal::Int(-653_330_422 & i32::MAX))
        );
    }

    #[test]
    fn a_bucket_masks_the_sign_and_takes_the_remainder() {
        let hash = -2_047_944_441i32;
        assert_eq!(
            apply(&Transform::Bucket(16), &Literal::Micros(SPEC_TIME)),
            Some(Literal::Int((hash & i32::MAX) % 16))
        );
        assert_eq!(
            apply(&Transform::Bucket(100), &Literal::String("iceberg".into())),
            Some(Literal::Int(1_210_000_089 % 100))
        );
        assert_eq!(apply(&Transform::Bucket(4), &Literal::Double(1.0)), None);
    }

    #[test]
    fn time_transforms_count_from_1970() {
        let t = Literal::Micros(SPEC_TIME);
        assert_eq!(apply(&Transform::Year, &t), Some(Literal::Int(47)));
        assert_eq!(apply(&Transform::Month, &t), Some(Literal::Int(47 * 12 + 10)));
        assert_eq!(apply(&Transform::Day, &t), Some(Literal::Date(17_486)));
        assert_eq!(apply(&Transform::Hour, &t), Some(Literal::Int(17_486 * 24 + 22)));
        let d = Literal::Date(17_486);
        assert_eq!(apply(&Transform::Year, &d), Some(Literal::Int(47)));
        assert_eq!(apply(&Transform::Month, &d), Some(Literal::Int(574)));
        assert_eq!(apply(&Transform::Day, &d), Some(d.clone()));
        assert_eq!(apply(&Transform::Hour, &d), None);
        // Before 1970 counts down, a whole unit at a time.
        let before = Literal::Micros(-1);
        assert_eq!(apply(&Transform::Year, &before), Some(Literal::Int(-1)));
        assert_eq!(apply(&Transform::Month, &before), Some(Literal::Int(-1)));
        assert_eq!(apply(&Transform::Day, &before), Some(Literal::Date(-1)));
        assert_eq!(apply(&Transform::Hour, &before), Some(Literal::Int(-1)));
    }

    #[test]
    fn truncate_floors_numbers_and_shortens_text() {
        let t = |w, v| apply(&Transform::Truncate(w), &v);
        assert_eq!(t(10, Literal::Int(1)), Some(Literal::Int(0)));
        assert_eq!(t(10, Literal::Int(-1)), Some(Literal::Int(-10)));
        assert_eq!(t(10, Literal::Long(-10)), Some(Literal::Long(-10)));
        assert_eq!(t(10, Literal::Long(i64::MIN)), None);
        assert_eq!(
            t(3, Literal::String("iceberg".into())),
            Some(Literal::String("ice".into()))
        );
        assert_eq!(t(2, Literal::String("łąka".into())), Some(Literal::String("łą".into())));
        assert_eq!(
            t(3, Literal::Binary(vec![0, 1, 2, 3])),
            Some(Literal::Binary(vec![0, 1, 2]))
        );
        assert_eq!(t(9, Literal::Binary(vec![7])), Some(Literal::Binary(vec![7])));
    }

    #[test]
    fn identity_keeps_and_void_drops() {
        let v = Literal::String("a".into());
        assert_eq!(apply(&Transform::Identity, &v), Some(v.clone()));
        assert_eq!(apply(&Transform::Void, &v), None);
        assert_eq!(apply(&Transform::Other("zorder".into()), &v), None);
    }

    #[test]
    fn result_types_follow_the_spec() {
        use Primitive as P;
        let ok = |t: Transform, s: P| result_type(&t, &s).unwrap();
        assert_eq!(ok(Transform::Identity, P::Timestamptz), P::Timestamptz);
        assert_eq!(ok(Transform::Day, P::Timestamptz), P::Date);
        assert_eq!(ok(Transform::Hour, P::Timestamptz), P::Int);
        assert_eq!(ok(Transform::Month, P::Date), P::Int);
        assert_eq!(ok(Transform::Bucket(8), P::String), P::Int);
        assert_eq!(ok(Transform::Truncate(8), P::Long), P::Long);
        assert_eq!(ok(Transform::Void, P::Double), P::Double);
        assert_eq!(ok(Transform::Identity, P::Float), P::Float);
        assert_eq!(ok(Transform::Day, P::Timestamp), P::Date);
        assert_eq!(ok(Transform::Bucket(8), P::Timestamp), P::Int);
        for (t, s) in [
            (Transform::Hour, P::Date),
            (Transform::Bucket(8), P::Double),
            (Transform::Bucket(8), P::Float),
            (Transform::Bucket(0), P::Long),
            (Transform::Truncate(4), P::Boolean),
            (Transform::Truncate(4), P::Timestamp),
            (Transform::Other("zorder".into()), P::Long),
            (Transform::Identity, P::Other("decimal(9,2)".into())),
        ] {
            assert!(result_type(&t, &s).is_err(), "{t} of {s}");
        }
    }

    #[test]
    fn path_text_reads_as_iceberg_writes_it_and_back() {
        use Primitive as P;
        let day = 20_736; // 2026-10-10
        let at_13 = day as i64 * MICROS_PER_DAY + 13 * MICROS_PER_HOUR + 5 * 60_000_000 + 9_000_123;
        for (value, result, transform, text) in [
            (Some(Literal::Date(day)), P::Date, Transform::Day, "2026-10-10"),
            (
                Some(Literal::Int(day * 24 + 13)),
                P::Int,
                Transform::Hour,
                "2026-10-10-13",
            ),
            (Some(Literal::Int(56 * 12 + 9)), P::Int, Transform::Month, "2026-10"),
            (Some(Literal::Int(56)), P::Int, Transform::Year, "2026"),
            (Some(Literal::Int(-1)), P::Int, Transform::Hour, "1969-12-31-23"),
            (Some(Literal::Int(-1)), P::Int, Transform::Month, "1969-12"),
            (Some(Literal::Int(7)), P::Int, Transform::Bucket(16), "7"),
            (Some(Literal::Long(-30)), P::Long, Transform::Truncate(10), "-30"),
            (Some(Literal::Boolean(true)), P::Boolean, Transform::Identity, "true"),
            (Some(Literal::Double(1.5)), P::Double, Transform::Identity, "1.5"),
            (Some(Literal::Double(1e10)), P::Double, Transform::Identity, "1.0E10"),
            (
                Some(Literal::Double(-2.5e-7)),
                P::Double,
                Transform::Identity,
                "-2.5E-7",
            ),
            (
                Some(Literal::Double(0.1f32 as f64)),
                P::Float,
                Transform::Identity,
                "0.1",
            ),
            (
                Some(Literal::Double(1e10f32 as f64)),
                P::Float,
                Transform::Identity,
                "1.0E10",
            ),
            (
                Some(Literal::Date(-719_528)),
                P::Date,
                Transform::Identity,
                "0000-01-01",
            ),
            (
                Some(Literal::Micros(at_13)),
                P::Timestamptz,
                Transform::Identity,
                "2026-10-10T13%3A05%3A09.000123%2B00%3A00",
            ),
            (
                Some(Literal::Micros(at_13)),
                P::Timestamp,
                Transform::Identity,
                "2026-10-10T13%3A05%3A09.000123",
            ),
            (
                Some(Literal::String("a b/ü=1".into())),
                P::String,
                Transform::Identity,
                "a+b%2F%C3%BC%3D1",
            ),
            (
                Some(Literal::String("null".into())),
                P::String,
                Transform::Identity,
                "%6Eull",
            ),
            (
                Some(Literal::Binary(vec![0xfb, 0xff])),
                P::Binary,
                Transform::Identity,
                "%2B%2F8%3D",
            ),
            (
                Some(Literal::Binary(vec![0x9e, 0xe9, 0x65])),
                P::Binary,
                Transform::Identity,
                "%6Eull",
            ),
            (None, P::String, Transform::Identity, "null"),
            (None, P::Int, Transform::Void, "null"),
        ] {
            assert_eq!(
                path_text(value.as_ref(), &result, &transform),
                text,
                "{value:?} by {transform}"
            );
            assert_eq!(literal_of(&result, &transform, text), Ok(value), "{text}");
        }
    }

    #[test]
    fn path_text_round_trips_across_the_calendar() {
        for days in (-800_000..800_000).step_by(997) {
            for (transform, value) in [
                (Transform::Day, apply(&Transform::Day, &Literal::Date(days))),
                (Transform::Month, apply(&Transform::Month, &Literal::Date(days))),
                (Transform::Year, apply(&Transform::Year, &Literal::Date(days))),
                (
                    Transform::Hour,
                    apply(&Transform::Hour, &Literal::Micros(days as i64 * MICROS_PER_DAY + 7)),
                ),
                (
                    Transform::Identity,
                    Some(Literal::Micros(days as i64 * MICROS_PER_DAY - 1)),
                ),
            ] {
                let source = match transform {
                    Transform::Hour | Transform::Identity => Primitive::Timestamptz,
                    _ => Primitive::Date,
                };
                let result = result_type(&transform, &source).unwrap();
                let text = path_text(value.as_ref(), &result, &transform);
                assert_eq!(literal_of(&result, &transform, &text), Ok(value), "{text}");
            }
        }
    }

    #[test]
    fn literal_of_refuses_what_path_text_never_writes() {
        use Primitive as P;
        for (result, transform, text) in [
            (P::Date, Transform::Day, "2026-02-30"),
            (P::Int, Transform::Month, "2026-13"),
            (P::Int, Transform::Hour, "2026-10-10-24"),
            (P::Int, Transform::Year, "twenty"),
            (P::Timestamptz, Transform::Identity, "2026-10-10T13:05:09.1234567+00:00"),
            (P::Timestamptz, Transform::Identity, "2026-10-10T13:05:09.123456"),
            (P::Timestamp, Transform::Identity, "2026-10-10T13:05:09.123456+00:00"),
            (P::String, Transform::Identity, "%zz"),
            (P::Other("uuid".into()), Transform::Identity, "x"),
        ] {
            assert!(literal_of(&result, &transform, text).is_err(), "{text}");
        }
    }

    fn field(id: i32, name: &str, field_type: Type) -> NestedField {
        NestedField {
            id,
            name: name.into(),
            required: false,
            field_type,
            doc: None,
            other: Default::default(),
        }
    }

    fn table_schema() -> Schema {
        let p = Type::Primitive;
        Schema {
            kind: Default::default(),
            schema_id: 0,
            identifier_field_ids: Vec::new(),
            fields: vec![
                field(1, "ts", p(Primitive::Timestamptz)),
                field(2, "host", p(Primitive::String)),
                field(3, "n", p(Primitive::Long)),
                field(
                    4,
                    "ctx",
                    Type::Struct(StructType {
                        fields: vec![field(5, "region", p(Primitive::String))],
                    }),
                ),
                field(
                    6,
                    "items",
                    Type::List(ListType {
                        element_id: 7,
                        element_required: false,
                        element: Box::new(Type::Struct(StructType {
                            fields: vec![field(8, "sku", p(Primitive::String))],
                        })),
                    }),
                ),
                field(9, "ratio", p(Primitive::Double)),
            ],
        }
    }

    fn spec(fields: &[(&str, Transform, i32)]) -> PartitionSpec {
        PartitionSpec {
            spec_id: 0,
            fields: fields
                .iter()
                .enumerate()
                .map(|(i, (name, transform, source_id))| PartitionField {
                    source_ids: None,
                    name: name.to_string(),
                    transform: transform.clone(),
                    source_id: *source_id,
                    field_id: 1000 + i as i32,
                })
                .collect(),
        }
    }

    fn str_col(pool: &mut BufferPool, vals: &[Option<&str>]) -> Column {
        let mut sb = StrBuilder::new(pool, VarType::Str, vals.len());
        sb.reserve(pool, vals.iter().flatten().map(|s| s.len()).sum());
        for v in vals {
            match v {
                Some(s) => sb.push(s.as_bytes()),
                None => sb.push_null(),
            }
        }
        sb.finish()
    }

    /// Rows of `{ts, host, n, ctx: {region}}` cut to Parquet's columns, as egress cuts them.
    fn rows() -> Vec<OwnedRecord> {
        let mut pool = BufferPool::new();
        let mut ts = Column::alloc(&mut pool, &DataType::DateTime, 3);
        ts.datetime_mut().unwrap().values.copy_from_slice(&[SPEC_TIME, 0, -1]);
        let mut n = Column::alloc(&mut pool, &DataType::I64, 3);
        n.i64_mut().unwrap().values.copy_from_slice(&[34, 7, 0]);
        n.i64_mut().unwrap().validity.set_valid(2, false);
        let host = str_col(&mut pool, &[Some("a b"), None, Some("c")]);
        let region = str_col(&mut pool, &[Some("eu"), Some("us"), None]);
        let record = DataType::Struct(
            vec![
                ("ts".into(), DataType::DateTime),
                ("host".into(), DataType::Str),
                ("n".into(), DataType::I64),
                (
                    "ctx".into(),
                    DataType::Struct(vec![("region".into(), DataType::Str)], false),
                ),
            ],
            false,
        );
        let schema = parquet::Schema::new(&record).unwrap();
        let chunk = {
            let view = RecordView::from_named([("ts", &ts), ("host", &host), ("n", &n), ("ctx.region", &region)]);
            Arc::new(Chunk::cut(&schema, &[(&view, &[0, 1, 2])], 0, &mut pool))
        };
        for c in [ts, n, host, region] {
            c.reclaim(&mut pool);
        }
        (0..3)
            .map(|row| OwnedRecord {
                columns: Some(Columns {
                    chunk: chunk.clone(),
                    row,
                }),
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn rows_partition_by_the_spec_in_its_order() {
        let partitioner = TablePartitioner::new(
            &spec(&[
                ("ts_day", Transform::Day, 1),
                ("ts_hour", Transform::Hour, 1),
                ("host", Transform::Identity, 2),
                ("n_bucket", Transform::Bucket(16), 3),
                ("region", Transform::Truncate(1), 5),
                ("gone", Transform::Void, 3),
            ]),
            &table_schema(),
        )
        .unwrap();
        let rows = rows();
        let first = partitioner.partition(&rows[0]).unwrap();
        assert_eq!(first.part, 0);
        assert_eq!(
            first.path,
            [
                "ts_day=2017-11-16",
                "ts_hour=2017-11-16-22",
                "host=a+b",
                &format!("n_bucket={}", 2_017_239_379 % 16),
                "region=e",
                "gone=null",
            ]
        );
        assert_eq!(first.values[2], ("host".to_string(), "a+b".to_string()));
        assert_eq!(
            literal_of(&Primitive::String, &Transform::Identity, &first.values[2].1),
            Ok(Some(Literal::String("a b".into())))
        );

        let second = partitioner.partition(&rows[1]).unwrap();
        assert_eq!(second.path[0], "ts_day=1970-01-01");
        assert_eq!(second.path[2], "host=null");
        assert_eq!(second.path[4], "region=u");

        let third = partitioner.partition(&rows[2]).unwrap();
        assert_eq!(third.path[0], "ts_day=1969-12-31");
        assert_eq!(third.path[3], "n_bucket=null");
        assert_eq!(third.path[4], "region=null");
        assert_eq!(
            literal_of(&Primitive::Int, &Transform::Bucket(16), &third.values[3].1),
            Ok(None)
        );
    }

    #[test]
    fn a_row_the_spec_cannot_read_is_refused() {
        let no_columns = OwnedRecord::default();
        let by_host = TablePartitioner::new(&spec(&[("host", Transform::Identity, 2)]), &table_schema()).unwrap();
        assert!(by_host.partition(&no_columns).is_err());
    }

    #[test]
    fn a_column_the_record_does_not_write_partitions_as_null() {
        let rows = rows();
        let by_ratio = TablePartitioner::new(&spec(&[("ratio", Transform::Identity, 9)]), &table_schema()).unwrap();
        assert_eq!(by_ratio.partition(&rows[0]).unwrap().path, ["ratio=null"]);
    }

    #[test]
    fn a_column_the_record_does_not_write_partitions_by_its_write_default() {
        let default = |mut f: NestedField, value: serde_json::Value| {
            f.other.insert("write-default".into(), value);
            f
        };
        let p = Type::Primitive;
        let mut schema = table_schema();
        schema.fields[5] = default(field(9, "ratio", p(Primitive::Double)), serde_json::json!(0.5));
        let Type::Struct(ctx) = &mut schema.fields[3].field_type else {
            panic!("ctx is a struct")
        };
        ctx.fields.push(default(
            field(10, "zone", p(Primitive::String)),
            serde_json::json!("z1"),
        ));
        let child = |id, name| default(field(id, name, p(Primitive::String)), serde_json::json!("nl"));
        schema.fields.extend([
            default(
                field(
                    11,
                    "geo",
                    Type::Struct(StructType {
                        fields: vec![child(12, "cc")],
                    }),
                ),
                serde_json::json!({}),
            ),
            field(
                13,
                "none",
                Type::Struct(StructType {
                    fields: vec![child(14, "cc")],
                }),
            ),
        ]);
        let partitioner = TablePartitioner::new(
            &spec(&[
                ("ratio", Transform::Identity, 9),
                ("zone", Transform::Identity, 10),
                ("cc", Transform::Identity, 12),
                ("none_cc", Transform::Identity, 14),
            ]),
            &schema,
        )
        .unwrap();
        let rows = rows();
        assert_eq!(
            partitioner.partition(&rows[0]).unwrap().path,
            ["ratio=0.5", "zone=z1", "cc=nl", "none_cc=null"]
        );
        // The third row's ctx is null, and so is the zone in it.
        assert_eq!(partitioner.partition(&rows[2]).unwrap().path[1], "zone=null");
    }

    #[test]
    fn a_spec_the_writer_cannot_follow_is_refused_at_new() {
        let schema = table_schema();
        for (fields, said) in [
            (
                [("x", Transform::Identity, 42)],
                "the partition field x names column id 42, which the table's schema lacks",
            ),
            ([("x", Transform::Identity, 8)], "column id 8 is inside the list items"),
            (
                [("x", Transform::Identity, 4)],
                "the partition field x is of ctx, a struct column, and a partition's source is a primitive",
            ),
            (
                [("x", Transform::Hour, 2)],
                "the partition field x: the transform hour cannot be applied to a string column",
            ),
            (
                [("x", Transform::Other("zorder".into()), 1)],
                "the partition field x: the transform zorder cannot be applied to a timestamptz column",
            ),
        ] {
            assert_eq!(
                TablePartitioner::new(&spec(&fields), &schema).err().as_deref(),
                Some(said)
            );
        }
        TablePartitioner::new(&spec(&[]), &schema).unwrap();
        TablePartitioner::new(&spec(&[("dropped_null", Transform::Void, 42)]), &schema)
            .expect("void reads no source, so a dropped one is no matter");
    }
}
