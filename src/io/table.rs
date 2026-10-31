//! What every table format reads the same way: a partition column's value in a row, and a written file's column
//! statistics from its Parquet footer. Each format names its own types and writes these values its own way.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType as Arrow, TimeUnit};
use parquet::basic::{LogicalType, TimeUnit as ParquetTimeUnit};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use std::cmp::Ordering;
use std::fmt;

use crate::core::column::DataType;
use crate::ops::civil::civil_from_days;
use crate::plan::topology::Records;

/// A row group's size where a table sets none: Iceberg's default, and Spark's for Delta.
pub const ROW_GROUP_BYTES: usize = 128 * 1024 * 1024;

/// A primitive column type, as both formats have it.
#[derive(Clone, Debug, PartialEq)]
pub enum Kind {
    Boolean,
    /// Delta's 8-bit integer.
    Byte,
    /// Delta's 16-bit integer.
    Short,
    Int,
    Long,
    Float,
    Double,
    Date,
    /// Microseconds, without a time zone.
    Timestamp,
    /// Microseconds since 1970-01-01T00:00:00Z.
    Timestamptz,
    String,
    Binary,
}

impl Kind {
    /// The Parquet type a column of this kind is written as, where it is narrower than any record type (`parquet::narrows`).
    pub fn narrowed(&self) -> Option<Arrow> {
        Some(match self {
            Kind::Byte => Arrow::Int8,
            Kind::Short => Arrow::Int16,
            Kind::Int => Arrow::Int32,
            Kind::Float => Arrow::Float32,
            Kind::Date => Arrow::Date32,
            Kind::Timestamp => Arrow::Timestamp(TimeUnit::Microsecond, None),
            _ => return None,
        })
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Kind::Boolean => "boolean",
            Kind::Byte => "byte",
            Kind::Short => "short",
            Kind::Int => "int",
            Kind::Long => "long",
            Kind::Float => "float",
            Kind::Double => "double",
            Kind::Date => "date",
            Kind::Timestamp => "timestamp",
            Kind::Timestamptz => "timestamptz",
            Kind::String => "string",
            Kind::Binary => "binary",
        })
    }
}

/// One value of a primitive type, as a partition holds it. The column's type says how it is written: an `Int` of a
/// byte column, a `Double` of a float column, `Micros` of a timestamp with or without zone.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Double(f64),
    /// Days since 1970-01-01.
    Date(i32),
    /// Microseconds since 1970-01-01T00:00:00Z. A time without zone is its wall clock in UTC.
    Micros(i64),
    String(String),
    Binary(Vec<u8>),
}

/// The one record every route writing here writes. A table has one schema.
pub fn one_record(records: Option<&Records>) -> Result<&DataType, String> {
    let mut found: Option<&DataType> = None;
    for record in records.into_iter().flatten().map(|(_, record)| record) {
        match found {
            Some(first) if first != record => {
                return Err("the routes writing here write different records, and a table has one schema".into());
            }
            _ => found = Some(record),
        }
    }
    found.ok_or_else(|| "no route writes here, so there is no record to write".into())
}

/// The column at `names` in `batch`, or None when the record does not write it, or a struct on the way to it is null
/// on row `at`: a column the table keeps and the record does not write is null in its files.
pub fn column_at(batch: &arrow_array::RecordBatch, names: &[String], at: usize) -> Result<Option<ArrayRef>, String> {
    let Some(mut column) = batch.column_by_name(&names[0]).cloned() else {
        return Ok(None);
    };
    for name in &names[1..] {
        if column.is_null(at) {
            return Ok(None);
        }
        let Some(parent) = column.as_struct_opt() else {
            return Err(format!(
                "the row's {} is no struct, and the table's partition reads {} in it",
                names[..names.len() - 1].join("."),
                names.join(".")
            ));
        };
        match parent.column_by_name(name) {
            Some(child) => column = child.clone(),
            None => return Ok(None),
        }
    }
    Ok(Some(column))
}

/// Row `at` of `column` as a value of the table's `kind`; None when null.
pub fn read(column: &ArrayRef, at: usize, kind: &Kind, names: &[String]) -> Result<Option<Literal>, String> {
    // A field that never holds a value is a null column, which keeps no null buffer.
    if column.data_type() == &Arrow::Null || column.is_null(at) {
        return Ok(None);
    }
    let value = match (kind, column.data_type()) {
        (Kind::Boolean, Arrow::Boolean) => Literal::Boolean(column.as_boolean().value(at)),
        (Kind::Long, Arrow::Int64) => Literal::Long(column.as_primitive::<Int64Type>().value(at)),
        (Kind::Int, Arrow::Int32) => Literal::Int(column.as_primitive::<Int32Type>().value(at)),
        // A row's column is the record's, wider than a table may hold it (`parquet::narrows`).
        (Kind::Byte | Kind::Short | Kind::Int, Arrow::Int64) | (Kind::Float, Arrow::Float64) => {
            let to = kind.narrowed().expect("a narrower kind");
            if let Some(why) = crate::io::parquet::misfit(column, at, &to) {
                return Err(format!("{} {why}", names.join(".")));
            }
            match column.data_type() {
                Arrow::Int64 => Literal::Int(column.as_primitive::<Int64Type>().value(at) as i32),
                // Rounded: what a float column is.
                _ => Literal::Double(column.as_primitive::<Float64Type>().value(at) as f32 as f64),
            }
        }
        (Kind::Date, Arrow::Timestamp(TimeUnit::Microsecond, Some(_))) => {
            let us = column.as_primitive::<TimestampMicrosecondType>().value(at);
            // Every day an i64 of microseconds reaches is an i32.
            Literal::Date(us.div_euclid(crate::ops::civil::MICROS_PER_DAY) as i32)
        }
        (Kind::Float, Arrow::Float32) => Literal::Double(column.as_primitive::<Float32Type>().value(at) as f64),
        (Kind::Double, Arrow::Float64) => Literal::Double(column.as_primitive::<Float64Type>().value(at)),
        (Kind::Date, Arrow::Date32) => Literal::Date(column.as_primitive::<Date32Type>().value(at)),
        (Kind::Timestamp | Kind::Timestamptz, Arrow::Timestamp(TimeUnit::Microsecond, _)) => {
            Literal::Micros(column.as_primitive::<TimestampMicrosecondType>().value(at))
        }
        (Kind::String, Arrow::Utf8) => Literal::String(column.as_string::<i32>().value(at).to_string()),
        (Kind::Binary, Arrow::Binary) => Literal::Binary(column.as_binary::<i32>().value(at).to_vec()),
        (_, arrow) => {
            return Err(format!(
                "the column {} is {arrow} in the row, and the table's partition reads it as {kind}",
                names.join(".")
            ));
        }
    };
    Ok(Some(value))
}

/// Escapes `s` as Java's `URLEncoder` does: letters, digits and `.-*_` as they are, a space as `+`, every other byte
/// of its UTF-8 as `%XX`.
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'*' | b'_' => out.push(b as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A date as Java's LocalDate writes it: four digits of year, more with a `+` past 9999.
pub fn iso_date(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    let year = match y {
        10_000.. => format!("+{y}"),
        0..10_000 => format!("{y:04}"),
        _ => format!("-{:04}", -y),
    };
    format!("{year}-{m:02}-{d:02}")
}

/// A row group's min or max as the footer holds it.
#[derive(Clone, Debug, PartialEq)]
pub enum Bound {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
}

impl Bound {
    /// The order of bounds of one variant, None across two. A float's is IEEE 754 totalOrder, where -0.0 comes
    /// before 0.0, as Iceberg orders its bounds; `PartialOrd` would take them as equal.
    fn order(&self, other: &Bound) -> Option<Ordering> {
        use Bound as B;
        match (self, other) {
            (B::Boolean(a), B::Boolean(b)) => Some(a.cmp(b)),
            (B::Int(a), B::Int(b)) => Some(a.cmp(b)),
            (B::Long(a), B::Long(b)) => Some(a.cmp(b)),
            (B::Float(a), B::Float(b)) => Some(a.total_cmp(b)),
            (B::Double(a), B::Double(b)) => Some(a.total_cmp(b)),
            (B::Bytes(a), B::Bytes(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// One leaf column's statistics across a file's row groups.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStats {
    /// Compressed bytes.
    pub size: i64,
    /// Values, nulls included.
    pub values: i64,
    /// `None` when a row group does not count them.
    pub nulls: Option<i64>,
    /// The least and the greatest value. `None` when the column takes no bounds or a row group with values lacks them.
    pub bounds: Option<(Bound, Bound)>,
}

/// The statistics of leaf `column` of the file `metadata` describes. `kind` is its type when the format keeps bounds
/// for it; a leaf inside a list or a map, and a timestamp not in microseconds, has none.
pub fn column_stats(metadata: &ParquetMetaData, column: usize, kind: Option<&Kind>) -> ColumnStats {
    let descr = metadata.file_metadata().schema_descr().column(column);
    let micros = match descr.logical_type_ref() {
        Some(LogicalType::Timestamp(t)) => matches!(t.unit, ParquetTimeUnit::MICROS),
        _ => true,
    };
    let bound_kind = kind.filter(|_| descr.max_rep_level() == 0 && micros);

    let (mut size, mut values, mut nulls) = (0i64, 0i64, Some(0i64));
    // `None` once a row group with values has no min or max: then the file has no bounds for the column.
    let mut bounds: Option<Option<(Bound, Bound)>> = bound_kind.map(|_| None);
    for row_group in metadata.row_groups() {
        let chunk = row_group.column(column);
        size += chunk.compressed_size();
        values += chunk.num_values();
        let statistics = chunk.statistics();
        let null_count = statistics.and_then(|s| s.null_count_opt());
        nulls = nulls.zip(null_count).map(|(n, more)| n + more as i64);
        let (Some(kind), Some(so_far)) = (bound_kind, bounds.take()) else {
            continue;
        };
        bounds = match (statistics.and_then(|s| min_max(s, kind)), so_far) {
            (Some(here), None) => Some(Some(here)),
            (Some((lo, hi)), Some((low, high))) => Some(Some((
                if lo.order(&low) == Some(Ordering::Less) {
                    lo
                } else {
                    low
                },
                if hi.order(&high) == Some(Ordering::Greater) {
                    hi
                } else {
                    high
                },
            ))),
            (None, so_far) if null_count == Some(chunk.num_values() as u64) => Some(so_far),
            (None, _) => None,
        };
    }
    ColumnStats {
        size,
        values,
        nulls,
        bounds: bounds.flatten(),
    }
}

fn min_max(statistics: &Statistics, kind: &Kind) -> Option<(Bound, Bound)> {
    use Kind as K;
    match (kind, statistics) {
        (K::Boolean, Statistics::Boolean(s)) => Some((Bound::Boolean(*s.min_opt()?), Bound::Boolean(*s.max_opt()?))),
        (K::Byte | K::Short | K::Int | K::Date, Statistics::Int32(s)) => {
            Some((Bound::Int(*s.min_opt()?), Bound::Int(*s.max_opt()?)))
        }
        (K::Long | K::Timestamp | K::Timestamptz, Statistics::Int64(s)) => {
            Some((Bound::Long(*s.min_opt()?), Bound::Long(*s.max_opt()?)))
        }
        (K::Float, Statistics::Float(s)) => {
            let (min, max) = (*s.min_opt()?, *s.max_opt()?);
            (!min.is_nan() && !max.is_nan()).then_some((Bound::Float(min), Bound::Float(max)))
        }
        (K::Double, Statistics::Double(s)) => {
            let (min, max) = (*s.min_opt()?, *s.max_opt()?);
            (!min.is_nan() && !max.is_nan()).then_some((Bound::Double(min), Bound::Double(max)))
        }
        // A deprecated min/max was ordered as signed bytes, which is not the order of a string or a binary.
        (K::String | K::Binary, Statistics::ByteArray(s)) if !statistics.is_min_max_deprecated() => Some((
            Bound::Bytes(s.min_opt()?.data().to_vec()),
            Bound::Bytes(s.max_opt()?.data().to_vec()),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;

    #[test]
    fn bounds_across_row_groups_put_negative_zero_below_zero() {
        let bounds = |values: Vec<f64>| {
            let batch = RecordBatch::try_from_iter([("v", Arc::new(Float64Array::from(values)) as ArrayRef)]).unwrap();
            let properties = WriterProperties::builder().set_max_row_group_row_count(Some(1)).build();
            let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap();
            writer.write(&batch).unwrap();
            let metadata = writer.close().unwrap();
            assert_eq!(metadata.num_row_groups(), 3);
            match column_stats(&metadata, 0, Some(&Kind::Double)).bounds {
                Some((Bound::Double(lo), Bound::Double(hi))) => (lo.to_bits(), hi.to_bits()),
                other => panic!("bounds {other:?}"),
            }
        };
        let zeros = ((-0.0f64).to_bits(), 0.0f64.to_bits());
        assert_eq!(bounds(vec![0.0, -0.0, 0.0]), zeros);
        assert_eq!(bounds(vec![-0.0, 0.0, -0.0]), zeros);
    }

    #[test]
    fn a_partition_value_is_read_as_the_table_holds_it_or_refused_where_it_cannot() {
        let names = ["v".to_string()];
        let longs: ArrayRef = Arc::new(Int64Array::from(vec![127, 128, -32_768, 40_000]));
        let at = |kind: Kind, row| read(&longs, row, &kind, &names);
        assert_eq!(at(Kind::Byte, 0), Ok(Some(Literal::Int(127))));
        assert_eq!(
            at(Kind::Byte, 1),
            Err("v is 128, which an 8-bit integer column cannot hold".into())
        );
        assert_eq!(at(Kind::Short, 2), Ok(Some(Literal::Int(-32_768))));
        assert_eq!(
            at(Kind::Short, 3),
            Err("v is 40000, which a 16-bit integer column cannot hold".into())
        );
        assert_eq!(at(Kind::Int, 3), Ok(Some(Literal::Int(40_000))));

        let doubles: ArrayRef = Arc::new(Float64Array::from(vec![0.1, 1e300, f64::NAN]));
        let at = |row| read(&doubles, row, &Kind::Float, &names);
        assert_eq!(at(0), Ok(Some(Literal::Double(0.1f32 as f64))));
        assert_eq!(at(1), Err("v is 1e300, beyond a 32-bit float column's range".into()));
        assert!(matches!(at(2), Ok(Some(Literal::Double(v))) if v.is_nan()));

        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![-1]).with_timezone("UTC"));
        assert_eq!(read(&times, 0, &Kind::Timestamp, &names), Ok(Some(Literal::Micros(-1))));
    }
}
