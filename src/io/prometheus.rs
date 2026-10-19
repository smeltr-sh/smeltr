//! What a Prometheus receiver reads: remote_write requests, as Prometheus and the agents that speak its protocol send
//! them.
//!
//! A request is a snappy-compressed protobuf `WriteRequest` (remote_write 1.0): time series, each its labels and its
//! samples, a float or a native histogram each. Each sample is one record: its series' `__name__` is its `name`, every
//! other label is in its `attributes`, and its time rides beside them. A float sample's number is its `value`; a
//! histogram is typed as an OTLP histogram point is, an exponential one's buckets in `positive` and `negative` and a
//! custom one's in `bucketCounts` and `explicitBounds`, so a pipeline reads a histogram from either receiver alike.
//! Its reset hint, which OTLP has none of, is its `resetHint`. A series' exemplars ride in `exemplars` on its last
//! record of the request, each shaped as an OTLP point's exemplar is: its `time`, `value`, the labels `trace_id` and
//! `span_id` as `traceId` and `spanId`, and its other labels in `filteredAttributes`. A series of exemplars alone, as
//! Prometheus sends them, is one record of them, its `value` null. Each metric family's metadata is a record of its
//! own, with no time, value or attributes: the family is its `name`, its help its `description`, its unit its `unit`,
//! and its type is typed as an OTLP metric is, a counter a monotonic cumulative `sum`. A body is decompressed and
//! checked whole before any record is taken, so a body that does not read is refused whole; its series and metadata
//! are decoded one at a time as the reader takes them, so what a request holds is its decompressed body and one of
//! them.
//!
//! A remote_write 2.0 request (`io.prometheus.write.v2.Request`) reads to the same records. Its labels are indexes
//! into the request's table of strings, each series carries its family's metadata, and a sample or histogram may carry
//! the time its series started counting, its `startTime`. A family's metadata is one record per request, its family
//! the series' name, less `_bucket`, `_sum` and `_count` for a classic histogram's or summary's series. A label index
//! past the table, a table whose first string is not empty, or a series holding samples and histograms both refuses
//! the request, as the protocol has it.

use std::collections::HashSet;
use std::ops::Range;

use prost::Message;

use super::otlp::{Cell, Row, variant_of};
use crate::core::variant;

/// Where remote_write senders POST, as Prometheus serves it.
pub const WRITE_PATH: &str = "/api/v1/write";

/// The label holding a series' metric name.
pub(super) const NAME_LABEL: &str = "__name__";

/// The one `Content-Type` remote_write sends, with a `proto` naming its message, which 1.0 may leave out.
const CONTENT_TYPE: &str = "application/x-protobuf";

/// The `proto` remote_write 1.0 names, where a sender names one.
const V1_PROTO: &str = "prometheus.WriteRequest";

/// The `proto` remote_write 2.0 names.
const V2_PROTO: &str = "io.prometheus.write.v2.Request";

/// The version of remote_write a request is sent in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    V1,
    V2,
}

/// The version of remote_write a request of `content_type` is sent in, or why it is none this receiver reads. A sender
/// of 1.0 that predates content negotiation names no `proto`.
pub fn readable(content_type: Option<&str>) -> Result<Version, String> {
    let refused = || {
        format!(
            "a remote_write request is sent as {CONTENT_TYPE} with proto={V1_PROTO} or proto={V2_PROTO}, not as {}",
            content_type.unwrap_or("nothing")
        )
    };
    let mut parts = content_type.ok_or_else(refused)?.split(';').map(str::trim);
    if !parts.next().is_some_and(|t| t.eq_ignore_ascii_case(CONTENT_TYPE)) {
        return Err(refused());
    }
    let mut version = Version::V1;
    for part in parts {
        if let Some((key, value)) = part.split_once('=')
            && key.trim().eq_ignore_ascii_case("proto")
        {
            version = match value.trim().trim_matches('"') {
                V1_PROTO => Version::V1,
                V2_PROTO => Version::V2,
                _ => return Err(refused()),
            };
        }
    }
    Ok(version)
}

/// Whether `content_encoding` is snappy, the one compression remote_write sends.
pub fn is_snappy(content_encoding: Option<&str>) -> bool {
    content_encoding.is_some_and(|e| e.trim().eq_ignore_ascii_case("snappy"))
}

/// What `body`, snappy block format, holds decompressed, from its header, or why it is not snappy.
pub fn decompressed_len(body: &[u8]) -> Result<usize, String> {
    snap::raw::decompress_len(body).map_err(|e| format!("not snappy: {e}"))
}

// The messages of prometheus/prompb that remote_write 1.0 sends, by their field numbers there. Fields this module does
// not read are left out: protobuf skips them. 2.0's samples and histograms are 1.0's with a start time added, so the
// same messages read both.

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub(super) labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub(super) samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub(super) exemplars: Vec<Exemplar>,
    #[prost(message, repeated, tag = "4")]
    pub(super) histograms: Vec<Histogram>,
}

/// One observation the series counted, with labels naming where it came from, a trace's most often.
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Exemplar {
    #[prost(message, repeated, tag = "1")]
    pub(super) labels: Vec<Label>,
    #[prost(double, tag = "2")]
    pub(super) value: f64,
    /// Milliseconds since the epoch.
    #[prost(int64, tag = "3")]
    pub(super) timestamp: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Label {
    #[prost(string, tag = "1")]
    pub(super) name: String,
    #[prost(string, tag = "2")]
    pub(super) value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Sample {
    #[prost(double, tag = "1")]
    pub(super) value: f64,
    /// Milliseconds since the epoch.
    #[prost(int64, tag = "2")]
    pub(super) timestamp: i64,
    /// When its series started counting, in milliseconds since the epoch; 0 for unknown. 2.0's only.
    #[prost(int64, tag = "3")]
    pub(super) start_timestamp: i64,
}

/// A native histogram: integer counts as deltas from the bucket before, or float counts as they are. Bucket `i` of
/// an exponential schema `s` holds values in `(2^((i-1)/2^s), 2^(i/2^s)]`; a schema of
/// [`CUSTOM_BUCKETS_SCHEMA`] has the bounds in `custom_values` instead, bucket `i` ending at its `i`th.
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Histogram {
    #[prost(oneof = "HistogramCount", tags = "1, 2")]
    pub(super) count: Option<HistogramCount>,
    #[prost(double, tag = "3")]
    pub(super) sum: f64,
    #[prost(sint32, tag = "4")]
    pub(super) schema: i32,
    #[prost(double, tag = "5")]
    pub(super) zero_threshold: f64,
    #[prost(oneof = "HistogramZeroCount", tags = "6, 7")]
    pub(super) zero_count: Option<HistogramZeroCount>,
    #[prost(message, repeated, tag = "8")]
    pub(super) negative_spans: Vec<BucketSpan>,
    #[prost(sint64, repeated, tag = "9")]
    pub(super) negative_deltas: Vec<i64>,
    #[prost(double, repeated, tag = "10")]
    pub(super) negative_counts: Vec<f64>,
    #[prost(message, repeated, tag = "11")]
    pub(super) positive_spans: Vec<BucketSpan>,
    #[prost(sint64, repeated, tag = "12")]
    pub(super) positive_deltas: Vec<i64>,
    #[prost(double, repeated, tag = "13")]
    pub(super) positive_counts: Vec<f64>,
    #[prost(enumeration = "ResetHint", tag = "14")]
    pub(super) reset_hint: i32,
    /// Milliseconds since the epoch.
    #[prost(int64, tag = "15")]
    pub(super) timestamp: i64,
    #[prost(double, repeated, tag = "16")]
    pub(super) custom_values: Vec<f64>,
    /// When its series started counting, in milliseconds since the epoch; 0 for unknown. 2.0's only.
    #[prost(int64, tag = "17")]
    pub(super) start_timestamp: i64,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub(super) enum HistogramCount {
    #[prost(uint64, tag = "1")]
    Int(u64),
    #[prost(double, tag = "2")]
    Float(f64),
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub(super) enum HistogramZeroCount {
    #[prost(uint64, tag = "6")]
    Int(u64),
    #[prost(double, tag = "7")]
    Float(f64),
}

/// A run of `length` buckets, starting `offset` buckets past where the run before it ended, or at bucket `offset`
/// for the first.
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct BucketSpan {
    #[prost(sint32, tag = "1")]
    pub(super) offset: i32,
    #[prost(uint32, tag = "2")]
    pub(super) length: u32,
}

/// What a histogram says of its counter's resets since the one before; `Gauge` is a gauge histogram's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
pub(super) enum ResetHint {
    Unknown = 0,
    Yes = 1,
    No = 2,
    Gauge = 3,
}

/// The schema of a histogram whose buckets end at `custom_values`.
pub(super) const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

/// What a sender says of a metric family: its type, its help and its unit.
#[derive(Clone, PartialEq, prost::Message)]
struct MetricMetadata {
    #[prost(enumeration = "MetricType", tag = "1")]
    r#type: i32,
    #[prost(string, tag = "2")]
    metric_family_name: String,
    #[prost(string, tag = "4")]
    help: String,
    #[prost(string, tag = "5")]
    unit: String,
}

/// A metric family's type, as OpenMetrics has them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum MetricType {
    Unknown = 0,
    Counter = 1,
    Gauge = 2,
    Histogram = 3,
    GaugeHistogram = 4,
    Summary = 5,
    Info = 6,
    Stateset = 7,
}

/// `WriteRequest.timeseries`.
const TIMESERIES_FIELD: u64 = 1;

/// `WriteRequest.metadata`.
const METADATA_FIELD: u64 = 3;

// The messages of remote_write 2.0 that differ from 1.0's, by their field numbers in prompb's `write/v2`. A label, an
// exemplar's label, a help and a unit are each an index into the request's `symbols`.

#[derive(Clone, PartialEq, prost::Message)]
struct TimeSeriesV2 {
    /// Each label's name and value, in pairs.
    #[prost(uint32, repeated, tag = "1")]
    labels_refs: Vec<u32>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    histograms: Vec<Histogram>,
    #[prost(message, repeated, tag = "4")]
    exemplars: Vec<ExemplarV2>,
    #[prost(message, optional, tag = "5")]
    metadata: Option<MetadataV2>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ExemplarV2 {
    #[prost(uint32, repeated, tag = "1")]
    labels_refs: Vec<u32>,
    #[prost(double, tag = "2")]
    value: f64,
    /// Milliseconds since the epoch.
    #[prost(int64, tag = "3")]
    timestamp: i64,
}

/// A series' family's metadata. Its type numbers are 1.0's.
#[derive(Clone, PartialEq, prost::Message)]
struct MetadataV2 {
    #[prost(enumeration = "MetricType", tag = "1")]
    r#type: i32,
    #[prost(uint32, tag = "3")]
    help_ref: u32,
    #[prost(uint32, tag = "4")]
    unit_ref: u32,
}

/// `Request.symbols`.
const SYMBOLS_FIELD: u64 = 4;

/// `Request.timeseries`.
const TIMESERIES_V2_FIELD: u64 = 5;

/// Where each field of a Prometheus receiver's record sits in a [`Row`], in `SourceMeta::prometheus_receiver`'s order.
mod field {
    pub const TIME: usize = 0;
    pub const NAME: usize = 1;
    pub const VALUE: usize = 2;
    pub const ATTRIBUTES: usize = 3;
    pub const TYPE: usize = 4;
    pub const AGGREGATION_TEMPORALITY: usize = 5;
    pub const COUNT: usize = 6;
    pub const SUM: usize = 7;
    pub const BUCKET_COUNTS: usize = 8;
    pub const EXPLICIT_BOUNDS: usize = 9;
    pub const SCALE: usize = 10;
    pub const ZERO_COUNT: usize = 11;
    pub const ZERO_THRESHOLD: usize = 12;
    pub const POSITIVE: usize = 13;
    pub const NEGATIVE: usize = 14;
    pub const RESET_HINT: usize = 15;
    pub const EXEMPLARS: usize = 16;
    pub const DESCRIPTION: usize = 17;
    pub const UNIT: usize = 18;
    pub const IS_MONOTONIC: usize = 19;
    pub const START_TIME: usize = 20;
    /// How many fields a row holds.
    pub const ALL: usize = 21;
}

/// OTLP's `AGGREGATION_TEMPORALITY_CUMULATIVE`: a counter histogram counts from its series' start.
const CUMULATIVE: i64 = 2;

/// The schemas of an exponential histogram, as Prometheus has them.
const EXPONENTIAL_SCHEMAS: std::ops::RangeInclusive<i32> = -4..=8;

/// How far apart a float histogram's count and what its buckets hold may be, as a share of the larger: float counts
/// add up with rounding.
const FLOAT_COUNT_TOLERANCE: f64 = 1e-9;

/// What one number costs in a row's array or struct, as `core::variant` writes it: a tag and eight bytes.
const VARIANT_NUMBER: usize = 9;

/// The exemplar labels that name its trace and span, which an OTLP exemplar carries as `traceId` and `spanId`.
const TRACE_ID_LABEL: &str = "trace_id";
const SPAN_ID_LABEL: &str = "span_id";

/// What an exemplar costs as a variant beyond its labels, at most: its object, `time` and `value` as keys and
/// numbers, and `filteredAttributes` as a key and an object.
const VARIANT_EXEMPLAR: usize = 9 + (4 + 4 + VARIANT_NUMBER) + (4 + 5 + VARIANT_NUMBER) + (4 + 18 + 9);

/// What a label costs as a variant beyond its name and value, at most: a key's length, and a string's tag and length.
const VARIANT_LABEL: usize = 4 + 1 + 4;

/// A request's samples, float and histogram, its exemplars and its metadata, cut one record at a time from its
/// decompressed body.
pub struct Samples {
    body: Vec<u8>,
    version: Version,
    /// A 2.0 request's table of strings, as where each lies in its body. Empty for 1.0, and for a table too large to
    /// hold.
    symbols: Vec<Range<usize>>,
    /// How many strings a 2.0 request's table holds.
    symbol_count: usize,
    /// What its widest series or metadata entry holds decoded, which the one being read holds beside the body.
    widest: usize,
    /// Where the next top-level field of the `WriteRequest` starts.
    at: usize,
    /// The series being read: its name, its other labels, and the samples and exemplars it has left.
    series: Option<Series>,
    /// The families a 2.0 request's metadata has given a record so far, by their names' hashes.
    families: HashSet<u64>,
    /// How many series a 2.0 request holds, which bounds `families`.
    series_count: usize,
}

/// What a request holds that its records keep: its samples, its histograms, and its exemplars a record holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Written {
    pub samples: usize,
    pub histograms: usize,
    pub exemplars: usize,
}

struct Series {
    name: Option<String>,
    attributes: Option<Vec<u8>>,
    samples: std::vec::IntoIter<Sample>,
    histograms: std::vec::IntoIter<Histogram>,
    /// Its exemplars as `exemplars` holds them, until its last record takes them. `None` for none.
    exemplars: Option<Vec<u8>>,
    /// Its latest exemplar's time, in milliseconds: its one record's time when it has no samples.
    latest: i64,
}

impl Samples {
    /// A snappy-compressed `WriteRequest` of `version`, decompressed, with what each series and metadata entry holds
    /// decoded read from its bytes, or why it does not read. Its series are not decoded here, but each histogram is,
    /// as it is small and what it holds as a record is read from its spans; [`Samples::check`] decodes the series and
    /// metadata, once what that holds is held. A histogram whose buckets do not add up refuses the request, as taking
    /// the rest would drop it unsaid.
    ///
    /// A 2.0 request's table is counted before it is made, and one that would take the request past `limit` bytes is
    /// not made nor its series read: [`Samples::size`] says it passes `limit`, and [`Samples::check`] refuses it. A 2.0
    /// request whose series decode to more than `limit` between them is refused, as each copies the strings it names.
    pub fn snappy(body: &[u8], version: Version, limit: usize) -> Result<Samples, String> {
        let body = snap::raw::Decoder::new()
            .decompress_vec(body)
            .map_err(|e| format!("not snappy: {e}"))?;
        let not = |e: String| format!("not a remote_write request: {e}");
        let symbol_count = match version {
            Version::V1 => 0,
            Version::V2 => symbol_count(&body).map_err(not)?,
        };
        let mut samples = Samples {
            body,
            version,
            symbols: Vec::new(),
            symbol_count,
            widest: 0,
            at: 0,
            series: None,
            families: HashSet::new(),
            series_count: 0,
        };
        if samples.size() > limit {
            return Ok(samples);
        }
        let body = &samples.body;
        let symbols = match version {
            Version::V1 => Vec::new(),
            Version::V2 => symbols(body, symbol_count),
        };
        let (mut at, mut widest, mut series_count, mut decoded) = (0, 0, 0, 0usize);
        while let Some((entry, next)) = next_entry(body, at, version).map_err(not)? {
            let size = match entry {
                Entry::Series(series) => decoded_size(series)?,
                Entry::Metadata(metadata) => metadata_size(metadata),
                Entry::SeriesV2(series) => {
                    series_count += 1;
                    let size = decoded_size_v2(series, &symbols)?;
                    // Many series naming one long string decode to far more than the request holds, one at a time.
                    decoded = decoded.saturating_add(size);
                    if decoded > limit {
                        return Err(format!(
                            "a remote_write 2.0 request's series decode to more than {limit} bytes between them, \
                             what a receiver reads of one request"
                        ));
                    }
                    size
                }
            };
            widest = widest.max(size);
            at = next;
        }
        if version == Version::V2 && symbol_count == 0 && series_count > 0 {
            return Err(not("a remote_write 2.0 request holds series and no symbols".into()));
        }
        (samples.symbols, samples.widest, samples.series_count) = (symbols, widest, series_count);
        Ok(samples)
    }

    /// Why a series or metadata entry of this request is not one as its version of remote_write has it, or what its
    /// records keep. Decodes each in turn, so it holds what [`Samples::size`] says.
    pub fn check(&self) -> Result<Written, String> {
        if self.symbols.len() != self.symbol_count {
            return Err(format!(
                "a request's table of {} strings is more than a receiver holds",
                self.symbol_count
            ));
        }
        let not = |e: prost::DecodeError| format!("not a remote_write request: {e}");
        let mut written = Written::default();
        let mut at = 0;
        while let Some((entry, next)) =
            next_entry(&self.body, at, self.version).expect("a request is scanned when it is made")
        {
            let series = match entry {
                Entry::Series(series) => TimeSeries::decode(series).map_err(not)?,
                Entry::Metadata(metadata) => {
                    MetricMetadata::decode(metadata).map_err(not)?;
                    at = next;
                    continue;
                }
                Entry::SeriesV2(series) => {
                    let series = TimeSeriesV2::decode(series).map_err(not)?;
                    if !series.samples.is_empty() && !series.histograms.is_empty() {
                        return Err(format!(
                            "a remote_write 2.0 series holds {} samples and {} histograms, and a series holds one or \
                             the other",
                            series.samples.len(),
                            series.histograms.len()
                        ));
                    }
                    self.resolve(series)?.0
                }
            };
            labels_valid(&series.labels)?;
            written.samples += series.samples.len();
            written.histograms += series.histograms.len();
            written.exemplars += series.exemplars.iter().filter(|e| exemplar_kept(&e.labels)).count();
            at = next;
        }
        Ok(written)
    }

    /// What this request holds at most: its decompressed body, its table of strings, the families its metadata has
    /// given a record, and its widest series or metadata entry decoded beside them.
    pub fn size(&self) -> usize {
        self.body.len()
            + room(self.symbol_count) * size_of::<Range<usize>>()
            + room(self.series_count) * 2 * size_of::<u64>()
            + self.widest
    }

    /// The next record, in the order the request holds them: each series' float samples, then its histograms, its
    /// exemplars on the last of them, or one record of its exemplars where it has no samples; and one record of each
    /// metadata entry that names its family, a 2.0 series' ahead of its samples where it is the first of its family.
    /// `None` when every record is taken.
    pub fn next_record(&mut self) -> Option<Row> {
        let checked = "a request is checked whole when it is made";
        loop {
            if let Some(row) = self.series.as_mut().and_then(Series::next) {
                return Some(row);
            }
            let (entry, next) = next_entry(&self.body, self.at, self.version).expect(checked)?;
            self.at = next;
            match entry {
                Entry::Series(series) => {
                    self.series = Some(Series::of(TimeSeries::decode(series).expect(checked)));
                }
                Entry::Metadata(metadata) => {
                    self.series = None;
                    if let Some(row) = metadata_row(MetricMetadata::decode(metadata).expect(checked)) {
                        return Some(row);
                    }
                }
                Entry::SeriesV2(series) => {
                    let (series, metadata) = self
                        .resolve(TimeSeriesV2::decode(series).expect(checked))
                        .expect(checked);
                    self.series = Some(Series::of(series));
                    if let Some(metadata) = metadata
                        && self.families.insert(hash(&metadata.metric_family_name))
                        && let Some(row) = metadata_row(metadata)
                    {
                        return Some(row);
                    }
                }
            }
        }
    }

    /// `s`, a string of the table.
    fn symbol(&self, s: u32) -> Result<&str, String> {
        let range = (self.symbols.get(s as usize)).ok_or_else(|| {
            format!(
                "a remote_write 2.0 series names string {s}, past the request's {} strings",
                self.symbols.len()
            )
        })?;
        Ok(std::str::from_utf8(&self.body[range.clone()]).expect("each string is checked when the request is scanned"))
    }

    fn labels(&self, refs: &[u32]) -> Result<Vec<Label>, String> {
        if !refs.len().is_multiple_of(2) {
            return Err(format!(
                "a remote_write 2.0 series names {} strings for its labels, and a label is two",
                refs.len()
            ));
        }
        (refs.as_chunks::<2>().0.iter())
            .map(|&[name, value]| {
                Ok(Label {
                    name: self.symbol(name)?.to_string(),
                    value: self.symbol(value)?.to_string(),
                })
            })
            .collect()
    }

    /// A 2.0 series as 1.0 has it, its strings taken from the table, and its family's metadata where it says any: the
    /// series' name, less a classic histogram's or summary's suffix. Why it does not read, for a string past the table.
    fn resolve(&self, series: TimeSeriesV2) -> Result<(TimeSeries, Option<MetricMetadata>), String> {
        let labels = self.labels(&series.labels_refs)?;
        let exemplars = (series.exemplars.into_iter())
            .map(|e| {
                Ok(Exemplar {
                    labels: self.labels(&e.labels_refs)?,
                    value: e.value,
                    timestamp: e.timestamp,
                })
            })
            .collect::<Result<_, String>>()?;
        let metadata = match series.metadata {
            Some(m) => {
                let (help, unit) = (self.symbol(m.help_ref)?, self.symbol(m.unit_ref)?);
                let name = labels.iter().find(|l| l.name == NAME_LABEL).map(|l| l.value.as_str());
                let said = m.r#type != MetricType::Unknown as i32 || !help.is_empty() || !unit.is_empty();
                name.filter(|_| said).map(|name| MetricMetadata {
                    r#type: m.r#type,
                    metric_family_name: family(name, m.r#type, series.histograms.is_empty()).to_string(),
                    help: help.to_string(),
                    unit: unit.to_string(),
                })
            }
            None => None,
        };
        let series = TimeSeries {
            labels,
            samples: series.samples,
            exemplars,
            histograms: series.histograms,
        };
        Ok((series, metadata))
    }
}

/// The family of a series named `name` of type `kind`: a classic histogram's or summary's series, which `classic` says
/// it is, less the suffix its type gives it, and any other its name as it is. A gauge histogram's sum and count are
/// `_gsum` and `_gcount` from an OpenMetrics scrape, and `_sum` and `_count` from a sender that writes them as a
/// histogram's.
fn family(name: &str, kind: i32, classic: bool) -> &str {
    let suffixes: &[&str] = match MetricType::try_from(kind) {
        _ if !classic => &[],
        Ok(MetricType::Histogram) => &["_bucket", "_sum", "_count"],
        Ok(MetricType::GaugeHistogram) => &["_bucket", "_gsum", "_gcount", "_sum", "_count"],
        Ok(MetricType::Summary) => &["_sum", "_count"],
        _ => &[],
    };
    (suffixes.iter())
        .find_map(|suffix| name.strip_suffix(suffix).filter(|family| !family.is_empty()))
        .unwrap_or(name)
}

/// `family`'s hash. Two families whose hashes collide in one request give one metadata record, which a 64-bit hash
/// makes as unlikely as it is harmless.
fn hash(family: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    family.hash(&mut hasher);
    hasher.finish()
}

/// How many strings a 2.0 request's table holds, or why the table does not read: a string that is not UTF-8, or a
/// first string that is not empty. Allocates nothing, so a table is accounted before it is made.
fn symbol_count(body: &[u8]) -> Result<usize, String> {
    let (mut count, mut at) = (0, 0);
    while let Some(field) = next_field(body, at)? {
        if field.number == SYMBOLS_FIELD {
            if field.wire != 2 {
                return Err(format!("a symbol as wire type {}, where it is a string", field.wire));
            }
            std::str::from_utf8(field.value).map_err(|e| format!("symbol {count} is not UTF-8: {e}"))?;
            if count == 0 && !field.value.is_empty() {
                return Err("its first symbol is not the empty string".into());
            }
            count += 1;
        }
        at = field.next;
    }
    Ok(count)
}

/// Where each of the `count` strings of a 2.0 request's table lies in `body`. [`symbol_count`] has read the table.
fn symbols(body: &[u8], count: usize) -> Vec<Range<usize>> {
    let mut symbols = Vec::with_capacity(count);
    let mut at = 0;
    while let Some(field) = next_field(body, at).expect("the table is read when it is counted") {
        if field.number == SYMBOLS_FIELD {
            symbols.push(field.next - field.value.len()..field.next);
        }
        at = field.next;
    }
    symbols
}

impl Series {
    fn of(series: TimeSeries) -> Series {
        let mut name = None;
        let mut out = Vec::new();
        let open = variant::begin_object(&mut out);
        let mut others = 0;
        for label in series.labels {
            if label.name == NAME_LABEL {
                name = Some(label.value);
                continue;
            }
            variant::put_key(&mut out, &label.name);
            variant::put_str(&mut out, &label.value);
            others += 1;
        }
        variant::end(&mut out, open, others);
        let stored: Vec<Exemplar> = (series.exemplars.into_iter())
            .filter(|e| exemplar_kept(&e.labels))
            .collect();
        Series {
            name,
            attributes: (others > 0).then_some(out),
            samples: series.samples.into_iter(),
            histograms: series.histograms.into_iter(),
            exemplars: exemplars(&stored),
            latest: stored.iter().map(|e| e.timestamp).max().unwrap_or(0),
        }
    }

    /// Its next record, its exemplars on its last. `None` when it has none left.
    fn next(&mut self) -> Option<Row> {
        let mut row = if let Some(sample) = self.samples.next() {
            self.sample(&sample)
        } else if let Some(histogram) = self.histograms.next() {
            self.histogram(&histogram)
        } else {
            // Prometheus sends a series' exemplars in a series of their own, with no samples to ride on.
            let exemplars = self.exemplars.take()?;
            let mut row = self.row(self.latest);
            row.0[field::EXEMPLARS] = Some(Cell::Variant(exemplars));
            return Some(row);
        };
        if self.samples.len() == 0 && self.histograms.len() == 0 {
            row.0[field::EXEMPLARS] = self.exemplars.take().map(Cell::Variant);
        }
        Some(row)
    }

    /// A row of this series at `timestamp`, in milliseconds, with every field but its name, labels and time null.
    fn row(&self, timestamp: i64) -> Row {
        let mut row = vec![None; field::ALL];
        row[field::TIME] = Some(Cell::Int(timestamp.saturating_mul(1000)));
        row[field::NAME] = self.name.clone().map(Cell::Text);
        row[field::ATTRIBUTES] = self.attributes.clone().map(Cell::Variant);
        Row(row)
    }

    fn sample(&self, sample: &Sample) -> Row {
        let mut row = self.row(sample.timestamp);
        // A variant, as an OTLP metric point's value is, so a pipeline reads a value from either alike.
        row.0[field::VALUE] = Some(Cell::Variant(variant_of(|out| variant::put_f64(out, sample.value))));
        row.0[field::START_TIME] = start_time(sample.start_timestamp);
        row
    }

    /// `h` as an OTLP histogram point is typed: an exponential one's buckets as `positive` and `negative`, a custom
    /// one's as `bucketCounts` ending at `explicitBounds`. Its reset hint is `resetHint`: `yes` or `no` where the
    /// sender says, null where it does not or `h` is a gauge histogram's. `h` is checked when its request is scanned.
    fn histogram(&self, h: &Histogram) -> Row {
        let mut row = self.row(h.timestamp);
        let cells = &mut row.0;
        cells[field::START_TIME] = start_time(h.start_timestamp);
        cells[field::AGGREGATION_TEMPORALITY] =
            (h.reset_hint != ResetHint::Gauge as i32).then_some(Cell::Int(CUMULATIVE));
        cells[field::RESET_HINT] = match ResetHint::try_from(h.reset_hint) {
            Ok(ResetHint::Yes) => Some(Cell::Text("yes".into())),
            Ok(ResetHint::No) => Some(Cell::Text("no".into())),
            _ => None,
        };
        cells[field::COUNT] = Some(Cell::Float(match h.count {
            Some(HistogramCount::Float(n)) => n,
            Some(HistogramCount::Int(n)) => n as f64,
            None => 0.0,
        }));
        cells[field::SUM] = Some(Cell::Float(h.sum));
        let positive = Side::positive(h)
            .dense()
            .expect("a histogram is checked when its request is scanned");
        if h.schema == CUSTOM_BUCKETS_SCHEMA {
            cells[field::TYPE] = Some(Cell::Text("histogram".into()));
            let mut counts = vec![0.0; h.custom_values.len() + 1];
            if let Some((first, dense)) = positive {
                let first = usize::try_from(first).expect("a histogram is checked when its request is scanned");
                counts[first..first + dense.len()].copy_from_slice(&dense);
            }
            cells[field::BUCKET_COUNTS] = Some(numbers(&counts));
            cells[field::EXPLICIT_BOUNDS] = Some(numbers(&h.custom_values));
            return row;
        }
        cells[field::TYPE] = Some(Cell::Text("exponentialHistogram".into()));
        cells[field::SCALE] = Some(Cell::Int(h.schema.into()));
        cells[field::ZERO_COUNT] = h.zero_count.as_ref().map(|n| {
            Cell::Float(match *n {
                HistogramZeroCount::Int(n) => n as f64,
                HistogramZeroCount::Float(n) => n,
            })
        });
        cells[field::ZERO_THRESHOLD] = (h.zero_threshold != 0.0).then_some(Cell::Float(h.zero_threshold));
        let negative = Side::negative(h)
            .dense()
            .expect("a histogram is checked when its request is scanned");
        cells[field::POSITIVE] = positive.map(buckets);
        cells[field::NEGATIVE] = negative.map(buckets);
        row
    }
}

/// A start time of `millis` as a record's `startTime`. Null for 0, which 2.0 sends for unknown.
fn start_time(millis: i64) -> Option<Cell> {
    (millis != 0).then(|| Cell::Int(millis.saturating_mul(1000)))
}

/// `metadata` as a record of no time, value or attributes: its family as `name`, its help as `description` and its unit
/// as `unit`, each null where empty, and its type as an OTLP metric's: a counter a monotonic cumulative `sum`, a
/// histogram a cumulative `histogram`, a gauge histogram a `histogram` of no temporality, a gauge and a summary as OTLP
/// names them, and an info and a stateset, which OTLP has none of, by their OpenMetrics names. An unknown type is null.
/// `None` for a family of no name: Prometheus drops metadata it cannot store and keeps the request, so it is left out
/// rather than refusing the request.
fn metadata_row(metadata: MetricMetadata) -> Option<Row> {
    if metadata.metric_family_name.is_empty() {
        return None;
    }
    let mut row = vec![None; field::ALL];
    let (kind, temporality) = match MetricType::try_from(metadata.r#type) {
        Ok(MetricType::Counter) => (Some("sum"), Some(CUMULATIVE)),
        Ok(MetricType::Gauge) => (Some("gauge"), None),
        Ok(MetricType::Histogram) => (Some("histogram"), Some(CUMULATIVE)),
        Ok(MetricType::GaugeHistogram) => (Some("histogram"), None),
        Ok(MetricType::Summary) => (Some("summary"), None),
        Ok(MetricType::Info) => (Some("info"), None),
        Ok(MetricType::Stateset) => (Some("stateset"), None),
        Ok(MetricType::Unknown) | Err(_) => (None, None),
    };
    row[field::NAME] = Some(Cell::Text(metadata.metric_family_name));
    row[field::TYPE] = kind.map(|kind| Cell::Text(kind.into()));
    row[field::AGGREGATION_TEMPORALITY] = temporality.map(Cell::Int);
    row[field::IS_MONOTONIC] = (metadata.r#type == MetricType::Counter as i32).then_some(Cell::Bool(true));
    let text = |s: String| (!s.is_empty()).then_some(Cell::Text(s));
    row[field::DESCRIPTION] = text(metadata.help);
    row[field::UNIT] = text(metadata.unit);
    Some(Row(row))
}

/// `exemplars` as an OTLP metric point's are, so a pipeline reads them from either receiver alike: a list of objects,
/// each its `time` (left out at 0), its `value`, the labels `trace_id` and `span_id` as `traceId` and `spanId`, and its
/// other labels as `filteredAttributes` (left out for none). A label of no value is left out. `None` for none.
fn exemplars(exemplars: &[Exemplar]) -> Option<Vec<u8>> {
    (!exemplars.is_empty()).then(|| {
        variant_of(|out| {
            let open = variant::begin_array(out);
            exemplars.iter().for_each(|e| put_exemplar(out, e));
            variant::end(out, open, exemplars.len());
        })
    })
}

fn put_exemplar(out: &mut Vec<u8>, e: &Exemplar) {
    let open = variant::begin_object(out);
    let mut count = 1;
    if e.timestamp != 0 {
        variant::put_key(out, "time");
        variant::put_datetime(out, e.timestamp.saturating_mul(1000));
        count += 1;
    }
    variant::put_key(out, "value");
    variant::put_f64(out, e.value);
    let labels = || e.labels.iter().filter(|l| !l.value.is_empty());
    for (key, name) in [("traceId", TRACE_ID_LABEL), ("spanId", SPAN_ID_LABEL)] {
        if let Some(label) = labels().find(|l| l.name == name) {
            variant::put_key(out, key);
            variant::put_str(out, &label.value);
            count += 1;
        }
    }
    let mut others = labels()
        .filter(|l| l.name != TRACE_ID_LABEL && l.name != SPAN_ID_LABEL)
        .peekable();
    if others.peek().is_some() {
        variant::put_key(out, "filteredAttributes");
        let attributes = variant::begin_object(out);
        let mut n = 0;
        for label in others {
            variant::put_key(out, &label.name);
            variant::put_str(out, &label.value);
            n += 1;
        }
        variant::end(out, attributes, n);
        count += 1;
    }
    variant::end(out, open, count);
}

/// `values` as an array of doubles.
fn numbers(values: &[f64]) -> Cell {
    Cell::Variant(variant_of(|out| put_numbers(out, values)))
}

fn put_numbers(out: &mut Vec<u8>, values: &[f64]) {
    let open = variant::begin_array(out);
    values.iter().for_each(|v| variant::put_f64(out, *v));
    variant::end(out, open, values.len());
}

/// One side's buckets as OTLP's `{offset, bucketCounts}`, from the Prometheus index of its first bucket. Prometheus
/// bucket `i` ends at `base^i` and OTLP bucket `i` starts there, so OTLP's index is one less.
fn buckets((first, counts): (i64, Vec<f64>)) -> Cell {
    Cell::Variant(variant_of(|out| {
        let open = variant::begin_object(out);
        variant::put_key(out, "offset");
        variant::put_i64(out, first - 1);
        variant::put_key(out, "bucketCounts");
        put_numbers(out, &counts);
        variant::end(out, open, 2);
    }))
}

/// One side of zero of a histogram: its spans and its counts, as deltas or as they are.
struct Side<'a> {
    name: &'static str,
    spans: &'a [BucketSpan],
    counts: Counts<'a>,
}

enum Counts<'a> {
    /// An integer histogram's: each bucket's count less the one before's, the first as it is.
    Deltas(&'a [i64]),
    /// A float histogram's.
    Floats(&'a [f64]),
}

/// Where a side's spans put its buckets.
struct Extent {
    /// The index of its first bucket.
    first: i64,
    /// How many buckets lie from its first to its last, gaps between spans included.
    dense: usize,
    /// How many buckets its spans name.
    named: usize,
}

impl<'a> Side<'a> {
    fn positive(h: &'a Histogram) -> Side<'a> {
        Side {
            name: "positive",
            spans: &h.positive_spans,
            counts: Counts::of(h, &h.positive_deltas, &h.positive_counts),
        }
    }

    fn negative(h: &'a Histogram) -> Side<'a> {
        Side {
            name: "negative",
            spans: &h.negative_spans,
            counts: Counts::of(h, &h.negative_deltas, &h.negative_counts),
        }
    }

    /// Where its spans put its buckets, or why they do not read: a span after the first goes back, or the sum
    /// overflows. Allocates nothing, so a request is accounted at what its buckets hold before they are made.
    fn extent(&self) -> Result<Extent, String> {
        let overflow = || format!("a remote_write histogram's {} spans reach past any bucket", self.name);
        let (mut at, mut first, mut end, mut named) = (0i64, None, 0i64, 0usize);
        for (i, span) in self.spans.iter().enumerate() {
            if i > 0 && span.offset < 0 {
                return Err(format!(
                    "a remote_write histogram's {} span {i} starts {} buckets before the span ahead of it ends, \
                     and a span after the first starts at or after it",
                    self.name,
                    span.offset.unsigned_abs()
                ));
            }
            at = at.checked_add(span.offset.into()).ok_or_else(overflow)?;
            let next = at.checked_add(span.length.into()).ok_or_else(overflow)?;
            if span.length > 0 {
                first.get_or_insert(at);
                end = next;
                named = named.checked_add(span.length as usize).ok_or_else(overflow)?;
            }
            at = next;
        }
        let first = first.unwrap_or(0);
        let dense = end
            .checked_sub(first)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(overflow)?;
        Ok(Extent { first, dense, named })
    }

    /// Why its counts are not its spans' buckets, or what they hold as a row: one count per bucket its spans name,
    /// and an integer count never below zero.
    fn check(&self) -> Result<Extent, String> {
        let extent = self.extent()?;
        let len = match self.counts {
            Counts::Deltas(d) => d.len(),
            Counts::Floats(f) => f.len(),
        };
        if len != extent.named {
            return Err(format!(
                "a remote_write histogram's {} spans name {} buckets, and it holds {len} counts for them",
                self.name, extent.named
            ));
        }
        if let Counts::Deltas(deltas) = self.counts {
            let mut count = 0i64;
            for delta in deltas {
                count = count
                    .checked_add(*delta)
                    .filter(|n| *n >= 0)
                    .ok_or_else(|| format!("a remote_write histogram's {} bucket counts go below zero", self.name))?;
            }
        }
        if let Counts::Floats(floats) = self.counts
            && let Some(bad) = floats.iter().find(|n| n.is_nan() || **n < 0.0)
        {
            return Err(format!(
                "a remote_write histogram's {} bucket counts hold {bad}, and a count is a number of zero or more",
                self.name
            ));
        }
        Ok(extent)
    }

    /// The Prometheus index of its first bucket and each count from there to its last, absolute, a gap between spans
    /// as zeros. `None` for no buckets.
    fn dense(&self) -> Result<Option<(i64, Vec<f64>)>, String> {
        let extent = self.check()?;
        if extent.dense == 0 {
            return Ok(None);
        }
        let mut counts = vec![0.0; extent.dense];
        let mut values: Box<dyn Iterator<Item = f64>> = match self.counts {
            Counts::Deltas(deltas) => Box::new(deltas.iter().scan(0i64, |n, d| {
                *n += d;
                Some(*n as f64)
            })),
            Counts::Floats(floats) => Box::new(floats.iter().copied()),
        };
        let mut at = 0i64;
        for span in self.spans {
            at += i64::from(span.offset);
            for _ in 0..span.length {
                counts[(at - extent.first) as usize] = values.next().expect("one count per bucket, checked");
                at += 1;
            }
        }
        Ok(Some((extent.first, counts)))
    }
}

impl<'a> Counts<'a> {
    /// A histogram is a float one when its count is.
    fn of(h: &Histogram, deltas: &'a [i64], floats: &'a [f64]) -> Counts<'a> {
        match h.count {
            Some(HistogramCount::Float(_)) => Counts::Floats(floats),
            _ => Counts::Deltas(deltas),
        }
    }

    /// What its buckets hold together. Its side is checked first, so an integer count is never below zero.
    fn total(&self) -> Total {
        match self {
            Counts::Deltas(deltas) => Total::Int(
                deltas
                    .iter()
                    .scan(0i64, |n, d| {
                        *n += d;
                        Some(*n as u128)
                    })
                    .sum(),
            ),
            Counts::Floats(floats) => Total::Float(floats.iter().sum()),
        }
    }
}

/// What a histogram counts or its buckets hold: exact for integer counts.
#[derive(Clone, Copy)]
enum Total {
    Int(u128),
    Float(f64),
}

impl Total {
    fn float(self) -> f64 {
        match self {
            Total::Int(n) => n as f64,
            Total::Float(n) => n,
        }
    }
}

impl std::ops::Add for Total {
    type Output = Total;

    fn add(self, other: Total) -> Total {
        match (self, other) {
            (Total::Int(a), Total::Int(b)) => Total::Int(a + b),
            (a, b) => Total::Float(a.float() + b.float()),
        }
    }
}

impl std::fmt::Display for Total {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Total::Int(n) => n.fmt(f),
            Total::Float(n) => n.fmt(f),
        }
    }
}

/// What `h`'s zero bucket holds.
fn zero_total(h: &Histogram) -> Total {
    match h.zero_count {
        Some(HistogramZeroCount::Int(n)) => Total::Int(n.into()),
        Some(HistogramZeroCount::Float(n)) => Total::Float(n),
        None => Total::Int(0),
    }
}

/// Why `h`'s count is not what its buckets hold, its zero bucket's included, or `Ok`, as Prometheus checks it.
/// Prometheus counts an observation of NaN and puts it in no bucket, so a histogram whose sum is NaN may count more
/// than its buckets hold; any other counts exactly what they hold. Float counts are compared within
/// [`FLOAT_COUNT_TOLERANCE`]. Its sides are checked first.
fn count_valid(h: &Histogram) -> Result<(), String> {
    let count = match h.count {
        Some(HistogramCount::Int(n)) => Total::Int(n.into()),
        Some(HistogramCount::Float(n)) => Total::Float(n),
        None => Total::Int(0),
    };
    let buckets = zero_total(h) + Side::positive(h).counts.total() + Side::negative(h).counts.total();
    let (over, equal) = match (count, buckets) {
        (Total::Int(c), Total::Int(b)) => (b > c, b == c),
        (c, b) => {
            let (c, b) = (c.float(), b.float());
            let equal = (c - b).abs() <= FLOAT_COUNT_TOLERANCE * c.abs().max(b.abs());
            (b > c && !equal, equal)
        }
    };
    if over {
        return Err(format!(
            "a remote_write histogram counts {count} observations, and its buckets hold {buckets}, more than it counts"
        ));
    }
    if !equal && !h.sum.is_nan() {
        return Err(format!(
            "a remote_write histogram counts {count} observations, and its buckets hold {buckets}, where only a \
             histogram whose sum is NaN counts observations no bucket holds"
        ));
    }
    Ok(())
}

/// Why `h` is not a histogram as remote_write 1.0 has it, or what it holds as a row beyond its decoded fields: each
/// of its dense bucket arrays, so a histogram of few spans far apart is accounted at what its row holds before it is
/// made. A histogram's count is what its buckets hold, and a custom one has no zero bucket, as Prometheus has them.
fn histogram_size(h: &Histogram) -> Result<usize, String> {
    let (positive, negative) = (Side::positive(h), Side::negative(h));
    let numbers = if h.schema == CUSTOM_BUCKETS_SCHEMA {
        if !h.negative_spans.is_empty() || !h.negative_deltas.is_empty() || !h.negative_counts.is_empty() {
            return Err(
                "a remote_write histogram of custom buckets holds negative buckets, which it has none of".into(),
            );
        }
        let zero = zero_total(h);
        if zero.float() != 0.0 || h.zero_threshold != 0.0 {
            return Err(format!(
                "a remote_write histogram of custom buckets has a zero bucket of threshold {} holding {zero}, and \
                 custom buckets have no zero bucket",
                h.zero_threshold
            ));
        }
        let bounds = &h.custom_values;
        if bounds.iter().any(|b| !b.is_finite()) || bounds.windows(2).any(|w| w[0] >= w[1]) {
            return Err(format!(
                "a remote_write histogram's custom bucket bounds {bounds:?} are not finite and strictly increasing"
            ));
        }
        let extent = positive.check()?;
        let last = i64::try_from(bounds.len()).unwrap_or(i64::MAX);
        if extent.dense > 0 && (extent.first < 0 || extent.first + extent.dense as i64 - 1 > last) {
            return Err(format!(
                "a remote_write histogram of {} custom buckets has a span past its buckets, which are 0 to {last}",
                bounds.len() + 1
            ));
        }
        // `bucketCounts` and `explicitBounds`.
        (bounds.len() + 1).saturating_add(bounds.len())
    } else if EXPONENTIAL_SCHEMAS.contains(&h.schema) {
        positive.check()?.dense.saturating_add(negative.check()?.dense)
    } else {
        return Err(format!(
            "a remote_write histogram of schema {}, where a schema is {} to {} or {CUSTOM_BUCKETS_SCHEMA} for custom \
             buckets",
            h.schema,
            EXPONENTIAL_SCHEMAS.start(),
            EXPONENTIAL_SCHEMAS.end()
        ));
    };
    count_valid(h)?;
    numbers
        .checked_mul(VARIANT_NUMBER)
        .ok_or_else(|| "a remote_write histogram holds more buckets than memory".to_string())
}

/// What `h`'s vectors hold decoded.
fn histogram_held(h: &Histogram) -> usize {
    (h.negative_spans.capacity() + h.positive_spans.capacity()) * size_of::<BucketSpan>()
        + (h.negative_deltas.capacity() + h.positive_deltas.capacity()) * size_of::<i64>()
        + (h.negative_counts.capacity() + h.positive_counts.capacity() + h.custom_values.capacity()) * size_of::<f64>()
}

/// Why `labels` is not a series' labels as remote_write 1.0 has them, or `Ok`: each name and value non-empty, and no
/// name twice. A request holding one is refused whole, rather than taken as another series than was sent.
fn labels_valid(labels: &[Label]) -> Result<(), String> {
    let mut names: Vec<&str> = labels.iter().map(|l| l.name.as_str()).collect();
    if let Some(l) = labels.iter().find(|l| l.name.is_empty() || l.value.is_empty()) {
        return Err(format!(
            "a remote_write series holds the label {:?}={:?}, and a label's name and value are not empty",
            l.name, l.value
        ));
    }
    names.sort_unstable();
    match names.windows(2).find(|w| w[0] == w[1]) {
        Some(w) => Err(format!("a remote_write series holds the label {:?} twice", w[0])),
        None => Ok(()),
    }
}

/// Whether an exemplar of `labels` is one a record holds: each label's name non-empty and none named twice, a label
/// of no value left out, as Prometheus leaves it out. Prometheus drops an exemplar it cannot store and keeps the
/// request, so one that is not is left out rather than refusing its series' samples.
fn exemplar_kept(labels: &[Label]) -> bool {
    let mut names: Vec<&str> = (labels.iter())
        .filter(|l| !l.value.is_empty())
        .map(|l| l.name.as_str())
        .collect();
    names.sort_unstable();
    !names.first().is_some_and(|name| name.is_empty()) && !names.windows(2).any(|w| w[0] == w[1])
}

/// What `series`, a `TimeSeries`'s bytes, holds once decoded, read from its fields without decoding it but for its
/// histograms: each label, sample, exemplar and histogram, at the room their vectors grow to as they are pushed, the
/// attributes its labels are written into as it is read, the variant its exemplars are written into, and what each
/// histogram holds and its row's buckets hold. Refuses a histogram that is not one.
fn decoded_size(series: &[u8]) -> Result<usize, String> {
    let (mut labels, mut label_bytes, mut samples, mut histograms, mut histogram_bytes) = (0usize, 0, 0, 0, 0usize);
    let (mut exemplars, mut exemplar_bytes) = (0usize, 0usize);
    let mut at = 0;
    let not = |e: String| format!("not a remote_write request: {e}");
    while let Some(field) = next_field(series, at).map_err(not)? {
        match (field.number, field.wire) {
            (1, 2) => (labels, label_bytes) = (labels + 1, label_bytes + field.value.len()),
            (2, 2) => samples += 1,
            (3, 2) => {
                exemplars += 1;
                exemplar_bytes = exemplar_bytes.saturating_add(exemplar_size(field.value).map_err(not)?);
            }
            (4, 2) => {
                let h = Histogram::decode(field.value).map_err(|e| not(e.to_string()))?;
                let size = histogram_size(&h)?.saturating_add(histogram_held(&h));
                (histograms, histogram_bytes) = (histograms + 1, histogram_bytes.saturating_add(size));
            }
            _ => {}
        }
        at = field.next;
    }
    // The exemplars' variant is an array of them.
    let exemplar_bytes = if exemplars == 0 {
        0
    } else {
        exemplar_bytes.saturating_add(9)
    };
    Ok((size_of::<TimeSeries>()
        + room(labels) * size_of::<Label>()
        + 2 * label_bytes
        + room(samples) * size_of::<Sample>()
        + room(exemplars) * size_of::<Exemplar>()
        + room(histograms) * size_of::<Histogram>())
    .saturating_add(histogram_bytes)
    .saturating_add(exemplar_bytes))
}

/// What `exemplar`, an `Exemplar`'s bytes, holds beyond its struct once decoded, read from its fields: its labels, at
/// the room their vector grows to, and their names and values, and what it is written as in a variant.
fn exemplar_size(exemplar: &[u8]) -> Result<usize, String> {
    let (mut labels, mut label_bytes, mut at) = (0usize, 0usize, 0);
    while let Some(field) = next_field(exemplar, at)? {
        if (field.number, field.wire) == (1, 2) {
            (labels, label_bytes) = (labels + 1, label_bytes + field.value.len());
        }
        at = field.next;
    }
    // A label's bytes on the wire bound its name and value decoded, and once more as they are written.
    Ok(room(labels) * size_of::<Label>() + 2 * label_bytes + labels * VARIANT_LABEL + VARIANT_EXEMPLAR)
}

/// What `metadata`, a `MetricMetadata`'s bytes, holds once decoded: its struct, and its strings, which its bytes on the
/// wire bound. Its row takes those strings as they are.
fn metadata_size(metadata: &[u8]) -> usize {
    size_of::<MetricMetadata>() + metadata.len()
}

/// What `series`, a 2.0 `TimeSeries`'s bytes, holds once decoded and taken as 1.0's, read from its fields as
/// [`decoded_size`] reads 1.0's: its references, its labels and exemplars at the strings they name, its samples and
/// histograms, and its metadata's strings. A reference past the table counts as nothing here; checking the series
/// refuses it.
fn decoded_size_v2(series: &[u8], symbols: &[Range<usize>]) -> Result<usize, String> {
    let not = |e: String| format!("not a remote_write request: {e}");
    let named = |refs: &[u8]| -> Result<(usize, usize), String> {
        let (mut n, mut bytes, mut at) = (0usize, 0usize, 0);
        while at < refs.len() {
            let s = varint(refs, &mut at)?;
            bytes = bytes.saturating_add(
                usize::try_from(s)
                    .ok()
                    .and_then(|s| symbols.get(s))
                    .map_or(0, Range::len),
            );
            n += 1;
        }
        Ok((n, bytes))
    };
    let (mut refs, mut label_bytes, mut samples, mut histograms, mut histogram_bytes) = (0usize, 0usize, 0, 0, 0usize);
    let (mut exemplars, mut exemplar_bytes, mut metadata_bytes) = (0usize, 0usize, 0usize);
    let mut at = 0;
    while let Some(field) = next_field(series, at).map_err(not)? {
        match (field.number, field.wire) {
            // Packed, or one at a time as a sender may send a repeated number.
            (1, 2) | (1, 0) => {
                let (n, bytes) = named(field.value).map_err(not)?;
                (refs, label_bytes) = (refs + n, label_bytes.saturating_add(bytes));
            }
            (2, 2) => samples += 1,
            (3, 2) => {
                let h = Histogram::decode(field.value).map_err(|e| not(e.to_string()))?;
                let size = histogram_size(&h)?.saturating_add(histogram_held(&h));
                (histograms, histogram_bytes) = (histograms + 1, histogram_bytes.saturating_add(size));
            }
            (4, 2) => {
                let (mut n, mut bytes, mut at) = (0usize, 0usize, 0);
                while let Some(field) = next_field(field.value, at).map_err(not)? {
                    if field.number == 1 && (field.wire == 2 || field.wire == 0) {
                        let (more, more_bytes) = named(field.value).map_err(not)?;
                        (n, bytes) = (n + more, bytes.saturating_add(more_bytes));
                    }
                    at = field.next;
                }
                let size = room(n) * size_of::<u32>() + room(n / 2) * size_of::<Label>() + 2 * bytes;
                exemplars += 1;
                exemplar_bytes =
                    (exemplar_bytes.saturating_add(size)).saturating_add((n / 2) * VARIANT_LABEL + VARIANT_EXEMPLAR);
            }
            (5, 2) => {
                let m = MetadataV2::decode(field.value).map_err(|e| not(e.to_string()))?;
                let len = |s: u32| symbols.get(s as usize).map_or(0, Range::len);
                metadata_bytes = size_of::<MetricMetadata>() + len(m.help_ref) + len(m.unit_ref);
            }
            _ => {}
        }
        at = field.next;
    }
    let exemplar_bytes = if exemplars == 0 {
        0
    } else {
        exemplar_bytes.saturating_add(9)
    };
    Ok((size_of::<TimeSeriesV2>()
        + room(refs) * size_of::<u32>()
        + room(refs / 2) * size_of::<Label>()
        + 2 * label_bytes
        + room(samples) * size_of::<Sample>()
        + room(exemplars) * (size_of::<ExemplarV2>() + size_of::<Exemplar>())
        + room(histograms) * size_of::<Histogram>())
    .saturating_add(histogram_bytes)
    .saturating_add(exemplar_bytes)
    // Its family's name, which its labels' bytes bound.
    .saturating_add(if metadata_bytes == 0 {
        0
    } else {
        metadata_bytes + label_bytes
    }))
}

/// The room a vector of `n` items grows to as they are pushed one at a time.
fn room(n: usize) -> usize {
    if n == 0 { 0 } else { n.next_power_of_two().max(4) }
}

/// A `WriteRequest` field that is records: a series, a metric family's metadata, or a 2.0 series, as its message's
/// bytes.
enum Entry<'a> {
    Series(&'a [u8]),
    Metadata(&'a [u8]),
    SeriesV2(&'a [u8]),
}

/// The next field of a request of `version` that is records from `at`, and where the field after it starts, skipping
/// every other field. `None` past the last.
fn next_entry(body: &[u8], mut at: usize, version: Version) -> Result<Option<(Entry<'_>, usize)>, String> {
    while let Some(field) = next_field(body, at)? {
        let (entry, what) = match (version, field.number) {
            (Version::V1, TIMESERIES_FIELD) => (Entry::Series(field.value), "timeseries"),
            (Version::V1, METADATA_FIELD) => (Entry::Metadata(field.value), "metadata"),
            (Version::V2, TIMESERIES_V2_FIELD) => (Entry::SeriesV2(field.value), "timeseries"),
            _ => {
                at = field.next;
                continue;
            }
        };
        return match field.wire {
            2 => Ok(Some((entry, field.next))),
            wire => Err(format!("a {what} as wire type {wire}, where it is a message")),
        };
    }
    Ok(None)
}

/// One protobuf field of a message.
struct Field<'a> {
    number: u64,
    wire: u64,
    /// Its bytes: a length-delimited field's contents.
    value: &'a [u8],
    /// Where the field after it starts.
    next: usize,
}

/// The protobuf field of `message` at `at`. `None` past the last.
fn next_field(message: &[u8], mut at: usize) -> Result<Option<Field<'_>>, String> {
    if at >= message.len() {
        return Ok(None);
    }
    let key = varint(message, &mut at)?;
    let (number, wire) = (key >> 3, key & 7);
    let len = match wire {
        0 => {
            let start = at;
            varint(message, &mut at)?;
            return Ok(Some(Field {
                number,
                wire,
                value: &message[start..at],
                next: at,
            }));
        }
        1 => 8,
        2 => usize::try_from(varint(message, &mut at)?).map_err(|_| "a field longer than memory".to_string())?,
        5 => 4,
        other => return Err(format!("wire type {other}, which remote_write does not send")),
    };
    let end = at
        .checked_add(len)
        .filter(|end| *end <= message.len())
        .ok_or("a field past the body's end")?;
    Ok(Some(Field {
        number,
        wire,
        value: &message[at..end],
        next: end,
    }))
}

fn varint(body: &[u8], at: &mut usize) -> Result<u64, String> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *body.get(*at).ok_or("a varint past the body's end")?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("a varint longer than ten bytes".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, prost::Message)]
    struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        timeseries: Vec<TimeSeries>,
        #[prost(message, repeated, tag = "3")]
        metadata: Vec<MetricMetadata>,
    }

    fn metadata(r#type: i32, family: &str, help: &str, unit: &str) -> MetricMetadata {
        MetricMetadata {
            r#type,
            metric_family_name: family.into(),
            help: help.into(),
            unit: unit.into(),
        }
    }

    fn boolean(row: &Row, at: usize) -> Option<bool> {
        match row.0[at] {
            Some(Cell::Bool(b)) => Some(b),
            None => None,
            ref other => panic!("field {at} is a bool, not {other:?}"),
        }
    }

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.into(),
            value: value.into(),
        }
    }

    fn series(labels: &[(&str, &str)], samples: &[(f64, i64)]) -> TimeSeries {
        TimeSeries {
            labels: labels.iter().map(|(n, v)| label(n, v)).collect(),
            samples: samples
                .iter()
                .map(|&(value, timestamp)| Sample {
                    value,
                    timestamp,
                    start_timestamp: 0,
                })
                .collect(),
            exemplars: Vec::new(),
            histograms: Vec::new(),
        }
    }

    fn snappy(request: &WriteRequest) -> Vec<u8> {
        snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap()
    }

    fn rows(body: &[u8]) -> Vec<Row> {
        let mut samples = Samples::snappy(body, Version::V1, usize::MAX).unwrap();
        std::iter::from_fn(|| samples.next_record()).collect()
    }

    fn name(row: &Row) -> Option<&str> {
        text(row, field::NAME)
    }

    fn time(row: &Row) -> i64 {
        int(row, field::TIME).expect("a sample has a time")
    }

    fn value(row: &Row) -> f64 {
        let Some(Cell::Variant(value)) = &row.0[field::VALUE] else {
            panic!("a sample's value is a variant")
        };
        match variant::read(value) {
            variant::Value::F64(value) => value,
            other => panic!("a sample's value is a double, not {other:?}"),
        }
    }

    /// The row's attributes as label pairs, in their order.
    fn attributes(row: &Row) -> Vec<(String, String)> {
        let Some(Cell::Variant(attributes)) = &row.0[field::ATTRIBUTES] else {
            panic!("attributes are a variant")
        };
        let variant::Value::Object(entries) = variant::read(attributes) else {
            panic!("attributes are an object")
        };
        entries
            .map(|(key, value)| match variant::read(value) {
                variant::Value::Str(text) => (key.to_string(), text.to_string()),
                other => panic!("a label's value is a str, not {other:?}"),
            })
            .collect()
    }

    fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn each_sample_is_a_record_under_its_series_name_and_labels() {
        let request = WriteRequest {
            timeseries: vec![
                series(
                    &[("__name__", "up"), ("job", "api"), ("instance", "a:9100")],
                    &[(1.0, 1_700_000_000_000), (0.0, 1_700_000_015_000)],
                ),
                series(&[("__name__", "temp")], &[(21.5, 1_700_000_000_123)]),
            ],
            metadata: Vec::new(),
        };
        let rows = rows(&snappy(&request));
        assert_eq!(rows.len(), 3);
        assert_eq!(name(&rows[0]), Some("up"));
        assert_eq!(value(&rows[0]), 1.0);
        assert_eq!(time(&rows[0]), 1_700_000_000_000_000);
        assert_eq!(attributes(&rows[0]), pairs(&[("job", "api"), ("instance", "a:9100")]));
        assert_eq!((value(&rows[1]), time(&rows[1])), (0.0, 1_700_000_015_000_000));
        assert_eq!(name(&rows[2]), Some("temp"));
        assert!(
            rows.iter()
                .all(|r| r.0[field::ATTRIBUTES + 1..].iter().all(Option::is_none)),
            "a float sample has nothing past its attributes"
        );
        assert_eq!(
            rows[2].0[field::ATTRIBUTES],
            None,
            "a series with no labels but its name has no attributes"
        );
    }

    #[test]
    fn a_staleness_marker_keeps_its_bits() {
        // Prometheus marks a series gone with this NaN, and a writer downstream reads it by its bits.
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        let request = WriteRequest {
            timeseries: vec![series(&[("__name__", "up")], &[(stale, 1)])],
            metadata: Vec::new(),
        };
        assert_eq!(value(&rows(&snappy(&request))[0]).to_bits(), stale.to_bits());
    }

    #[test]
    fn each_metadata_entry_is_a_record_typed_as_an_otlp_metric() {
        let types = [
            (MetricType::Unknown as i32, None, None, None),
            (MetricType::Counter as i32, Some("sum"), Some(2), Some(true)),
            (MetricType::Gauge as i32, Some("gauge"), None, None),
            (MetricType::Histogram as i32, Some("histogram"), Some(2), None),
            (MetricType::GaugeHistogram as i32, Some("histogram"), None, None),
            (MetricType::Summary as i32, Some("summary"), None, None),
            (MetricType::Info as i32, Some("info"), None, None),
            (MetricType::Stateset as i32, Some("stateset"), None, None),
            (8, None, None, None),
            (-1, None, None, None),
        ];
        let request = WriteRequest {
            timeseries: Vec::new(),
            metadata: (types.iter())
                .map(|&(t, ..)| metadata(t, &format!("family_{t}"), "What it counts.", "seconds"))
                .collect(),
        };
        let rows = rows(&snappy(&request));
        assert_eq!(rows.len(), types.len(), "a request of metadata alone is its records");
        for (row, (t, kind, temporality, monotonic)) in rows.iter().zip(types) {
            assert_eq!(name(row), Some(format!("family_{t}").as_str()));
            assert_eq!(text(row, field::TYPE), kind, "type {t}");
            assert_eq!(int(row, field::AGGREGATION_TEMPORALITY), temporality, "type {t}");
            assert_eq!(boolean(row, field::IS_MONOTONIC), monotonic, "type {t}");
            assert_eq!(text(row, field::DESCRIPTION), Some("What it counts."));
            assert_eq!(text(row, field::UNIT), Some("seconds"));
            let named = [
                field::NAME,
                field::TYPE,
                field::AGGREGATION_TEMPORALITY,
                field::DESCRIPTION,
                field::UNIT,
                field::IS_MONOTONIC,
            ];
            for at in (0..field::ALL).filter(|at| !named.contains(at)) {
                assert_eq!(row.0[at], None, "type {t}, field {at}");
            }
        }
    }

    #[test]
    fn metadata_of_no_help_or_unit_has_them_null() {
        let request = WriteRequest {
            timeseries: Vec::new(),
            metadata: vec![metadata(MetricType::Gauge as i32, "temp", "", "")],
        };
        let rows = rows(&snappy(&request));
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], field::DESCRIPTION), None);
        assert_eq!(text(&rows[0], field::UNIT), None);
    }

    /// Prometheus drops metadata it cannot store and keeps the request.
    #[test]
    fn metadata_of_no_family_name_is_left_out() {
        let request = WriteRequest {
            timeseries: Vec::new(),
            metadata: vec![
                metadata(MetricType::Counter as i32, "", "Nameless.", "bytes"),
                metadata(MetricType::Gauge as i32, "temp", "", ""),
            ],
        };
        let mut samples = Samples::snappy(&snappy(&request), Version::V1, usize::MAX).unwrap();
        assert!(samples.check().is_ok());
        let rows: Vec<_> = std::iter::from_fn(|| samples.next_record()).collect();
        assert_eq!(rows.iter().map(name).collect::<Vec<_>>(), [Some("temp")]);
    }

    #[test]
    fn metadata_beside_series_is_records_in_the_requests_order() {
        let request = WriteRequest {
            timeseries: vec![
                series(&[("__name__", "requests_total"), ("job", "api")], &[(1.0, 1), (2.0, 2)]),
                series(&[("__name__", "temp")], &[(21.5, 3)]),
            ],
            metadata: vec![
                metadata(MetricType::Counter as i32, "requests_total", "Requests served.", ""),
                metadata(MetricType::Gauge as i32, "temp", "", "celsius"),
            ],
        };
        let rows = rows(&snappy(&request));
        let kinds: Vec<_> = rows.iter().map(|r| (name(r), text(r, field::TYPE))).collect();
        assert_eq!(
            kinds,
            [
                (Some("requests_total"), None),
                (Some("requests_total"), None),
                (Some("temp"), None),
                (Some("requests_total"), Some("sum")),
                (Some("temp"), Some("gauge")),
            ]
        );
        for sample in &rows[..3] {
            for at in [field::DESCRIPTION, field::UNIT, field::IS_MONOTONIC] {
                assert_eq!(sample.0[at], None, "a sample has no field {at}");
            }
        }
        assert_eq!(text(&rows[3], field::DESCRIPTION), Some("Requests served."));
        assert_eq!(text(&rows[4], field::UNIT), Some("celsius"));
        assert_eq!(
            (rows[3].0[field::TIME].clone(), rows[3].0[field::ATTRIBUTES].clone()),
            (None, None)
        );
    }

    #[test]
    fn a_request_holds_its_body_and_its_widest_metadata_entry_decoded() {
        let entry = metadata(MetricType::Counter as i32, "requests_total", &"h".repeat(1000), "");
        let request = WriteRequest {
            timeseries: vec![series(&[("__name__", "x")], &[(1.0, 1)])],
            metadata: vec![entry.clone()],
        };
        let samples = Samples::snappy(&snappy(&request), Version::V1, usize::MAX).unwrap();
        assert_eq!(
            samples.size(),
            request.encoded_len() + metadata_size(&entry.encode_to_vec())
        );
        let decoded = MetricMetadata::decode(entry.encode_to_vec().as_slice()).unwrap();
        let held = size_of::<MetricMetadata>()
            + decoded.metric_family_name.capacity()
            + decoded.help.capacity()
            + decoded.unit.capacity();
        assert!(metadata_size(&entry.encode_to_vec()) >= held);
    }

    #[test]
    fn metadata_that_does_not_read_refuses_the_request() {
        // Field 3 as a varint: key (3 << 3) | 0, then 5.
        let body = snap::raw::Encoder::new().compress_vec(&[0x18, 0x05]).unwrap();
        let why = Samples::snappy(&body, Version::V1, usize::MAX).err().unwrap();
        assert!(why.contains("a metadata as wire type 0"), "{why}");
        // Field 3 holding a family name of bytes that are not UTF-8: key (2 << 3) | 2, length 1, 0xff.
        let body = snap::raw::Encoder::new()
            .compress_vec(&[0x1a, 0x03, 0x12, 0x01, 0xff])
            .unwrap();
        let why = Samples::snappy(&body, Version::V1, usize::MAX)
            .unwrap()
            .check()
            .err()
            .unwrap();
        assert!(why.starts_with("not a remote_write request"), "{why}");
    }

    #[test]
    fn an_empty_or_repeated_label_refuses_the_request() {
        for labels in [
            &[("__name__", "x"), ("a", "")][..],
            &[("__name__", "x"), ("", "1")],
            &[("__name__", "x"), ("b", "1"), ("b", "2")],
        ] {
            let request = WriteRequest {
                timeseries: vec![series(labels, &[(1.0, 1)])],
                metadata: Vec::new(),
            };
            let why = Samples::snappy(&snappy(&request), Version::V1, usize::MAX)
                .unwrap()
                .check()
                .err()
                .unwrap();
            assert!(why.starts_with("a remote_write series holds the label"), "{why}");
        }
    }

    #[test]
    fn a_timeseries_that_is_not_a_message_refuses_the_request() {
        // Field 1 as a varint: key (1 << 3) | 0, then 5.
        let body = snap::raw::Encoder::new().compress_vec(&[0x08, 0x05]).unwrap();
        let why = Samples::snappy(&body, Version::V1, usize::MAX).err().unwrap();
        assert!(why.contains("wire type 0"), "{why}");
    }

    #[test]
    fn a_request_holds_its_body_and_its_widest_series_decoded() {
        let narrow = series(&[("__name__", "x")], &[(1.0, 1)]);
        let wide = series(&[("__name__", "y"), ("a", "1")], &[(1.0, 1), (2.0, 2), (3.0, 3)]);
        let request = WriteRequest {
            timeseries: vec![narrow.clone(), wide.clone()],
            metadata: Vec::new(),
        };
        let samples = Samples::snappy(&snappy(&request), Version::V1, usize::MAX).unwrap();
        let widest = decoded_size(&wide.encode_to_vec()).unwrap();
        assert_eq!(samples.size(), request.encoded_len() + widest);
        assert!(widest > decoded_size(&narrow.encode_to_vec()).unwrap());
    }

    #[test]
    fn a_series_is_held_at_what_it_decodes_to_before_it_is_decoded() {
        let empty = Label {
            name: String::new(),
            value: String::new(),
        };
        let series = TimeSeries {
            labels: vec![empty; 1000],
            samples: Vec::new(),
            exemplars: Vec::new(),
            histograms: Vec::new(),
        };
        let request = WriteRequest {
            timeseries: vec![series.clone()],
            metadata: Vec::new(),
        };
        let samples = Samples::snappy(&snappy(&request), Version::V1, usize::MAX).unwrap();
        assert!(samples.size() >= request.encoded_len() + 1000 * size_of::<Label>());
        let decoded = TimeSeries::decode(series.encode_to_vec().as_slice()).unwrap();
        assert!(decoded_size(&series.encode_to_vec()).unwrap() >= decoded.labels.capacity() * size_of::<Label>());
        assert!(
            samples
                .check()
                .err()
                .unwrap()
                .starts_with("a remote_write series holds the label")
        );
    }

    #[test]
    fn a_body_that_is_not_snappy_or_not_a_request_is_refused_whole() {
        assert!(
            Samples::snappy(b"not snappy at all", Version::V1, usize::MAX)
                .err()
                .unwrap()
                .contains("snappy")
        );
        let truncated = {
            let request = WriteRequest {
                timeseries: vec![series(&[("__name__", "up")], &[(1.0, 1)])],
                metadata: Vec::new(),
            };
            let mut bytes = request.encode_to_vec();
            bytes.truncate(bytes.len() - 3);
            snap::raw::Encoder::new().compress_vec(&bytes).unwrap()
        };
        let why = Samples::snappy(&truncated, Version::V1, usize::MAX).err().unwrap();
        assert!(why.starts_with("not a remote_write request"), "{why}");
    }

    fn span(offset: i32, length: u32) -> BucketSpan {
        BucketSpan { offset, length }
    }

    /// An integer counter histogram of schema 3 with nothing in it, at 1 ms.
    fn histogram() -> Histogram {
        Histogram {
            count: Some(HistogramCount::Int(0)),
            sum: 0.0,
            schema: 3,
            zero_threshold: 0.0,
            zero_count: None,
            negative_spans: Vec::new(),
            negative_deltas: Vec::new(),
            negative_counts: Vec::new(),
            positive_spans: Vec::new(),
            positive_deltas: Vec::new(),
            positive_counts: Vec::new(),
            reset_hint: ResetHint::No as i32,
            timestamp: 1,
            custom_values: Vec::new(),
            start_timestamp: 0,
        }
    }

    fn histograms(histograms: Vec<Histogram>) -> Vec<u8> {
        let mut series = series(&[("__name__", "latency"), ("job", "api")], &[]);
        series.histograms = histograms;
        snappy(&WriteRequest {
            timeseries: vec![series],
            metadata: Vec::new(),
        })
    }

    fn float(row: &Row, at: usize) -> Option<f64> {
        match row.0[at] {
            Some(Cell::Float(f)) => Some(f),
            None => None,
            ref other => panic!("field {at} is a double, not {other:?}"),
        }
    }

    fn int(row: &Row, at: usize) -> Option<i64> {
        match row.0[at] {
            Some(Cell::Int(i)) => Some(i),
            None => None,
            ref other => panic!("field {at} is an int, not {other:?}"),
        }
    }

    fn text(row: &Row, at: usize) -> Option<&str> {
        match &row.0[at] {
            Some(Cell::Text(t)) => Some(t),
            None => None,
            other => panic!("field {at} is text, not {other:?}"),
        }
    }

    fn doubles(value: &[u8]) -> Vec<f64> {
        let variant::Value::Array(items) = variant::read(value) else {
            panic!("an array")
        };
        items
            .map(|item| match variant::read(item) {
                variant::Value::F64(f) => f,
                other => panic!("a double, not {other:?}"),
            })
            .collect()
    }

    fn array(row: &Row, at: usize) -> Option<Vec<f64>> {
        match &row.0[at] {
            Some(Cell::Variant(v)) => Some(doubles(v)),
            None => None,
            other => panic!("field {at} is a variant, not {other:?}"),
        }
    }

    /// A side's buckets as `(offset, bucketCounts)`.
    fn side(row: &Row, at: usize) -> Option<(i64, Vec<f64>)> {
        let Some(Cell::Variant(v)) = &row.0[at] else {
            assert_eq!(row.0[at], None);
            return None;
        };
        let variant::Value::Object(entries) = variant::read(v) else {
            panic!("buckets are an object")
        };
        let entries: Vec<_> = entries.collect();
        assert_eq!(entries.len(), 2);
        assert_eq!((entries[0].0, entries[1].0), ("offset", "bucketCounts"));
        let variant::Value::I64(offset) = variant::read(entries[0].1) else {
            panic!("an offset is an int")
        };
        Some((offset, doubles(entries[1].1)))
    }

    fn refused(h: Histogram) -> String {
        Samples::snappy(&histograms(vec![h]), Version::V1, usize::MAX)
            .err()
            .unwrap()
    }

    #[test]
    fn a_row_holds_the_records_fields_in_its_order() {
        let meta = crate::plan::topology::SourceMeta::prometheus_receiver();
        let names: Vec<&str> = meta.record().iter().map(|(name, _)| name.as_ref()).collect();
        assert_eq!(names.len(), field::ALL);
        for (at, name) in [
            (field::TIME, "time"),
            (field::NAME, "name"),
            (field::VALUE, "value"),
            (field::ATTRIBUTES, "attributes"),
            (field::TYPE, "type"),
            (field::AGGREGATION_TEMPORALITY, "aggregationTemporality"),
            (field::COUNT, "count"),
            (field::SUM, "sum"),
            (field::BUCKET_COUNTS, "bucketCounts"),
            (field::EXPLICIT_BOUNDS, "explicitBounds"),
            (field::SCALE, "scale"),
            (field::ZERO_COUNT, "zeroCount"),
            (field::ZERO_THRESHOLD, "zeroThreshold"),
            (field::POSITIVE, "positive"),
            (field::NEGATIVE, "negative"),
            (field::RESET_HINT, "resetHint"),
            (field::EXEMPLARS, "exemplars"),
            (field::DESCRIPTION, "description"),
            (field::UNIT, "unit"),
            (field::IS_MONOTONIC, "isMonotonic"),
        ] {
            assert_eq!(names[at], name);
        }
    }

    #[test]
    fn an_integer_histogram_is_an_exponential_histogram_of_absolute_dense_counts() {
        let h = Histogram {
            count: Some(HistogramCount::Int(20)),
            sum: 12.5,
            zero_count: Some(HistogramZeroCount::Int(6)),
            zero_threshold: 0.001,
            // Buckets 2, 3 and 5, a gap at 4.
            positive_spans: vec![span(2, 2), span(1, 1)],
            positive_deltas: vec![3, -1, 2],
            negative_spans: vec![span(-1, 1)],
            negative_deltas: vec![5],
            timestamp: 1_700_000_000_123,
            ..histogram()
        };
        let rows = rows(&histograms(vec![h]));
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(name(row), Some("latency"));
        assert_eq!(attributes(row), pairs(&[("job", "api")]));
        assert_eq!(time(row), 1_700_000_000_123_000);
        assert_eq!(row.0[field::VALUE], None);
        assert_eq!(text(row, field::TYPE), Some("exponentialHistogram"));
        assert_eq!(int(row, field::AGGREGATION_TEMPORALITY), Some(2));
        assert_eq!(float(row, field::COUNT), Some(20.0));
        assert_eq!(float(row, field::SUM), Some(12.5));
        assert_eq!(int(row, field::SCALE), Some(3));
        assert_eq!(float(row, field::ZERO_COUNT), Some(6.0));
        assert_eq!(float(row, field::ZERO_THRESHOLD), Some(0.001));
        // Prometheus bucket 2 is OTLP bucket 1.
        assert_eq!(side(row, field::POSITIVE), Some((1, vec![3.0, 2.0, 0.0, 4.0])));
        assert_eq!(side(row, field::NEGATIVE), Some((-2, vec![5.0])));
        assert_eq!(array(row, field::BUCKET_COUNTS), None);
        assert_eq!(array(row, field::EXPLICIT_BOUNDS), None);
    }

    #[test]
    fn a_float_histogram_keeps_its_counts_as_they_are() {
        let h = Histogram {
            count: Some(HistogramCount::Float(7.5)),
            sum: 3.25,
            positive_spans: vec![span(0, 2)],
            positive_counts: vec![1.5, 6.0],
            ..histogram()
        };
        let row = &rows(&histograms(vec![h]))[0];
        assert_eq!(float(row, field::COUNT), Some(7.5));
        assert_eq!(side(row, field::POSITIVE), Some((-1, vec![1.5, 6.0])));
        assert_eq!(side(row, field::NEGATIVE), None, "a side with no buckets is null");
        assert_eq!(float(row, field::ZERO_COUNT), None);
        assert_eq!(float(row, field::ZERO_THRESHOLD), None, "a zero threshold of 0 is null");
    }

    #[test]
    fn a_gauge_histogram_has_no_temporality() {
        let h = Histogram {
            reset_hint: ResetHint::Gauge as i32,
            ..histogram()
        };
        let row = &rows(&histograms(vec![h]))[0];
        assert_eq!(int(row, field::AGGREGATION_TEMPORALITY), None);
        assert_eq!(text(row, field::TYPE), Some("exponentialHistogram"));
    }

    #[test]
    fn a_custom_bucket_histogram_is_a_histogram_of_explicit_bounds() {
        let h = Histogram {
            count: Some(HistogramCount::Int(5)),
            sum: 1.75,
            schema: CUSTOM_BUCKETS_SCHEMA,
            custom_values: vec![0.1, 0.5, 1.0],
            // Buckets 0, 2 and 3 of 0 to 3.
            positive_spans: vec![span(0, 1), span(1, 2)],
            positive_deltas: vec![2, 1, -3],
            ..histogram()
        };
        let row = &rows(&histograms(vec![h]))[0];
        assert_eq!(text(row, field::TYPE), Some("histogram"));
        assert_eq!(array(row, field::BUCKET_COUNTS), Some(vec![2.0, 0.0, 3.0, 0.0]));
        assert_eq!(array(row, field::EXPLICIT_BOUNDS), Some(vec![0.1, 0.5, 1.0]));
        assert_eq!(float(row, field::COUNT), Some(5.0));
        assert_eq!(float(row, field::SUM), Some(1.75));
        assert_eq!(int(row, field::AGGREGATION_TEMPORALITY), Some(2));
        for at in [
            field::SCALE,
            field::ZERO_COUNT,
            field::ZERO_THRESHOLD,
            field::POSITIVE,
            field::NEGATIVE,
        ] {
            assert_eq!(row.0[at], None, "field {at}");
        }
    }

    #[test]
    fn a_series_gives_its_samples_then_its_histograms() {
        let mut mixed = series(&[("__name__", "latency")], &[(1.0, 1), (2.0, 2)]);
        mixed.histograms = vec![histogram(), histogram()];
        let request = WriteRequest {
            timeseries: vec![mixed, series(&[("__name__", "up")], &[(1.0, 3)])],
            metadata: Vec::new(),
        };
        let rows = rows(&snappy(&request));
        let kinds: Vec<_> = rows.iter().map(|r| (name(r), text(r, field::TYPE))).collect();
        let histogram = Some("exponentialHistogram");
        assert_eq!(
            kinds,
            [
                (Some("latency"), None),
                (Some("latency"), None),
                (Some("latency"), histogram),
                (Some("latency"), histogram),
                (Some("up"), None),
            ]
        );
        assert_eq!(value(&rows[1]), 2.0);
    }

    #[test]
    fn a_histogram_whose_buckets_do_not_add_up_refuses_the_request() {
        let custom = || Histogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            custom_values: vec![1.0, 2.0],
            ..histogram()
        };
        for (h, why) in [
            (
                Histogram {
                    positive_spans: vec![span(0, 2)],
                    positive_deltas: vec![1],
                    ..histogram()
                },
                "spans name 2 buckets, and it holds 1 counts",
            ),
            (
                Histogram {
                    count: Some(HistogramCount::Float(1.0)),
                    negative_spans: vec![span(0, 1)],
                    negative_deltas: vec![1],
                    ..histogram()
                },
                "negative spans name 1 buckets, and it holds 0 counts",
            ),
            (
                Histogram {
                    count: Some(HistogramCount::Float(1.0)),
                    positive_spans: vec![span(0, 2)],
                    positive_counts: vec![2.0, -1.0],
                    ..histogram()
                },
                "positive bucket counts hold -1, and a count is a number of zero or more",
            ),
            (
                Histogram {
                    positive_spans: vec![span(0, 2)],
                    positive_deltas: vec![1, -2],
                    ..histogram()
                },
                "positive bucket counts go below zero",
            ),
            (
                Histogram {
                    positive_spans: vec![span(0, 1), span(-1, 1)],
                    positive_deltas: vec![1, 0],
                    ..histogram()
                },
                "span 1 starts 1 buckets before",
            ),
            (
                Histogram {
                    schema: 9,
                    ..histogram()
                },
                "of schema 9",
            ),
            (
                Histogram {
                    schema: -5,
                    ..histogram()
                },
                "of schema -5",
            ),
            (
                Histogram {
                    negative_spans: vec![span(0, 1)],
                    negative_deltas: vec![1],
                    ..custom()
                },
                "custom buckets holds negative buckets",
            ),
            (
                Histogram {
                    custom_values: vec![1.0, 1.0],
                    ..custom()
                },
                "not finite and strictly increasing",
            ),
            (
                Histogram {
                    custom_values: vec![1.0, f64::INFINITY],
                    ..custom()
                },
                "not finite and strictly increasing",
            ),
            (
                Histogram {
                    positive_spans: vec![span(1, 3)],
                    positive_deltas: vec![1, 0, 0],
                    ..custom()
                },
                "has a span past its buckets",
            ),
            (
                Histogram {
                    positive_spans: vec![span(-1, 1)],
                    positive_deltas: vec![1],
                    ..custom()
                },
                "has a span past its buckets",
            ),
        ] {
            let refused = refused(h);
            assert!(refused.contains(why), "{refused}, not {why}");
        }
    }

    #[test]
    fn a_histogram_counts_what_its_buckets_hold() {
        // A zero bucket of 1 and buckets of 2 and 3: 6 in all.
        let int = |count: u64, sum: f64| Histogram {
            count: Some(HistogramCount::Int(count)),
            sum,
            zero_count: Some(HistogramZeroCount::Int(1)),
            positive_spans: vec![span(0, 1)],
            positive_deltas: vec![2],
            negative_spans: vec![span(0, 1)],
            negative_deltas: vec![3],
            ..histogram()
        };
        let float = |count: f64, sum: f64| Histogram {
            count: Some(HistogramCount::Float(count)),
            sum,
            zero_count: Some(HistogramZeroCount::Float(0.1)),
            positive_spans: vec![span(0, 1)],
            positive_counts: vec![0.2],
            ..histogram()
        };
        for (h, why) in [
            (
                int(7, 1.0),
                "counts 7 observations, and its buckets hold 6, where only a histogram whose sum is NaN",
            ),
            (
                int(5, 1.0),
                "counts 5 observations, and its buckets hold 6, more than it counts",
            ),
            (
                int(5, f64::NAN),
                "counts 5 observations, and its buckets hold 6, more than it counts",
            ),
            (float(0.4, 1.0), "counts 0.4 observations, and its buckets hold 0.3"),
            (float(0.2, f64::NAN), "more than it counts"),
        ] {
            let refused = refused(h);
            assert!(refused.contains(why), "{refused}, not {why}");
        }
        // 0.1 + 0.2 is not 0.3 in doubles, and is within the tolerance of it.
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        let taken = [
            int(6, 1.0),
            int(7, f64::NAN),
            float(0.3, 1.0),
            float(0.4, f64::NAN),
            Histogram {
                sum: stale,
                ..histogram()
            },
        ];
        assert_eq!(rows(&histograms(taken.to_vec())).len(), taken.len());
        // An integer histogram's counts are compared exactly, past what a double holds.
        let exact = |count: u64| Histogram {
            count: Some(HistogramCount::Int(count)),
            zero_count: Some(HistogramZeroCount::Int(u64::MAX)),
            positive_spans: vec![span(0, 1)],
            positive_deltas: vec![1],
            ..histogram()
        };
        assert!(refused(exact(u64::MAX)).contains("more than it counts"));
        // What the buckets hold wraps to 0 in a u64.
        assert!(refused(exact(0)).contains("more than it counts"));
    }

    #[test]
    fn a_custom_bucket_histogram_has_no_zero_bucket() {
        let custom = || Histogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            custom_values: vec![1.0],
            ..histogram()
        };
        for h in [
            Histogram {
                count: Some(HistogramCount::Int(1)),
                zero_count: Some(HistogramZeroCount::Int(1)),
                ..custom()
            },
            Histogram {
                count: Some(HistogramCount::Float(0.5)),
                zero_count: Some(HistogramZeroCount::Float(0.5)),
                ..custom()
            },
            Histogram {
                zero_threshold: 0.001,
                ..custom()
            },
        ] {
            let refused = refused(h);
            assert!(refused.contains("custom buckets have no zero bucket"), "{refused}");
        }
        let empty = Histogram {
            zero_count: Some(HistogramZeroCount::Int(0)),
            ..custom()
        };
        assert_eq!(rows(&histograms(vec![empty])).len(), 1);
    }

    #[test]
    fn a_histogram_keeps_its_reset_hint() {
        let hinted = [ResetHint::Yes, ResetHint::No, ResetHint::Unknown, ResetHint::Gauge]
            .map(|hint| Histogram {
                reset_hint: hint as i32,
                ..histogram()
            })
            .to_vec();
        let mut series = series(&[("__name__", "latency")], &[(1.0, 1)]);
        series.histograms = hinted;
        let rows = rows(&snappy(&WriteRequest {
            timeseries: vec![series],
            metadata: Vec::new(),
        }));
        let hints: Vec<_> = rows.iter().map(|r| text(r, field::RESET_HINT)).collect();
        assert_eq!(
            hints,
            [None, Some("yes"), Some("no"), None, None],
            "a float sample has none"
        );
    }

    #[test]
    fn a_histogram_is_held_at_its_dense_buckets_before_they_are_made() {
        // A few bytes on the wire, two buckets 2^31 apart.
        let h = Histogram {
            count: Some(HistogramCount::Int(2)),
            positive_spans: vec![span(0, 1), span(i32::MAX, 1)],
            positive_deltas: vec![1, 0],
            ..histogram()
        };
        let samples = Samples::snappy(&histograms(vec![h]), Version::V1, usize::MAX).unwrap();
        assert!(samples.size() > (1 << 31) * VARIANT_NUMBER, "{}", samples.size());
    }

    fn exemplar(labels: &[(&str, &str)], value: f64, timestamp: i64) -> Exemplar {
        Exemplar {
            labels: labels.iter().map(|(n, v)| label(n, v)).collect(),
            value,
            timestamp,
        }
    }

    /// A row's exemplars, each its entries in their order, a string as it is, a number as its text, a time as
    /// `@micros`, and `filteredAttributes` as its pairs `k=v` joined by commas.
    fn exemplars_of(row: &Row) -> Option<Vec<Vec<(String, String)>>> {
        let Some(Cell::Variant(v)) = &row.0[field::EXEMPLARS] else {
            assert_eq!(row.0[field::EXEMPLARS], None);
            return None;
        };
        let variant::Value::Array(items) = variant::read(v) else {
            panic!("exemplars are an array")
        };
        let shown = |v: &[u8]| match variant::read(v) {
            variant::Value::Str(s) => s.to_string(),
            variant::Value::F64(f) => f.to_string(),
            variant::Value::DateTime(t) => format!("@{t}"),
            variant::Value::Object(entries) => {
                let pairs: Vec<_> = entries
                    .map(|(k, v)| match variant::read(v) {
                        variant::Value::Str(s) => format!("{k}={s}"),
                        other => panic!("a filtered attribute is a str, not {other:?}"),
                    })
                    .collect();
                pairs.join(",")
            }
            other => panic!("an exemplar holds no {other:?}"),
        };
        Some(
            items
                .map(|item| {
                    let variant::Value::Object(entries) = variant::read(item) else {
                        panic!("an exemplar is an object")
                    };
                    entries.map(|(k, v)| (k.to_string(), shown(v))).collect()
                })
                .collect(),
        )
    }

    #[test]
    fn a_series_exemplars_ride_on_its_last_sample() {
        let mut up = series(&[("__name__", "requests"), ("job", "api")], &[(1.0, 1), (2.0, 2)]);
        up.exemplars = vec![
            exemplar(
                &[("trace_id", "abc"), ("span_id", "def"), ("path", "/x"), ("empty", "")],
                0.25,
                1_700_000_000_123,
            ),
            exemplar(&[], 0.5, 0),
        ];
        let request = WriteRequest {
            timeseries: vec![up, series(&[("__name__", "other")], &[(3.0, 3)])],
            metadata: Vec::new(),
        };
        let rows = rows(&snappy(&request));
        assert_eq!(rows.len(), 3);
        assert_eq!(
            exemplars_of(&rows[0]),
            None,
            "only a series' last record has its exemplars"
        );
        assert_eq!(exemplars_of(&rows[2]), None, "a series without exemplars has none");
        assert_eq!(
            exemplars_of(&rows[1]),
            Some(vec![
                pairs(&[
                    ("time", "@1700000000123000"),
                    ("value", "0.25"),
                    ("traceId", "abc"),
                    ("spanId", "def"),
                    ("filteredAttributes", "path=/x"),
                ]),
                pairs(&[("value", "0.5")]),
            ])
        );
        assert_eq!(value(&rows[1]), 2.0);
    }

    #[test]
    fn a_series_exemplars_ride_on_its_last_histogram() {
        let mut latency = series(&[("__name__", "latency")], &[(1.0, 1)]);
        latency.histograms = vec![histogram(), histogram()];
        latency.exemplars = vec![exemplar(&[("trace_id", "abc")], 0.1, 5)];
        let request = WriteRequest {
            timeseries: vec![latency],
            metadata: Vec::new(),
        };
        let rows = rows(&snappy(&request));
        let with: Vec<_> = rows.iter().map(|r| exemplars_of(r).is_some()).collect();
        assert_eq!(with, [false, false, true]);
        assert_eq!(text(&rows[2], field::TYPE), Some("exponentialHistogram"));
    }

    #[test]
    fn a_series_of_exemplars_alone_is_one_record_of_them() {
        let mut alone = series(&[("__name__", "requests"), ("job", "api")], &[]);
        alone.exemplars = vec![
            exemplar(&[("trace_id", "a")], 1.0, 3_000),
            exemplar(&[("trace_id", "b")], 2.0, 7_000),
            exemplar(&[("trace_id", "c")], 3.0, 5_000),
        ];
        let request = WriteRequest {
            timeseries: vec![alone],
            metadata: Vec::new(),
        };
        let rows = rows(&snappy(&request));
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(name(row), Some("requests"));
        assert_eq!(attributes(row), pairs(&[("job", "api")]));
        assert_eq!(time(row), 7_000_000, "the latest exemplar's time");
        assert_eq!(exemplars_of(row).unwrap().len(), 3);
        for at in [field::VALUE]
            .into_iter()
            .chain(field::ATTRIBUTES + 1..field::EXEMPLARS)
        {
            assert_eq!(row.0[at], None, "field {at}");
        }
    }

    /// Prometheus drops an exemplar it cannot store and keeps the request.
    #[test]
    fn an_exemplar_label_of_no_name_or_named_twice_leaves_it_out() {
        let mut s = series(&[("__name__", "x")], &[(1.0, 1)]);
        s.exemplars = vec![
            exemplar(&[("", "x")], 1.0, 1),
            exemplar(&[("trace_id", "a"), ("trace_id", "b")], 2.0, 1),
            exemplar(&[("a", ""), ("a", "x"), ("", "")], 3.0, 1),
        ];
        let rows = rows(&snappy(&WriteRequest {
            timeseries: vec![s],
            metadata: Vec::new(),
        }));
        assert_eq!(rows.len(), 1);
        let kept = exemplars_of(&rows[0]).unwrap();
        assert_eq!(kept.len(), 1);
        assert!(kept[0].contains(&("value".to_string(), "3".to_string())), "{kept:?}");
    }

    #[test]
    fn a_series_of_exemplars_none_kept_is_no_record() {
        let mut s = series(&[("__name__", "x")], &[]);
        s.exemplars = vec![exemplar(&[("", "x")], 1.0, 1)];
        let rows = rows(&snappy(&WriteRequest {
            timeseries: vec![s],
            metadata: Vec::new(),
        }));
        assert!(rows.is_empty());
    }

    #[test]
    fn a_series_is_held_with_its_exemplars_decoded_and_written() {
        let bare = series(&[("__name__", "x")], &[(1.0, 1)]);
        let mut with = bare.clone();
        with.exemplars = vec![exemplar(&[("trace_id", "abc"), ("path", "/x")], 1.0, 1); 3];
        let more = decoded_size(&with.encode_to_vec()).unwrap() - decoded_size(&bare.encode_to_vec()).unwrap();
        let written = exemplars(&with.exemplars).unwrap().len();
        let decoded = TimeSeries::decode(with.encode_to_vec().as_slice()).unwrap();
        let held = decoded.exemplars.capacity() * size_of::<Exemplar>()
            + (decoded.exemplars.iter())
                .map(|e| e.labels.capacity() * size_of::<Label>())
                .sum::<usize>();
        assert!(more >= written + held, "{more} < {written} + {held}");
    }

    #[test]
    fn the_snappy_header_says_what_a_body_holds_decompressed() {
        let request = WriteRequest {
            timeseries: vec![series(&[("__name__", "up")], &[(1.0, 1)])],
            metadata: Vec::new(),
        };
        let plain = request.encode_to_vec();
        assert_eq!(decompressed_len(&snappy(&request)), Ok(plain.len()));
    }

    /// A remote_write 2.0 request, its strings each in `symbols`.
    #[derive(Clone, PartialEq, prost::Message)]
    struct RequestV2 {
        #[prost(string, repeated, tag = "4")]
        symbols: Vec<String>,
        #[prost(message, repeated, tag = "5")]
        timeseries: Vec<TimeSeriesV2>,
    }

    /// Builds a 2.0 request, adding each string to its table once.
    #[derive(Default)]
    struct V2 {
        request: RequestV2,
    }

    impl V2 {
        fn new() -> V2 {
            V2 {
                request: RequestV2 {
                    symbols: vec![String::new()],
                    timeseries: Vec::new(),
                },
            }
        }

        fn symbol(&mut self, s: &str) -> u32 {
            let symbols = &mut self.request.symbols;
            let at = symbols.iter().position(|x| x == s).unwrap_or_else(|| {
                symbols.push(s.into());
                symbols.len() - 1
            });
            at as u32
        }

        fn refs(&mut self, labels: &[(&str, &str)]) -> Vec<u32> {
            labels
                .iter()
                .flat_map(|(n, v)| [self.symbol(n), self.symbol(v)])
                .collect()
        }

        fn metadata(&mut self, kind: MetricType, help: &str, unit: &str) -> MetadataV2 {
            MetadataV2 {
                r#type: kind as i32,
                help_ref: self.symbol(help),
                unit_ref: self.symbol(unit),
            }
        }

        fn series(&mut self, labels: &[(&str, &str)], samples: &[(f64, i64, i64)]) -> &mut TimeSeriesV2 {
            let labels_refs = self.refs(labels);
            self.request.timeseries.push(TimeSeriesV2 {
                labels_refs,
                samples: (samples.iter())
                    .map(|&(value, timestamp, start_timestamp)| Sample {
                        value,
                        timestamp,
                        start_timestamp,
                    })
                    .collect(),
                ..TimeSeriesV2::default()
            });
            self.request.timeseries.last_mut().unwrap()
        }

        fn body(&self) -> Vec<u8> {
            snap::raw::Encoder::new()
                .compress_vec(&self.request.encode_to_vec())
                .unwrap()
        }

        fn samples(&self) -> Result<Samples, String> {
            let samples = Samples::snappy(&self.body(), Version::V2, usize::MAX)?;
            samples.check()?;
            Ok(samples)
        }

        fn rows(&self) -> Vec<Row> {
            let mut samples = self.samples().unwrap();
            std::iter::from_fn(|| samples.next_record()).collect()
        }
    }

    #[test]
    fn a_v2_sample_reads_as_a_v1_sample_with_its_start_time() {
        let mut v2 = V2::new();
        v2.series(
            &[("__name__", "http_requests_total"), ("job", "api")],
            &[(5.0, 1_700_000_000_000, 1_699_999_000_000), (6.0, 1_700_000_015_000, 0)],
        );
        let rows = v2.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(name(&rows[0]), Some("http_requests_total"));
        assert_eq!(attributes(&rows[0]), pairs(&[("job", "api")]));
        assert_eq!((value(&rows[0]), time(&rows[0])), (5.0, 1_700_000_000_000_000));
        assert_eq!(int(&rows[0], field::START_TIME), Some(1_699_999_000_000_000));
        assert_eq!(int(&rows[1], field::START_TIME), None, "a start time of 0 is unknown");
    }

    #[test]
    fn a_v1_sample_has_no_start_time() {
        let request = WriteRequest {
            timeseries: vec![series(&[("__name__", "up")], &[(1.0, 1)])],
            metadata: Vec::new(),
        };
        assert_eq!(rows(&snappy(&request))[0].0[field::START_TIME], None);
    }

    #[test]
    fn a_v2_histogram_carries_its_start_time() {
        let mut v2 = V2::new();
        let h = Histogram {
            start_timestamp: 1_000,
            ..histogram()
        };
        v2.series(&[("__name__", "latency")], &[]).histograms = vec![h];
        let rows = v2.rows();
        assert_eq!(text(&rows[0], field::TYPE), Some("exponentialHistogram"));
        assert_eq!(int(&rows[0], field::START_TIME), Some(1_000_000));
    }

    #[test]
    fn a_v2_family_has_one_metadata_record_per_request_ahead_of_its_samples() {
        let mut v2 = V2::new();
        let counter = v2.metadata(MetricType::Counter, "Requests served.", "");
        let classic = v2.metadata(MetricType::Histogram, "Request latency.", "seconds");
        for (labels, metadata) in [
            (
                vec![("__name__", "http_requests_total"), ("code", "200")],
                counter.clone(),
            ),
            (vec![("__name__", "http_requests_total"), ("code", "500")], counter),
            (vec![("__name__", "latency_bucket"), ("le", "1")], classic.clone()),
            (vec![("__name__", "latency_sum")], classic.clone()),
            (vec![("__name__", "latency_count")], classic),
        ] {
            v2.series(&labels, &[(1.0, 1, 0)]).metadata = Some(metadata);
        }
        let rows = v2.rows();
        let kinds: Vec<_> = (rows.iter()).map(|r| (name(r), r.0[field::TIME].is_none())).collect();
        assert_eq!(
            kinds,
            [
                (Some("http_requests_total"), true),
                (Some("http_requests_total"), false),
                (Some("http_requests_total"), false),
                (Some("latency"), true),
                (Some("latency_bucket"), false),
                (Some("latency_sum"), false),
                (Some("latency_count"), false),
            ]
        );
        assert_eq!(text(&rows[0], field::TYPE), Some("sum"));
        assert_eq!(boolean(&rows[0], field::IS_MONOTONIC), Some(true));
        assert_eq!(text(&rows[0], field::DESCRIPTION), Some("Requests served."));
        assert_eq!(text(&rows[0], field::UNIT), None);
        assert_eq!(text(&rows[3], field::TYPE), Some("histogram"));
        assert_eq!(text(&rows[3], field::UNIT), Some("seconds"));
    }

    #[test]
    fn a_gauge_histogram_s_sum_and_count_name_its_family_either_way() {
        for suffix in ["_gsum", "_gcount", "_sum", "_count", "_bucket"] {
            let mut v2 = V2::new();
            let metadata = v2.metadata(MetricType::GaugeHistogram, "Queue depth.", "");
            let series = format!("depth{suffix}");
            v2.series(&[("__name__", &series)], &[(1.0, 1, 0)]).metadata = Some(metadata);
            assert_eq!(name(&v2.rows()[0]), Some("depth"), "{suffix}");
        }
    }

    #[test]
    fn a_native_histogram_keeps_its_name_as_its_family() {
        let mut v2 = V2::new();
        let metadata = v2.metadata(MetricType::Histogram, "", "");
        let series = v2.series(&[("__name__", "requests_count")], &[]);
        series.histograms = vec![histogram()];
        series.metadata = Some(metadata);
        assert_eq!(name(&v2.rows()[0]), Some("requests_count"));
    }

    #[test]
    fn v2_metadata_that_says_nothing_is_no_record() {
        let mut v2 = V2::new();
        let metadata = v2.metadata(MetricType::Unknown, "", "");
        v2.series(&[("__name__", "up")], &[(1.0, 1, 0)]).metadata = Some(metadata);
        let rows = v2.rows();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].0[field::TIME].is_some());
    }

    #[test]
    fn v2_exemplars_take_their_labels_from_the_table() {
        let mut v2 = V2::new();
        let labels_refs = v2.refs(&[("trace_id", "abc"), ("user", "u1")]);
        let bad = v2.refs(&[("", "x")]);
        let series = v2.series(&[("__name__", "latency")], &[(1.0, 1, 0)]);
        series.exemplars = vec![
            ExemplarV2 {
                labels_refs,
                value: 0.5,
                timestamp: 2,
            },
            ExemplarV2 {
                labels_refs: bad,
                value: 0.7,
                timestamp: 3,
            },
        ];
        let samples = v2.samples().unwrap();
        assert_eq!(
            samples.check(),
            Ok(Written {
                samples: 1,
                histograms: 0,
                exemplars: 1
            }),
            "an exemplar left out is not written"
        );
        let rows = v2.rows();
        let Some(Cell::Variant(exemplars)) = &rows[0].0[field::EXEMPLARS] else {
            panic!("the sample carries its exemplars")
        };
        let variant::Value::Array(mut items) = variant::read(exemplars) else {
            panic!("exemplars are an array")
        };
        let variant::Value::Object(fields) = variant::read(items.next().unwrap()) else {
            panic!("an exemplar is an object")
        };
        let keys: Vec<_> = fields.map(|(k, _)| k.to_string()).collect();
        assert_eq!(keys, ["time", "value", "traceId", "filteredAttributes"]);
        assert!(items.next().is_none());
    }

    #[test]
    fn a_v2_request_that_breaks_the_protocol_is_refused() {
        let refused = |v2: &V2| v2.samples().err().expect("refused");
        let mut v2 = V2::new();
        v2.series(&[("__name__", "up")], &[(1.0, 1, 0)]).labels_refs.push(99);
        assert!(refused(&v2).contains("two"), "{}", refused(&v2));
        let mut v2 = V2::new();
        let series = v2.series(&[("__name__", "up")], &[(1.0, 1, 0)]);
        series.labels_refs.extend([99, 99]);
        assert!(refused(&v2).contains("past the request's"), "{}", refused(&v2));
        let mut v2 = V2::new();
        v2.series(&[("__name__", "up")], &[(1.0, 1, 0)]).histograms = vec![histogram()];
        assert!(refused(&v2).contains("one or the other"), "{}", refused(&v2));
        let mut v2 = V2::new();
        v2.series(&[("__name__", "up")], &[(1.0, 1, 0)]);
        v2.request.symbols[0] = "x".into();
        assert!(refused(&v2).contains("first symbol"), "{}", refused(&v2));
        let mut v2 = V2::new();
        let metadata = MetadataV2 {
            r#type: MetricType::Gauge as i32,
            help_ref: 99,
            unit_ref: 0,
        };
        v2.series(&[("__name__", "up")], &[(1.0, 1, 0)]).metadata = Some(metadata);
        assert!(refused(&v2).contains("past the request's"), "{}", refused(&v2));
    }

    #[test]
    fn a_v2_symbol_that_is_not_utf8_is_refused() {
        // A request of one symbol, its one byte not UTF-8.
        let body = snap::raw::Encoder::new().compress_vec(&[0x22, 1, 0xff]).unwrap();
        let why = Samples::snappy(&body, Version::V2, usize::MAX).err().unwrap();
        assert!(why.contains("UTF-8"), "{why}");
    }

    #[test]
    fn a_v2_table_too_large_to_hold_is_counted_and_not_made() {
        // A million empty strings: two bytes each on the wire, a range each once made.
        let mut v2 = V2::new();
        v2.request.symbols = vec![String::new(); 1_000_000];
        let body = v2.body();
        let limit = 4_000_000;
        let samples = Samples::snappy(&body, Version::V2, limit).unwrap();
        assert!(samples.symbols.is_empty(), "the table is not made");
        assert!(samples.size() > limit, "{}", samples.size());
        assert!(samples.check().err().unwrap().contains("more than a receiver holds"));
        let samples = Samples::snappy(&body, Version::V2, usize::MAX).unwrap();
        assert_eq!(samples.symbols.len(), 1_000_000);
    }

    #[test]
    fn v2_series_naming_one_long_string_are_refused_past_the_limit() {
        let mut v2 = V2::new();
        let long = "v".repeat(100_000);
        for _ in 0..100 {
            v2.series(&[("__name__", "up"), ("big", &long)], &[]);
        }
        let why = Samples::snappy(&v2.body(), Version::V2, 1_000_000).err().unwrap();
        assert!(why.contains("decode to more than 1000000 bytes"), "{why}");
        assert!(Samples::snappy(&v2.body(), Version::V2, 100_000_000).is_ok());
    }

    #[test]
    fn a_v2_request_of_series_and_no_symbols_is_refused() {
        let mut v2 = V2::new();
        v2.series(&[], &[(1.0, 1, 0)]);
        v2.request.symbols.clear();
        let why = v2.samples().err().unwrap();
        assert!(why.contains("no symbols"), "{why}");
    }

    #[test]
    fn a_v2_request_counts_its_table_and_its_labels() {
        let mut v2 = V2::new();
        let long = "v".repeat(10_000);
        v2.series(&[("__name__", "up"), ("big", &long)], &[(1.0, 1, 0)]);
        let samples = v2.samples().unwrap();
        // Its label's value lies once in its body and once more in its record's attributes, and is decoded once.
        assert!(
            samples.size() >= samples.body.len() + 2 * long.len(),
            "{}",
            samples.size()
        );
    }

    #[test]
    fn content_types() {
        assert_eq!(readable(Some("application/x-protobuf")), Ok(Version::V1));
        assert_eq!(
            readable(Some("application/x-protobuf;proto=prometheus.WriteRequest")),
            Ok(Version::V1)
        );
        assert_eq!(
            readable(Some("application/x-protobuf; proto=io.prometheus.write.v2.Request")),
            Ok(Version::V2)
        );
        assert!(readable(Some("application/x-protobuf;proto=io.prometheus.write.v3.Request")).is_err());
        assert!(readable(Some("application/json")).is_err());
        assert!(readable(None).is_err());
        assert!(is_snappy(Some("snappy")) && !is_snappy(Some("gzip")) && !is_snappy(None));
    }
}
