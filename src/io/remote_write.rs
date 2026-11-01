//! The Prometheus destination, as a [`Protocol`] over the HTTP core (`io::http`): rows sent as remote_write 1.0, a
//! snappy-compressed protobuf `WriteRequest`, or as remote_write 2.0, an `io.prometheus.write.v2.Request`, to one URL.
//!
//! A row is one point of a series, in the record a Prometheus receiver gives and an OTLP metric point shares: `name`
//! is its series' `__name__`, `attributes` its other labels, `time` the point's. Its `type` says what the point is:
//!
//! | `type` | sent as |
//! |---|---|
//! | none, `gauge`, `sum` | a sample of `value` |
//! | `exponentialHistogram` | a native histogram of the schema `scale`, downscaled to 8 when finer |
//! | `histogram` | classic series: `<name>_bucket` for each of `explicitBounds` and `+Inf` as `le`, counting up to it; `<name>_sum`; `<name>_count` |
//!
//! remote_write 1.0 carries no custom buckets, so a `histogram` goes as Prometheus's classic series, each a sample.
//!
//! A row's `exemplars` are its series' exemplars, each its `traceId` as the label `trace_id`, its `spanId` as
//! `span_id` and its `filteredAttributes` as the rest, at its `time` or else the row's. A `histogram`'s go on the
//! `_bucket` series of the bucket that holds each one's value, as Prometheus exposes a classic histogram's. A row of no
//! `type`, `gauge` or `sum` with no `value` and some exemplars carries those alone, as a Prometheus receiver gives a
//! series that has exemplars and no sample. Prometheus drops an exemplar it cannot store and keeps its sample, so an
//! exemplar it would not store is left out and its row still sent.
//!
//! A family's metadata, its type, help and unit, goes as the request's `MetricMetadata` for `name`, once a request,
//! the first in the batch's order. A row of no `time`, no `value` and no exemplars is that metadata alone, as a
//! Prometheus receiver gives a family's: `description` its help and `unit` its unit. A point's row with a
//! `description` or `unit` carries its family's too, as an OTLP point does; a `histogram`'s is for `name`, not its
//! classic series. Its `type` gives the family's:
//!
//! | `type` | family's type |
//! |---|---|
//! | none | unknown |
//! | `gauge` | gauge |
//! | `sum` | counter where `isMonotonic` is true, else gauge |
//! | `histogram`, `exponentialHistogram` | histogram where `aggregationTemporality` is 2, else gauge histogram |
//! | `summary`, `info`, `stateset` | its own |
//!
//! remote_write 2.0 carries the same series, each label a string of the request's table, and a family's metadata on
//! each of its series rather than once a request. Metadata reaches this destination apart from points, a Prometheus
//! receiver giving it once a minute, so the destination remembers each family's latest, up to [`FAMILIES`] of them
//! and [`FAMILY_BYTES`] of their strings, and sends it on every series of the family: a series named the family, or a
//! classic histogram's or summary's series named it with its suffix. A row's `startTime` is each of its points' start
//! time. 1.0 has none, so drops it.
//!
//! A store refuses a point older than its series' newest, and Prometheus appends a `TimeSeries`' samples before its
//! histograms. So a series' points in a request go oldest first, one `TimeSeries` for each run of samples or of
//! histograms among them, a row's exemplars with its point's run and their own oldest first.
//!
//! What an answer means, as Prometheus and every remote_write store answer:
//!
//! | status | meaning |
//! |---|---|
//! | 2xx | landed |
//! | 2xx to 2.0 naming nothing written | the destination stalls: the store takes 1.0 alone, reading 2.0 as empty |
//! | 429, 5xx | retried, backing off |
//! | 400, 409, 413, 422 to 2.0 naming something written | landed; what it did not write counts as unwritten |
//! | 400, 409, 413, 422 | split until the row at fault is alone, which is refused |
//! | 415 to 2.0 | the destination stalls: the store takes 1.0 alone |
//! | anything else | the destination stalls, naming the status |

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use prost::Message;

use super::http::{HttpTransport, Order, Outcome, Protocol, Response, Unwritten};
use super::prometheus::{
    BucketSpan, Exemplar, Histogram, HistogramCount, HistogramZeroCount, Label, NAME_LABEL, ResetHint, Sample,
    TimeSeries,
};
use super::{IoError, OwnedRecord};
use crate::format::json_scan::{Next, Reader};
use crate::plan::topology::{PrometheusDestinationConfig, RemoteWriteVersion};

/// The `WriteRequest` this destination sends: its series, and each family's metadata once.
#[derive(Clone, PartialEq, prost::Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
    #[prost(message, repeated, tag = "3")]
    metadata: Vec<MetricMetadata>,
}

/// The `io.prometheus.write.v2.Request` this destination sends: its strings, and its series, each naming its strings
/// by their place in `symbols`.
#[derive(Clone, PartialEq, prost::Message)]
struct RequestV2 {
    #[prost(string, repeated, tag = "4")]
    symbols: Vec<String>,
    #[prost(message, repeated, tag = "5")]
    timeseries: Vec<TimeSeriesV2>,
}

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

/// The `Content-Type` of remote_write 2.0.
const V2_CONTENT_TYPE: &str = "application/x-protobuf;proto=io.prometheus.write.v2.Request";

/// The headers a 2.0 receiver answers with, saying how many of each it wrote.
const WRITTEN: [&str; 3] = [
    "x-prometheus-remote-write-samples-written",
    "x-prometheus-remote-write-histograms-written",
    "x-prometheus-remote-write-exemplars-written",
];

/// How many families' metadata a 2.0 destination remembers. Past it, a new family's takes the place of another's.
const FAMILIES: usize = 10_000;

/// How many bytes of names, help and units a 2.0 destination remembers. Past it, a new family's takes the place of
/// others', and one larger than all of it goes only with its own request.
const FAMILY_BYTES: usize = 16 << 20;

/// Each family's latest metadata, by its name, within [`FAMILIES`] and [`FAMILY_BYTES`].
#[derive(Debug, Default)]
struct Families {
    by_name: HashMap<String, MetricMetadata>,
    bytes: usize,
}

impl Families {
    fn remember(&mut self, metadata: &MetricMetadata) {
        let size = |m: &MetricMetadata| m.metric_family_name.len() + m.help.len() + m.unit.len();
        if let Some(old) = self.by_name.remove(&metadata.metric_family_name) {
            self.bytes -= size(&old);
        }
        if size(metadata) > FAMILY_BYTES {
            return;
        }
        while self.by_name.len() >= FAMILIES || self.bytes + size(metadata) > FAMILY_BYTES {
            let evicted = self.by_name.keys().next().cloned().expect("a full map has a key");
            let old = self.by_name.remove(&evicted).expect("the key is in the map");
            self.bytes -= size(&old);
        }
        self.bytes += size(metadata);
        self.by_name
            .insert(metadata.metric_family_name.clone(), metadata.clone());
    }
}

/// One family's metadata, as remote_write 1.0 carries it.
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

/// A family's type, as remote_write 1.0 numbers it.
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

/// A series' labels, `__name__` among them, sorted by name, as they are sent.
type Labels = Vec<(String, String)>;

/// One row as the points it sends, each of its own series, all at the row's time, and its family's metadata. A row
/// is sent or refused whole.
#[derive(Clone, Debug, PartialEq)]
pub struct Item {
    points: Vec<SeriesPoint>,
    /// Milliseconds since the epoch, or [`METADATA_TIME`] for metadata alone.
    time: i64,
    /// When its points' series started counting, in milliseconds since the epoch; 0 for unknown.
    start: i64,
    metadata: Option<MetricMetadata>,
}

/// The time of a row that is metadata alone. It has none, and a store orders no metadata against points, so it goes
/// first: a family's metadata then lands in the first request of its batch, no later than its points.
const METADATA_TIME: i64 = i64::MIN;

/// What a row sends of one series.
#[derive(Clone, Debug, PartialEq)]
struct SeriesPoint {
    labels: Labels,
    /// None for a row that carries its series' exemplars alone.
    point: Option<Point>,
    /// As they are sent, their labels sorted by name.
    exemplars: Vec<Exemplar>,
}

/// What a row sends of a series at its time.
#[derive(Clone, Debug, PartialEq)]
enum Point {
    Sample(f64),
    /// As it is sent, its timestamp the row's time.
    Histogram(Box<Histogram>),
}

/// The finest schema Prometheus takes.
const FINEST_SCHEMA: i64 = 8;

/// The coarsest schema Prometheus takes.
const COARSEST_SCHEMA: i64 = -4;

/// The least count a double cannot tell from the one after it, so an integer histogram carries only counts below it.
const INEXACT_FROM: f64 = 9_007_199_254_740_992.0;

/// The label of a classic histogram's `_bucket` series that names where its bucket ends.
const BUCKET_LABEL: &str = "le";

/// remote_write's framing and answers for one URL.
#[derive(Clone, Debug)]
pub struct RemoteWrite {
    /// The URL's trailing slashes, which the transport trims off its base address and this puts back.
    path: String,
    version: RemoteWriteVersion,
    /// The metadata 2.0 sends on the series of families a request carries none of.
    families: Arc<Mutex<Families>>,
    /// Counts what a 2.0 store says it did not write, where the destination reports on itself.
    unwritten: Option<Unwritten>,
}

impl RemoteWrite {
    pub fn new(config: &PrometheusDestinationConfig) -> RemoteWrite {
        let url = config.url.as_str();
        RemoteWrite {
            path: url[url.trim_end_matches('/').len()..].to_string(),
            version: config.version,
            families: Arc::default(),
            unwritten: None,
        }
    }

    pub fn with_unwritten(mut self, unwritten: Unwritten) -> Self {
        self.unwritten = Some(unwritten);
        self
    }
}

/// A transport to `config.url` that sends remote_write of its version, carrying its committed headers and
/// `credentials`, each a header's name and its value as this worker resolved it.
pub fn transport(
    config: &PrometheusDestinationConfig,
    credentials: &[(String, String)],
    agent: &ureq::config::Config,
) -> HttpTransport {
    let (content_type, version) = match config.version {
        RemoteWriteVersion::V1 => ("application/x-protobuf", "0.1.0"),
        RemoteWriteVersion::V2 => (V2_CONTENT_TYPE, "2.0.0"),
    };
    let mut headers: Vec<(String, String)> = config.headers.iter().chain(credentials).cloned().collect();
    headers.push(("Content-Encoding".to_string(), "snappy".to_string()));
    headers.push(("X-Prometheus-Remote-Write-Version".to_string(), version.to_string()));
    // A redirect would land nothing and carry the credential headers to wherever it points.
    HttpTransport::new(&config.url, content_type, headers, agent).without_redirects()
}

/// A JSON value as the row writes it, read when it is used.
#[derive(Clone, Copy)]
struct Json<'d>(&'d str);

impl<'d> Json<'d> {
    fn next(self) -> Option<Next<'d>> {
        Reader::of_str(self.0).value().ok()
    }

    fn is_null(self) -> bool {
        self.0 == "null"
    }

    /// The number, where an f64 holds it.
    fn as_f64(self) -> Option<f64> {
        match self.next()? {
            Next::Number(n) => n.parse().ok().filter(|n: &f64| n.is_finite()),
            _ => None,
        }
    }

    /// The number, where it is written whole and an i64 holds it.
    fn as_i64(self) -> Option<i64> {
        match self.next()? {
            Next::Number(n) => n.parse().ok(),
            _ => None,
        }
    }

    fn as_str(self) -> Option<Cow<'d, str>> {
        match self.next()? {
            Next::Str(s) => Some(s),
            _ => None,
        }
    }

    /// An object's members sorted by name, a name written twice holding its last value.
    fn members(self) -> Option<Vec<(Cow<'d, str>, Json<'d>)>> {
        let mut r = Reader::of_str(self.0);
        if r.value().ok()? != Next::Object {
            return None;
        }
        let mut members: Vec<(Cow<'d, str>, Json<'d>)> = Vec::new();
        while let Some(name) = r.key().ok()? {
            members.push((name, Json::read(self.0, &mut r)?));
        }
        // Reversed, a stable sort puts the last value of each name first, which `dedup_by` keeps.
        members.reverse();
        members.sort_by(|a, b| a.0.cmp(&b.0));
        members.dedup_by(|later, kept| later.0 == kept.0);
        Some(members)
    }

    fn items(self) -> Option<Vec<Json<'d>>> {
        let mut r = Reader::of_str(self.0);
        if r.value().ok()? != Next::Array {
            return None;
        }
        let mut items = Vec::new();
        while r.item().ok()? {
            items.push(Json::read(self.0, &mut r)?);
        }
        Some(items)
    }

    /// The value `r`, a reader of `text`, is at.
    fn read(text: &'d str, r: &mut Reader<'d>) -> Option<Json<'d>> {
        let raw = r.raw().ok()?.len();
        // `raw` ends where the reader is, so it is cut from `text` rather than checked as UTF-8 again.
        text.get(r.at() - raw..r.at()).map(Json)
    }
}

/// The member `name` of `members`.
fn member<'d>(members: &[(Cow<'d, str>, Json<'d>)], name: &str) -> Option<Json<'d>> {
    members.iter().find(|(given, _)| given == name).map(|(_, value)| *value)
}

/// The row's fields this destination reads. A JSON null reads as none.
#[derive(Default)]
struct Fields<'d> {
    name: Option<Json<'d>>,
    value: Option<Json<'d>>,
    time: Option<Json<'d>>,
    attributes: Option<Json<'d>>,
    kind: Option<Json<'d>>,
    aggregation_temporality: Option<Json<'d>>,
    count: Option<Json<'d>>,
    sum: Option<Json<'d>>,
    bucket_counts: Option<Json<'d>>,
    explicit_bounds: Option<Json<'d>>,
    reset_hint: Option<Json<'d>>,
    scale: Option<Json<'d>>,
    zero_count: Option<Json<'d>>,
    zero_threshold: Option<Json<'d>>,
    positive: Option<Json<'d>>,
    negative: Option<Json<'d>>,
    exemplars: Option<Json<'d>>,
    description: Option<Json<'d>>,
    unit: Option<Json<'d>>,
    is_monotonic: Option<Json<'d>>,
    start_time: Option<Json<'d>>,
}

impl<'d> Fields<'d> {
    /// None for a row that is no JSON object, or that names one of these fields twice.
    fn read(row: &'d [u8]) -> Option<Fields<'d>> {
        let text = std::str::from_utf8(row.trim_ascii()).ok()?;
        let mut r = Reader::of_str(text);
        if r.value().ok()? != Next::Object {
            return None;
        }
        let mut fields = Fields::default();
        let mut seen = 0u32;
        while let Some(key) = r.key().ok()? {
            let (i, field) = match &*key {
                "name" => (0, &mut fields.name),
                "value" => (1, &mut fields.value),
                "time" => (2, &mut fields.time),
                "attributes" => (3, &mut fields.attributes),
                "type" => (4, &mut fields.kind),
                "aggregationTemporality" => (5, &mut fields.aggregation_temporality),
                "count" => (6, &mut fields.count),
                "sum" => (7, &mut fields.sum),
                "bucketCounts" => (8, &mut fields.bucket_counts),
                "explicitBounds" => (9, &mut fields.explicit_bounds),
                "resetHint" => (10, &mut fields.reset_hint),
                "scale" => (11, &mut fields.scale),
                "zeroCount" => (12, &mut fields.zero_count),
                "zeroThreshold" => (13, &mut fields.zero_threshold),
                "positive" => (14, &mut fields.positive),
                "negative" => (15, &mut fields.negative),
                "exemplars" => (16, &mut fields.exemplars),
                "description" => (17, &mut fields.description),
                "unit" => (18, &mut fields.unit),
                "isMonotonic" => (19, &mut fields.is_monotonic),
                "startTime" => (20, &mut fields.start_time),
                _ => {
                    r.skip().ok()?;
                    continue;
                }
            };
            if seen & 1 << i != 0 {
                return None;
            }
            seen |= 1 << i;
            *field = Some(Json::read(text, &mut r)?).filter(|value| !value.is_null());
        }
        r.end().then_some(fields)
    }
}

/// The type of the family of a row of `fields`, as the module's table gives it.
fn metric_type(fields: &Fields) -> Result<MetricType, String> {
    let kind = match fields.kind {
        None => return Ok(MetricType::Unknown),
        Some(kind) => kind.as_str().ok_or("a row's type is a string in the field type")?,
    };
    let cumulative = fields.aggregation_temporality.and_then(Json::as_i64) == Some(2);
    Ok(match &*kind {
        "gauge" => MetricType::Gauge,
        "sum" if fields.is_monotonic.and_then(Json::next) == Some(Next::Bool(true)) => MetricType::Counter,
        "sum" => MetricType::Gauge,
        "histogram" | "exponentialHistogram" if cumulative => MetricType::Histogram,
        "histogram" | "exponentialHistogram" => MetricType::GaugeHistogram,
        "summary" => MetricType::Summary,
        "info" => MetricType::Info,
        "stateset" => MetricType::Stateset,
        kind => {
            return Err(format!(
                "a family's type is gauge, sum, histogram, exponentialHistogram, summary, info or stateset, and \
                 {kind:?} is none of them"
            ));
        }
    })
}

/// The text of a string field, empty when it is none or no string.
fn text(value: Option<Json<'_>>) -> Cow<'_, str> {
    value.and_then(Json::as_str).unwrap_or_default()
}

/// The metadata of `name`'s family, from the row of `fields`.
fn metadata(fields: &Fields, name: &str) -> Result<MetricMetadata, String> {
    Ok(MetricMetadata {
        r#type: metric_type(fields)? as i32,
        metric_family_name: name.to_string(),
        help: text(fields.description).into_owned(),
        unit: text(fields.unit).into_owned(),
    })
}

/// A sample's value.
fn sample(fields: &Fields) -> Result<f64, String> {
    (fields.value.and_then(Json::as_f64))
        .ok_or_else(|| "a sample's value is a number in the field value; a NaN or an infinity arrives as none".into())
}

/// The label value of an attribute, none when it is null or the empty string, as Prometheus reads a label with no
/// value. `what` names the attribute in a refusal.
fn label_value(value: Json<'_>, what: impl FnOnce() -> String) -> Result<Option<String>, String> {
    let value = match value.next() {
        Some(Next::Null) => return Ok(None),
        Some(Next::Bool(b)) => b.to_string(),
        Some(given @ (Next::Str(_) | Next::Number(_))) => super::splunk::text(given)
            .ok_or_else(|| format!("{} is a number no double holds, and a label's value is text", what()))?,
        _ => return Err(format!("{} is no string, and a label's value is one", what())),
    };
    Ok(Some(value).filter(|v| !v.is_empty()))
}

/// The most runes Prometheus stores in an exemplar's label names and values together.
const EXEMPLAR_LABEL_RUNES: usize = 128;

/// The exemplars of `items` Prometheus stores, each at its own time or else at `time`, the row's. Prometheus drops an
/// exemplar it cannot store and keeps its sample, so one that is not an exemplar is left out, not refused.
fn exemplars(items: Vec<Json<'_>>, time: i64) -> Vec<Exemplar> {
    items.into_iter().filter_map(|item| exemplar(item, time)).collect()
}

/// One exemplar, `{time, value, traceId, spanId, filteredAttributes}`, as a Prometheus receiver gives it and OTLP's
/// shares. None when it has no value or time Prometheus reads, or its trace and span alone pass
/// [`EXEMPLAR_LABEL_RUNES`]. An attribute that is no label, or names the trace or span label again, is left out, and
/// so are all of them where they would pass that limit: the trace and span are what an exemplar is for.
fn exemplar(item: Json<'_>, time: i64) -> Option<Exemplar> {
    let item = item.members()?;
    let timestamp = match member(&item, "time").filter(|time| !time.is_null()) {
        None => time,
        Some(text) => crate::ops::timeparse::parse_auto(&text.as_str()?)?.div_euclid(1000),
    };
    // A NaN value, which Prometheus holds, arrives as none.
    let value = member(&item, "value").and_then(Json::as_f64)?;
    let mut ids = Vec::new();
    for (field, name) in [("traceId", "trace_id"), ("spanId", "span_id")] {
        if let Some(id) = member(&item, field).and_then(Json::as_str)
            && !id.is_empty()
        {
            ids.push(Label {
                name: name.to_string(),
                value: id.into_owned(),
            });
        }
    }
    let mut attributes = Vec::new();
    if let Some(object) = member(&item, "filteredAttributes").and_then(Json::members) {
        for (name, value) in object {
            if name.is_empty() || ids.iter().any(|id| id.name == name) {
                continue;
            }
            if let Ok(Some(value)) = label_value(value, String::new) {
                attributes.push(Label {
                    name: name.into_owned(),
                    value,
                });
            }
        }
    }
    let runes = |labels: &[Label]| -> usize {
        (labels.iter())
            .map(|l| l.name.chars().count() + l.value.chars().count())
            .sum()
    };
    if runes(&ids) > EXEMPLAR_LABEL_RUNES {
        return None;
    }
    if runes(&ids) + runes(&attributes) > EXEMPLAR_LABEL_RUNES {
        attributes.clear();
    }
    let mut labels = ids;
    labels.append(&mut attributes);
    labels.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    Some(Exemplar {
        labels,
        value,
        timestamp,
    })
}

/// Prometheus's hash of `labels`, sorted by name: the xxhash64 of each name and value, each followed by `0xff`.
fn labels_hash(labels: &[Label]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    for label in labels {
        for part in [&label.name, &label.value] {
            hasher.write(part.as_bytes());
            hasher.write(&[0xff]);
        }
    }
    hasher.finish()
}

/// A count in `field`: a number, not negative.
fn count(value: Json<'_>, field: &str) -> Result<f64, String> {
    match value.as_f64() {
        Some(count) if count >= 0.0 => Ok(count),
        Some(_) => Err(format!("a histogram's {field} is negative, and a count is not")),
        None => Err(format!(
            "a histogram's {field} is a number; a NaN or an infinity arrives as none"
        )),
    }
}

/// The counts of the array in `field`, none when it is absent.
fn counts(value: Option<Json<'_>>, field: &str) -> Result<Vec<f64>, String> {
    match value.filter(|value| !value.is_null()) {
        None => Ok(Vec::new()),
        Some(value) => (value.items())
            .ok_or_else(|| format!("a histogram's {field} is an array of counts"))?
            .into_iter()
            .map(|c| count(c, field))
            .collect(),
    }
}

/// Whether a histogram is cumulative, as its `aggregationTemporality` says: 2 for a cumulative one, none for a gauge
/// histogram.
fn cumulative(fields: &Fields) -> Result<bool, String> {
    match fields.aggregation_temporality.map(Json::as_i64) {
        None => Ok(false),
        Some(Some(2)) => Ok(true),
        Some(Some(1)) => Err(
            "this histogram is a delta, and the destination sends no delta histogram; a cumulative one has \
             aggregationTemporality 2"
                .into(),
        ),
        Some(_) => Err("a histogram's aggregationTemporality is 2, cumulative, or none, a gauge histogram's".into()),
    }
}

/// A histogram's `count` and `sum`.
fn count_and_sum(fields: &Fields) -> Result<(f64, f64), String> {
    let count = count(
        fields
            .count
            .ok_or("a histogram's count is a number in the field count")?,
        "count",
    )?;
    // A staleness marker's sum is a NaN, which arrives as none.
    let sum = (fields.sum.and_then(Json::as_f64))
        .ok_or("a histogram's sum is a number in the field sum; a NaN or an infinity arrives as none")?;
    Ok((count, sum))
}

/// The reset hint a histogram carries: its `resetHint`, `yes` or `no`, as Prometheus's receiver gives it; else
/// unknown for a cumulative one and gauge for a gauge histogram.
fn reset_hint(fields: &Fields) -> Result<ResetHint, String> {
    let cumulative = cumulative(fields)?;
    let Some(hint) = fields.reset_hint else {
        return Ok(if cumulative {
            ResetHint::Unknown
        } else {
            ResetHint::Gauge
        });
    };
    let given = match hint.as_str().as_deref() {
        Some("yes") => ResetHint::Yes,
        Some("no") => ResetHint::No,
        _ => return Err("a histogram's resetHint is \"yes\", \"no\" or none".into()),
    };
    if !cumulative {
        return Err(
            "this histogram has a resetHint, which says whether a cumulative histogram reset, and no \
             aggregationTemporality, which makes it a gauge histogram"
                .into(),
        );
    }
    Ok(given)
}

/// A histogram as sent but for its buckets and timestamp, with float counts: its `count`, `sum`, `zeroCount` and
/// `zeroThreshold`, and its reset hint.
fn histogram(fields: &Fields) -> Result<Histogram, String> {
    let reset_hint = reset_hint(fields)?;
    let (count, sum) = count_and_sum(fields)?;
    let zero_count = (fields.zero_count).map_or(Ok(0.0), |z| self::count(z, "zeroCount"))?;
    let zero_threshold = match fields.zero_threshold {
        None => 0.0,
        Some(threshold) => threshold.as_f64().ok_or("a histogram's zeroThreshold is a number")?,
    };
    Ok(Histogram {
        count: Some(HistogramCount::Float(count)),
        sum,
        zero_threshold,
        zero_count: Some(HistogramZeroCount::Float(zero_count)),
        reset_hint: reset_hint as i32,
        ..Histogram::default()
    })
}

/// An exponential histogram, its schema `scale` downscaled to the finest Prometheus takes.
fn exponential(fields: &Fields) -> Result<Histogram, String> {
    let mut h = histogram(fields)?;
    let scale = (fields.scale.and_then(Json::as_i64))
        .ok_or("an exponential histogram's scale is an integer in the field scale")?;
    if scale < COARSEST_SCHEMA {
        return Err(format!(
            "this histogram's scale is {scale}, and the destination sends none coarser than {COARSEST_SCHEMA}"
        ));
    }
    // Prometheus's own OTLP ingest downscales a finer histogram the same way, merging buckets.
    let by = u32::try_from(scale - FINEST_SCHEMA).unwrap_or(0);
    h.schema = scale.min(FINEST_SCHEMA) as i32;
    (h.positive_spans, h.positive_counts) = side(fields.positive, "positive", by)?;
    (h.negative_spans, h.negative_counts) = side(fields.negative, "negative", by)?;
    Ok(integral(h))
}

/// One side of an exponential histogram, `{offset, bucketCounts}` in OTLP's buckets, as Prometheus's spans and the
/// count of each bucket they cover, its buckets merged `by` schemas coarser.
fn side(value: Option<Json<'_>>, field: &str, by: u32) -> Result<(Vec<BucketSpan>, Vec<f64>), String> {
    let (offset, counts) = match value.filter(|value| !value.is_null()) {
        None => return Ok((Vec::new(), Vec::new())),
        Some(side) => {
            let side = (side.members())
                .ok_or_else(|| format!("a histogram's {field} is an object of offset and bucketCounts"))?;
            let offset = match member(&side, "offset").filter(|offset| !offset.is_null()) {
                None => 0,
                Some(offset) => offset
                    .as_i64()
                    // OTLP's offset is 32 bits, which keeps each index within an i64.
                    .filter(|offset| i32::try_from(*offset).is_ok())
                    .ok_or_else(|| format!("a histogram's {field}.offset is a 32-bit integer"))?,
            };
            (
                offset,
                counts(member(&side, "bucketCounts"), &format!("{field}.bucketCounts"))?,
            )
        }
    };
    // A shift past 63 leaves each index's sign, as 63 does.
    let by = by.min(63);
    let first = offset >> by;
    let mut merged: Vec<f64> = Vec::new();
    for (i, count) in counts.into_iter().enumerate() {
        let at = (((offset + i as i64) >> by) - first) as usize;
        if merged.len() <= at {
            merged.resize(at + 1, 0.0);
        }
        merged[at] += count;
    }
    // OTLP's bucket i ends where Prometheus's bucket i + 1 does.
    spans(first + 1, &merged)
}

/// A sample of a classic histogram: its suffix to the row's name, its `le` label if a bucket's, and its value.
type ClassicSample = (&'static str, Option<String>, f64);

/// A histogram of explicit buckets as Prometheus's classic series, and its bounds.
fn classic(fields: &Fields) -> Result<(Vec<ClassicSample>, Vec<f64>), String> {
    cumulative(fields)?;
    let (count, sum) = count_and_sum(fields)?;
    let bounds = match fields.explicit_bounds.and_then(Json::items) {
        Some(bounds) => bounds
            .into_iter()
            .map(Json::as_f64)
            .collect::<Option<Vec<f64>>>()
            .ok_or("a histogram's explicitBounds are numbers; a NaN or an infinity arrives as none")?,
        _ => return Err("a histogram's bounds are an array in the field explicitBounds".into()),
    };
    if bounds.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("a histogram's explicitBounds increase, each past the one before".into());
    }
    let counts = counts(fields.bucket_counts, "bucketCounts")?;
    if counts.len() != bounds.len() + 1 {
        return Err(format!(
            "a histogram's bucketCounts are one more than its explicitBounds, and this one has {} for {}",
            counts.len(),
            bounds.len()
        ));
    }
    // Each bound in the shortest form that reads back as itself.
    let ends = (bounds.iter().map(f64::to_string)).chain(std::iter::once("+Inf".to_string()));
    let mut below = 0.0;
    let mut samples: Vec<_> = (ends.zip(counts))
        .map(|(end, count)| {
            below += count;
            ("_bucket", Some(end), below)
        })
        .collect();
    samples.push(("_sum", None, sum));
    samples.push(("_count", None, count));
    Ok((samples, bounds))
}

/// The spans over `counts`, bucket `start` the first, and the count of each bucket they cover. Prometheus leaves
/// out empty buckets at either end, and starts a new span past more than two in a row.
fn spans(start: i64, counts: &[f64]) -> Result<(Vec<BucketSpan>, Vec<f64>), String> {
    let mut spans: Vec<BucketSpan> = Vec::new();
    let mut covered = Vec::new();
    // The bucket past the last one covered.
    let mut end: Option<i64> = None;
    for (i, &count) in counts.iter().enumerate() {
        if count == 0.0 {
            continue;
        }
        let at = start + i as i64;
        match (end, spans.last_mut()) {
            (Some(end), Some(span)) if at - end <= 2 => {
                covered.extend((end..at).map(|_| 0.0));
                span.length += (at - end) as u32 + 1;
            }
            _ => {
                let offset = at - end.unwrap_or(0);
                spans.push(BucketSpan {
                    offset: i32::try_from(offset)
                        .map_err(|_| "a histogram's bucket lies past the range remote_write carries")?,
                    length: 1,
                });
            }
        }
        covered.push(count);
        end = Some(at + 1);
    }
    Ok((spans, covered))
}

/// `h` with integer counts, the buckets' as deltas from the one before, when every count of it is whole and
/// exact in a double; else `h` as it is.
fn integral(mut h: Histogram) -> Histogram {
    let (Some(HistogramCount::Float(count)), Some(HistogramZeroCount::Float(zero))) = (&h.count, &h.zero_count) else {
        return h;
    };
    let (count, zero) = (*count, *zero);
    let whole = |c: &f64| c.fract() == 0.0 && *c < INEXACT_FROM;
    if !([count, zero].iter().chain(&h.positive_counts).chain(&h.negative_counts)).all(whole) {
        return h;
    }
    let deltas = |counts: Vec<f64>| {
        let mut before = 0;
        (counts.into_iter())
            .map(|c| {
                let c = c as i64;
                let delta = c - before;
                before = c;
                delta
            })
            .collect()
    };
    h.count = Some(HistogramCount::Int(count as u64));
    h.zero_count = Some(HistogramZeroCount::Int(zero as u64));
    h.positive_deltas = deltas(std::mem::take(&mut h.positive_counts));
    h.negative_deltas = deltas(std::mem::take(&mut h.negative_counts));
    h
}

impl RemoteWrite {
    /// The series `items` send, as 1.0 has them: each series' points oldest first, one `TimeSeries` for each run of
    /// samples or of histograms among them, the series in the order they came. Each point carries its row's start
    /// time where 2.0 sends one.
    fn timeseries(&self, items: &[Item]) -> Vec<TimeSeries> {
        let start_of = |item: &Item| match self.version {
            RemoteWriteVersion::V1 => 0,
            RemoteWriteVersion::V2 => item.start,
        };
        // Each series' points, the series in the order they came.
        let mut order: Vec<&Labels> = Vec::new();
        let mut series: HashMap<&Labels, Vec<(i64, i64, &SeriesPoint)>> = HashMap::new();
        for item in items {
            for point in &item.points {
                series
                    .entry(&point.labels)
                    .or_insert_with(|| {
                        order.push(&point.labels);
                        Vec::new()
                    })
                    .push((item.time, start_of(item), point));
            }
        }
        let mut timeseries: Vec<TimeSeries> = Vec::new();
        for labels in order {
            let mut points = series.remove(labels).expect("each series in the order has its points");
            // Stable, so points at one time go in the order they came.
            points.sort_by_key(|(time, _, _)| *time);
            let is_sample = |point: &Point| matches!(point, Point::Sample(_));
            // Whether the last entry holds samples, none while it holds exemplars alone. Exemplars alone join any
            // entry.
            let mut kind: Option<bool> = None;
            let start = timeseries.len();
            for &(timestamp, started, p) in &points {
                let this = p.point.as_ref().map(is_sample);
                let joins = timeseries.len() > start
                    && match (kind, this) {
                        (Some(last), Some(this)) => last == this,
                        _ => true,
                    };
                if !joins {
                    kind = None;
                    timeseries.push(TimeSeries {
                        labels: (labels.iter())
                            .map(|(name, value)| Label {
                                name: name.clone(),
                                value: value.clone(),
                            })
                            .collect(),
                        samples: Vec::new(),
                        exemplars: Vec::new(),
                        histograms: Vec::new(),
                    });
                }
                kind = kind.or(this);
                let entry = timeseries.last_mut().expect("an entry was pushed for the first point");
                match &p.point {
                    None => {}
                    Some(Point::Sample(value)) => entry.samples.push(Sample {
                        value: *value,
                        timestamp,
                        start_timestamp: started,
                    }),
                    Some(Point::Histogram(h)) => entry.histograms.push(Histogram {
                        start_timestamp: started,
                        ..Histogram::clone(h)
                    }),
                }
                entry.exemplars.extend_from_slice(&p.exemplars);
            }
            for entry in &mut timeseries[start..] {
                // Prometheus keeps an exemplar at its series' newest time only when it comes no earlier in this order.
                entry.exemplars.sort_by(|a, b| {
                    (a.timestamp.cmp(&b.timestamp))
                        .then(a.value.total_cmp(&b.value))
                        .then_with(|| labels_hash(&a.labels).cmp(&labels_hash(&b.labels)))
                });
            }
        }
        timeseries
    }

    /// `timeseries` as a 2.0 request, its strings each once in its table, and each series carrying its family's
    /// metadata where this destination knows it, `items`' own the latest.
    fn v2(&self, items: &[Item], timeseries: Vec<TimeSeries>) -> RequestV2 {
        let mut families = self.families.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        // The request's own metadata goes whatever the remembered families' bounds evict.
        let mut own: HashMap<&str, &MetricMetadata> = HashMap::new();
        for metadata in items.iter().filter_map(|item| item.metadata.as_ref()) {
            families.remember(metadata);
            own.insert(&metadata.metric_family_name, metadata);
        }
        let known = |family: &str| own.get(family).copied().or_else(|| families.by_name.get(family));
        let mut symbols = Symbols::default();
        let timeseries = (timeseries.into_iter())
            .map(|series| {
                let metadata = (series.labels.iter())
                    .find(|l| l.name == NAME_LABEL)
                    .and_then(|name| family_of(known, &name.value))
                    .map(|m| MetadataV2 {
                        r#type: m.r#type,
                        help_ref: symbols.of(&m.help),
                        unit_ref: symbols.of(&m.unit),
                    });
                TimeSeriesV2 {
                    labels_refs: symbols.labels(&series.labels),
                    samples: series.samples,
                    histograms: series.histograms,
                    exemplars: (series.exemplars.into_iter())
                        .map(|e| ExemplarV2 {
                            labels_refs: symbols.labels(&e.labels),
                            value: e.value,
                            timestamp: e.timestamp,
                        })
                        .collect(),
                    metadata,
                }
            })
            .collect();
        RequestV2 {
            symbols: symbols.table,
            timeseries,
        }
    }
}

/// Counts what a 2.0 store's answer says it wrote short of what `items` sent, kind by kind. A store writes a request
/// in part when it refuses some of it without refusing the request, and does not say which, so the rest is not sent
/// again: that would write twice what it wrote.
/// Whether a 2.0 store's answer says it wrote any sample, histogram or exemplar of the request.
fn wrote_any(response: &Response) -> bool {
    WRITTEN
        .iter()
        .any(|h| response.header(h).and_then(|v| v.trim().parse::<u64>().ok()) > Some(0))
}

/// `missing` stands for a count the store leaves out: `None` where that says nothing of the kind, as on a success, and
/// `Some(0)` on a refusal, which wrote none of a kind it does not count.
fn note_unwritten(unwritten: &Unwritten, response: &Response, items: &[Item], missing: Option<u64>) {
    let (mut samples, mut histograms, mut exemplars) = (0u64, 0u64, 0u64);
    for point in items.iter().flat_map(|item| &item.points) {
        match point.point {
            Some(Point::Sample(_)) => samples += 1,
            Some(Point::Histogram(_)) => histograms += 1,
            None => {}
        }
        exemplars += point.exemplars.len() as u64;
    }
    for (header, kind, sent) in [
        (WRITTEN[0], "sample", samples),
        (WRITTEN[1], "histogram", histograms),
        (WRITTEN[2], "exemplar", exemplars),
    ] {
        let Some(written) = response
            .header(header)
            .and_then(|v| v.trim().parse::<u64>().ok())
            .or(missing)
        else {
            continue;
        };
        if written < sent {
            let why = format!("the store wrote {written} of {sent} {kind}(s) of a request");
            unwritten.note(kind, sent - written, &why);
        }
    }
}

/// The metadata of the family a series named `name` is of: the family of that name, or a classic histogram's or
/// summary's whose series it is by its suffix.
fn family_of<'a>(known: impl Fn(&str) -> Option<&'a MetricMetadata>, name: &str) -> Option<&'a MetricMetadata> {
    if let Some(m) = known(name) {
        return Some(m);
    }
    let suffixed = |kinds: &[MetricType], suffixes: &[&str]| {
        (suffixes.iter())
            .filter_map(|suffix| name.strip_suffix(suffix))
            .filter_map(&known)
            .find(|m| kinds.iter().any(|k| *k as i32 == m.r#type))
    };
    // This destination sends a gauge histogram's classic series as `_sum` and `_count`, as OpenMetrics' `_gsum` and
    // `_gcount` come from a scrape.
    let histogram = [MetricType::Histogram, MetricType::GaugeHistogram];
    suffixed(
        &[MetricType::Histogram, MetricType::GaugeHistogram, MetricType::Summary],
        &["_sum", "_count"],
    )
    .or_else(|| suffixed(&histogram, &["_bucket"]))
    .or_else(|| suffixed(&[MetricType::GaugeHistogram], &["_gsum", "_gcount"]))
}

/// A 2.0 request's table of strings, the empty string first, as 2.0 has it.
struct Symbols {
    table: Vec<String>,
    at: HashMap<String, u32>,
}

impl Default for Symbols {
    fn default() -> Symbols {
        Symbols {
            table: vec![String::new()],
            at: HashMap::from([(String::new(), 0)]),
        }
    }
}

impl Symbols {
    /// Where `s` is in the table, added the first time.
    fn of(&mut self, s: &str) -> u32 {
        if let Some(&at) = self.at.get(s) {
            return at;
        }
        let at = u32::try_from(self.table.len()).expect("a request holds far fewer than 2^32 strings");
        self.table.push(s.to_string());
        self.at.insert(s.to_string(), at);
        at
    }

    /// `labels` as references, each its name's and its value's.
    fn labels(&mut self, labels: &[Label]) -> Vec<u32> {
        (labels.iter())
            .flat_map(|l| [self.of(&l.name), self.of(&l.value)])
            .collect()
    }
}

impl Protocol for RemoteWrite {
    type Item = Item;

    fn name(&self) -> &'static str {
        "prometheus"
    }

    fn path(&self) -> &str {
        &self.path
    }

    /// A store refuses a point older than its series' newest, so a batch goes oldest first across its requests, its
    /// metadata alone before all.
    fn order(&self) -> Option<Order<Item>> {
        Some(|a, b| a.time.cmp(&b.time))
    }

    /// `name` a non-empty string, `time` a datetime, and `attributes` absent, null, or an object whose values are
    /// strings, numbers or booleans. An attribute that is null or the empty string is no label, as Prometheus reads a
    /// label with no value. JSON holds no NaN or infinity, so a number that was one arrives as none.
    ///
    /// A sample has `value` a number. A histogram has `count` and `sum` numbers, and `aggregationTemporality` 2 for a
    /// cumulative histogram or none for a gauge histogram.
    ///
    /// An exponential histogram has `scale` an integer from -4 up, `positive` and `negative` each none or
    /// `{offset, bucketCounts}`, `zeroCount` and `zeroThreshold` numbers or none for 0, and `resetHint` `"yes"` or
    /// `"no"` on a cumulative one, or none. Its counts are sent as integers when every one is whole, else as floats.
    ///
    /// A `histogram` has `explicitBounds` increasing and one more `bucketCounts`, and no `le` attribute, which its
    /// `_bucket` series uses.
    ///
    /// `exemplars` is absent, null, or an array of objects, each with `value` a number, `time` a datetime or none for
    /// the row's, `traceId` and `spanId` strings, and `filteredAttributes` an object of labels whose values are
    /// strings, numbers or booleans. What is not is left out, as [`exemplars`] says, and never refuses the row. A row
    /// of no `type`, `gauge` or `sum` with `value` none and some exemplars sends those alone.
    ///
    /// A row of `time`, `value` and `exemplars` all none or empty is its family's metadata alone, and sends no series.
    /// Its `type` is none or one the module's table names. A point's row whose `description` or `unit` is a non-empty
    /// string carries its family's metadata too. Each of `description` and `unit` is none or a string; one that is
    /// no string reads as none.
    ///
    /// `startTime` is a datetime, or none; one that is no datetime reads as none.
    ///
    /// Other fields are not read.
    fn admit(&self, row: &OwnedRecord) -> Result<Item, String> {
        let fields = Fields::read(&row.value)
            .ok_or("a Prometheus destination sends a record, and this row is no JSON object")?;
        let name = match fields.name.and_then(Json::as_str) {
            Some(name) if !name.is_empty() => name.into_owned(),
            _ => return Err("a row's name is a non-empty string in the field name".into()),
        };
        let exemplar_items = fields.exemplars.and_then(Json::items).unwrap_or_default();
        // Only a row of no time is metadata: one with a time and nothing at it is a point missing its value.
        if fields.time.is_none() && fields.value.is_none() && exemplar_items.is_empty() {
            return Ok(Item {
                points: Vec::new(),
                time: METADATA_TIME,
                start: 0,
                metadata: Some(metadata(&fields, &name)?),
            });
        }
        enum Shape {
            Sample(f64),
            /// Exemplars and no sample.
            Alone,
            Histogram(Box<Histogram>),
            Classic(Vec<ClassicSample>, Vec<f64>),
        }
        let datetime = |field: Option<Json<'_>>| {
            let text = field?.as_str()?;
            crate::ops::timeparse::parse_auto(&text).map(|us| us.div_euclid(1000))
        };
        let time =
            datetime(fields.time).ok_or("a row's time is a datetime from the year 0 to 9999 in the field time")?;
        let start = datetime(fields.start_time).unwrap_or(0);
        let exemplars = exemplars(exemplar_items, time);
        // A Prometheus receiver gives a series' exemplars with no sample as a row of no value.
        let exemplars_alone = fields.value.is_none() && !exemplars.is_empty();
        let sample_shape = |fields: &Fields| {
            if exemplars_alone {
                Ok(Shape::Alone)
            } else {
                sample(fields).map(Shape::Sample)
            }
        };
        let shape = match fields.kind.map(Json::as_str) {
            None => sample_shape(&fields)?,
            Some(Some(kind)) => match &*kind {
                "gauge" | "sum" => sample_shape(&fields)?,
                "exponentialHistogram" => Shape::Histogram(Box::new(exponential(&fields)?)),
                "histogram" => {
                    let (samples, bounds) = classic(&fields)?;
                    Shape::Classic(samples, bounds)
                }
                "summary" => {
                    return Err(
                        "a summary goes to Prometheus as its quantile, _sum and _count series, each a sample".into(),
                    );
                }
                kind => {
                    return Err(format!(
                        "a row's type is gauge, sum, histogram or exponentialHistogram, and {kind:?} is none of them"
                    ));
                }
            },
            Some(None) => return Err("a row's type is a string in the field type".into()),
        };
        let metadata = if text(fields.description).is_empty() && text(fields.unit).is_empty() {
            None
        } else {
            Some(metadata(&fields, &name)?)
        };
        let mut labels = vec![(NAME_LABEL.to_string(), name.clone())];
        if let Some(attributes) = fields.attributes {
            let attributes = attributes
                .members()
                .ok_or("a row's attributes are an object of its labels")?;
            for (key, value) in attributes {
                if key.is_empty() {
                    return Err("an attribute's name is empty, and a label's name is not".into());
                }
                if key == NAME_LABEL {
                    return Err("the attribute __name__ would name the series, which the field name does".into());
                }
                if let Some(value) = label_value(value, || format!("the attribute {key:?}"))? {
                    labels.push((key.into_owned(), value));
                }
            }
        }
        let one = |mut labels: Labels, point, exemplars| {
            labels.sort_unstable();
            vec![SeriesPoint {
                labels,
                point,
                exemplars,
            }]
        };
        let points = match shape {
            Shape::Sample(value) => one(labels, Some(Point::Sample(value)), exemplars),
            Shape::Alone => one(labels, None, exemplars),
            Shape::Histogram(mut h) => {
                h.timestamp = time;
                one(labels, Some(Point::Histogram(h)), exemplars)
            }
            Shape::Classic(samples, bounds) => {
                if labels.iter().any(|(key, _)| key == BUCKET_LABEL) {
                    return Err(
                        "a histogram's attribute le would clash with the label le its _bucket series ends each \
                         bucket with"
                            .into(),
                    );
                }
                let mut points: Vec<SeriesPoint> = (samples.into_iter())
                    .map(|(suffix, end, value)| {
                        let mut labels = labels.clone();
                        labels[0].1 = format!("{name}{suffix}");
                        labels.extend(end.map(|end| (BUCKET_LABEL.to_string(), end)));
                        labels.sort_unstable();
                        SeriesPoint {
                            labels,
                            point: Some(Point::Sample(value)),
                            exemplars: Vec::new(),
                        }
                    })
                    .collect();
                // The buckets lead the samples, the first ending at the first bound and the last at +Inf.
                for exemplar in exemplars {
                    let bucket = bounds.partition_point(|&bound| bound < exemplar.value);
                    points[bucket].exemplars.push(exemplar);
                }
                points
            }
        };
        Ok(Item {
            points,
            time,
            start,
            metadata,
        })
    }

    /// Each point's labels' bytes with each one's framing, the point's: a sample's, or a histogram's fixed fields
    /// and each of its buckets and spans at most, and each exemplar's fixed fields and labels. Its metadata's strings'
    /// bytes with their framing and its type's, though a request sends its family's once.
    fn size(&self, item: &Item) -> usize {
        let label = |name: &str, value: &str| name.len() + value.len() + 6;
        let metadata =
            (item.metadata.as_ref()).map_or(0, |m| m.metric_family_name.len() + m.help.len() + m.unit.len() + 10);
        let points: usize = (item.points.iter())
            .map(|p| {
                let point = match &p.point {
                    None => 0,
                    Some(Point::Sample(_)) => 24,
                    Some(Point::Histogram(h)) => {
                        let buckets = h.positive_deltas.len()
                            + h.positive_counts.len()
                            + h.negative_deltas.len()
                            + h.negative_counts.len();
                        let spans = h.positive_spans.len() + h.negative_spans.len();
                        64 + 9 * (buckets + spans)
                    }
                };
                let exemplars: usize = (p.exemplars.iter())
                    .map(|e| 24 + e.labels.iter().map(|l| label(&l.name, &l.value)).sum::<usize>())
                    .sum();
                p.labels.iter().map(|(n, v)| label(n, v)).sum::<usize>() + point + exemplars
            })
            .sum();
        metadata + points
    }

    fn body(&self, items: &[Item]) -> Vec<u8> {
        let timeseries = self.timeseries(items);
        let request = match self.version {
            RemoteWriteVersion::V1 => {
                // Each family's metadata, the first of the batch's order.
                let mut families: HashSet<&str> = HashSet::new();
                let metadata = (items.iter())
                    .filter_map(|item| item.metadata.as_ref())
                    .filter(|m| families.insert(&m.metric_family_name))
                    .cloned()
                    .collect();
                WriteRequest { timeseries, metadata }.encode_to_vec()
            }
            RemoteWriteVersion::V2 => self.v2(items, timeseries).encode_to_vec(),
        };
        snap::raw::Encoder::new()
            .compress_vec(&request)
            .expect("a request is far under snappy's 4 GiB input limit")
    }

    fn read(&self, response: &Response, items: &[Self::Item]) -> Result<Outcome, IoError> {
        let count = items.len();
        match response.status {
            // A 2.0 receiver names what it wrote, if only none. A 1.0 one that reads the request as 1.0's finds nothing
            // in it and says nothing; Prometheus' own sender takes a 2xx that wrote nothing as a failure too.
            200..=299
                if self.version == RemoteWriteVersion::V2 && !WRITTEN.iter().any(|h| response.header(h).is_some()) =>
            {
                Err(IoError::transient(format!(
                    "the store took a remote_write 2.0 request without saying what it wrote, as a store of only 1.0 \
                     does; set this destination's version to 1.0: {}",
                    response.describe()
                )))
            }
            200..=299 => {
                if let Some(unwritten) = &self.unwritten {
                    note_unwritten(unwritten, response, items, None);
                }
                Ok(Outcome::default())
            }
            // Prometheus sends these again itself, backing off.
            429 | 500..=599 => Ok(Outcome {
                retryable: (0..count).collect(),
                throttled: true,
                ..Outcome::default()
            }),
            // A 2.0 store that wrote part of a request holds each series' newest point of it, so any of it sent again is
            // older than that point, and a store without an out-of-order window refuses it.
            400 | 409 | 413 | 422 if self.version == RemoteWriteVersion::V2 && wrote_any(response) => {
                // Only this line carries the store's refusal: the rows it did not write are dropped, not sent again.
                tracing::error!(
                    target: "remote_write.partly_written",
                    subject = self.unwritten.as_ref().map_or("prometheus", |u| u.subject()),
                    status = response.status,
                    error = %response.describe(),
                    "the store wrote part of a request and refused the rest, which is dropped: sent again, it would be older than what the store now holds"
                );
                if let Some(unwritten) = &self.unwritten {
                    note_unwritten(unwritten, response, items, Some(0));
                }
                Ok(Outcome::default())
            }
            // A store refuses a request without naming the sample, out of order or malformed: the halves find it. Thanos
            // Receive says 409 for an out-of-order or duplicate sample, and some stores 422 for a malformed one.
            400 | 409 | 413 | 422 => Ok(Outcome {
                split: Some(response.describe()),
                ..Outcome::default()
            }),
            415 if self.version == RemoteWriteVersion::V2 => Err(IoError::transient(format!(
                "the store does not take remote_write 2.0; set this destination's version to 1.0: {}",
                response.describe()
            ))),
            401 | 403 => Err(IoError::transient(format!(
                "the store refused this destination's credentials: {}",
                response.describe()
            ))),
            _ => Err(IoError::transient(format!(
                "the store answered {}",
                response.describe()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::Payload;

    fn protocol() -> RemoteWrite {
        protocol_of(RemoteWriteVersion::V1)
    }

    fn protocol_of(version: RemoteWriteVersion) -> RemoteWrite {
        RemoteWrite {
            path: String::new(),
            version,
            families: Arc::default(),
            unwritten: None,
        }
    }

    fn row(json: &str) -> OwnedRecord {
        OwnedRecord {
            value: json.as_bytes().to_vec(),
            payload: Payload::Record,
            ..OwnedRecord::default()
        }
    }

    fn admit(json: &str) -> Result<Item, String> {
        protocol().admit(&row(json))
    }

    /// `n` rows of one sample each.
    fn samples(n: usize) -> Vec<Item> {
        (0..n)
            .map(|i| admit(&format!(r#"{{{T},"name":"s{i}","value":1}}"#)).unwrap())
            .collect()
    }

    fn sent(items: &[Item]) -> WriteRequest {
        let body = protocol().body(items);
        let raw = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
        WriteRequest::decode(raw.as_slice()).unwrap()
    }

    fn labels(series: &TimeSeries) -> Vec<(&str, &str)> {
        series
            .labels
            .iter()
            .map(|l| (l.name.as_str(), l.value.as_str()))
            .collect()
    }

    #[test]
    fn a_row_is_a_sample_of_its_series_labels_sorted() {
        let item = admit(
            r#"{"time":"2026-10-11T18:00:00.123000Z","name":"up","value":1,"attributes":{"job":"api","instance":"a:9100"}}"#,
        )
        .unwrap();
        assert_eq!(
            item,
            Item {
                points: vec![SeriesPoint {
                    labels: vec![
                        ("__name__".into(), "up".into()),
                        ("instance".into(), "a:9100".into()),
                        ("job".into(), "api".into()),
                    ],
                    point: Some(Point::Sample(1.0)),
                    exemplars: Vec::new(),
                }],
                time: 1_791_741_600_123,
                start: 0,
                metadata: None,
            }
        );
    }

    #[test]
    fn rows_of_one_series_are_one_series_oldest_first() {
        let a = |t: &str, v: f64| {
            admit(&format!(
                r#"{{"time":"{t}","name":"x","value":{v:?},"attributes":{{"a":"1"}}}}"#
            ))
            .unwrap()
        };
        let b = admit(r#"{"time":"2026-10-11T18:00:00.000000Z","name":"y","value":2.5}"#).unwrap();
        let request = sent(&[
            a("2026-10-11T18:00:02.000000Z", 3.0),
            b,
            a("2026-10-11T18:00:01.000000Z", 2.0),
        ]);
        assert_eq!(request.timeseries.len(), 2);
        assert_eq!(labels(&request.timeseries[0]), [("__name__", "x"), ("a", "1")]);
        let times: Vec<i64> = request.timeseries[0].samples.iter().map(|s| s.timestamp).collect();
        assert_eq!(times, [1_791_741_601_000, 1_791_741_602_000]);
        assert_eq!(labels(&request.timeseries[1]), [("__name__", "y")]);
        assert_eq!(request.timeseries[1].samples[0].value, 2.5);
    }

    #[test]
    fn labels_sort_by_their_bytes_and_a_time_reads_without_a_fraction() {
        let item = admit(r#"{"time":"2026-10-11T18:00:00Z","name":"x","value":1,"attributes":{"Job":"a","job":"b"}}"#)
            .unwrap();
        let names: Vec<&str> = item.points[0].labels.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["Job", "__name__", "job"]);
        assert_eq!(item.time, 1_791_741_600_000);
    }

    #[test]
    fn samples_of_one_series_at_one_time_go_in_the_order_they_came() {
        let at = |v: f64| {
            admit(&format!(
                r#"{{"time":"2026-10-11T18:00:00Z","name":"x","value":{v:?}}}"#
            ))
            .unwrap()
        };
        let request = sent(&[at(1.0), at(2.0)]);
        let values: Vec<f64> = request.timeseries[0].samples.iter().map(|s| s.value).collect();
        assert_eq!(values, [1.0, 2.0]);
    }

    #[test]
    fn an_attribute_with_no_value_is_no_label_and_a_scalar_is_its_text() {
        let item = admit(
            r#"{"time":"2026-10-11T18:00:00.000000Z","name":"x","value":1,"attributes":{"a":"","b":null,"c":2,"d":true}}"#,
        )
        .unwrap();
        let names: Vec<(&str, &str)> = item.points[0]
            .labels
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect();
        assert_eq!(names, [("__name__", "x"), ("c", "2"), ("d", "true")]);
    }

    #[test]
    fn an_attribute_given_twice_holds_its_last_value() {
        let item = admit(
            r#"{"time":"2026-10-11T18:00:00Z","name":"x","value":1,"attributes":{"a":"1","b":"2","a":"3","b":null}}"#,
        )
        .unwrap();
        let names: Vec<(&str, &str)> = item.points[0]
            .labels
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect();
        assert_eq!(names, [("__name__", "x"), ("a", "3")]);
    }

    #[test]
    fn a_row_naming_a_field_twice_or_running_on_past_its_object_is_no_json_object() {
        for json in [
            r#"{"time":"2026-10-11T18:00:00Z","name":"x","value":1,"value":2}"#,
            r#"{"time":"2026-10-11T18:00:00Z","name":"x","value":null,"value":2}"#,
            r#"{"time":"2026-10-11T18:00:00Z","name":"x","value":1} 2"#,
            r#"[1]"#,
        ] {
            let refused = admit(json).err().unwrap_or_default();
            assert!(refused.contains("no JSON object"), "{json}: {refused}");
        }
    }

    #[test]
    fn a_row_that_is_no_sample_is_refused_naming_why() {
        let t = r#""time":"2026-10-11T18:00:00.000000Z""#;
        for (json, why) in [
            (format!(r#"{{{t},"value":1}}"#), "name"),
            (format!(r#"{{{t},"name":"","value":1}}"#), "name"),
            (format!(r#"{{{t},"name":"x"}}"#), "NaN"),
            (format!(r#"{{{t},"name":"x","value":"1"}}"#), "value"),
            (r#"{"name":"x","value":1,"time":17}"#.to_string(), "time"),
            (
                format!(r#"{{{t},"name":"x","value":1,"attributes":{{"a":[1]}}}}"#),
                "\"a\"",
            ),
            (
                format!(r#"{{{t},"name":"x","value":1,"attributes":{{"__name__":"y"}}}}"#),
                "__name__",
            ),
            (format!(r#"{{{t},"name":"x","value":1,"attributes":"a"}}"#), "object"),
            (
                format!(r#"{{{t},"name":"x","value":1,"attributes":{{"__name__":null}}}}"#),
                "__name__",
            ),
            (
                format!(r#"{{{t},"name":"x","value":1,"attributes":{{"":null}}}}"#),
                "empty",
            ),
            ("not json".to_string(), "JSON"),
        ] {
            let refused = admit(&json).unwrap_err();
            assert!(refused.contains(why), "{json}: {refused}");
        }
    }

    const T: &str = r#""time":"2026-10-11T18:00:00Z""#;

    fn histogram(json: &str) -> Histogram {
        match admit(json).unwrap().points.remove(0).point {
            Some(Point::Histogram(h)) => *h,
            _ => panic!("{json} is no histogram"),
        }
    }

    /// A series' labels and its samples' values and times.
    type Series<'a> = (Vec<(&'a str, &'a str)>, Vec<(f64, i64)>);

    /// Each entry's labels and samples, as sent.
    fn series(request: &WriteRequest) -> Vec<Series<'_>> {
        (request.timeseries.iter())
            .map(|s| (labels(s), s.samples.iter().map(|s| (s.value, s.timestamp)).collect()))
            .collect()
    }

    fn span(offset: i32, length: u32) -> BucketSpan {
        BucketSpan { offset, length }
    }

    #[test]
    fn an_exponential_histogram_with_whole_counts_is_sent_as_integers() {
        let h = histogram(&format!(
            r#"{{{T},"name":"latency","type":"exponentialHistogram","aggregationTemporality":2,"count":10,"sum":20.5,
            "scale":0,"zeroCount":1,"zeroThreshold":0.001,"positive":{{"offset":0,"bucketCounts":[2,0,0,0,3,1]}},
            "negative":{{"offset":-2,"bucketCounts":[0,3]}}}}"#
        ));
        assert_eq!(
            h,
            Histogram {
                count: Some(HistogramCount::Int(10)),
                sum: 20.5,
                schema: 0,
                zero_threshold: 0.001,
                zero_count: Some(HistogramZeroCount::Int(1)),
                // OTLP's buckets 0 and 4..=5 are Prometheus's 1 and 5..=6; three empty ones between start a span.
                positive_spans: vec![span(1, 1), span(3, 2)],
                positive_deltas: vec![2, 1, -2],
                negative_spans: vec![span(0, 1)],
                negative_deltas: vec![3],
                reset_hint: ResetHint::Unknown as i32,
                timestamp: 1_791_741_600_000,
                ..Histogram::default()
            }
        );
    }

    #[test]
    fn two_empty_buckets_inside_a_span_are_sent_as_zeros() {
        let h = histogram(&format!(
            r#"{{{T},"name":"x","type":"exponentialHistogram","aggregationTemporality":2,"count":2,"sum":1,"scale":3,
            "positive":{{"offset":4,"bucketCounts":[0,1,0,0,1,0]}}}}"#
        ));
        assert_eq!(h.positive_spans, [span(6, 4)]);
        assert_eq!(h.positive_deltas, [1, -1, 0, 1]);
        assert!(h.negative_spans.is_empty());
        assert_eq!(h.zero_count, Some(HistogramZeroCount::Int(0)));
    }

    #[test]
    fn a_histogram_with_a_fractional_count_is_sent_as_floats() {
        let h = histogram(&format!(
            r#"{{{T},"name":"x","type":"exponentialHistogram","aggregationTemporality":2,"count":3.5,"sum":1,"scale":1,
            "positive":{{"offset":0,"bucketCounts":[1.5,2]}}}}"#
        ));
        assert_eq!(h.count, Some(HistogramCount::Float(3.5)));
        assert_eq!(h.zero_count, Some(HistogramZeroCount::Float(0.0)));
        assert_eq!(h.positive_spans, [span(1, 2)]);
        assert_eq!(h.positive_counts, [1.5, 2.0]);
        assert!(h.positive_deltas.is_empty());
    }

    #[test]
    fn a_histogram_with_no_temporality_is_a_gauge_histogram() {
        let h = histogram(&format!(
            r#"{{{T},"name":"x","type":"exponentialHistogram","count":0,"sum":0,"scale":0}}"#
        ));
        assert_eq!(h.reset_hint, ResetHint::Gauge as i32);
    }

    #[test]
    fn a_finer_histogram_is_downscaled_to_schema_8() {
        let h = histogram(&format!(
            r#"{{{T},"name":"x","type":"exponentialHistogram","aggregationTemporality":2,"count":8,"sum":1,"scale":10,
            "positive":{{"offset":3,"bucketCounts":[1,1,1,1,1,1]}},"negative":{{"offset":-3,"bucketCounts":[1,1]}}}}"#
        ));
        assert_eq!(h.schema, 8);
        // OTLP's buckets 3..=8 at scale 10 are 0, 1 (four of them) and 2 at scale 8, Prometheus's 1..=3.
        assert_eq!(h.positive_spans, [span(1, 3)]);
        assert_eq!(h.positive_deltas, [1, 3, -3]);
        // -3 and -2 are both -1, Prometheus's 0.
        assert_eq!(h.negative_spans, [span(0, 1)]);
        assert_eq!(h.negative_deltas, [2]);
    }

    #[test]
    fn a_histogram_of_explicit_buckets_is_sent_as_classic_series() {
        let request = sent(&[admit(&format!(
            r#"{{{T},"name":"x","type":"histogram","aggregationTemporality":2,"count":3,"sum":4.5,
            "attributes":{{"job":"a"}},"explicitBounds":[0.1,1,2.5],"bucketCounts":[0,2,0,1]}}"#
        ))
        .unwrap()]);
        let t = 1_791_741_600_000;
        assert_eq!(
            series(&request),
            [
                (
                    vec![("__name__", "x_bucket"), ("job", "a"), ("le", "0.1")],
                    vec![(0.0, t)]
                ),
                (
                    vec![("__name__", "x_bucket"), ("job", "a"), ("le", "1")],
                    vec![(2.0, t)]
                ),
                (
                    vec![("__name__", "x_bucket"), ("job", "a"), ("le", "2.5")],
                    vec![(2.0, t)]
                ),
                (
                    vec![("__name__", "x_bucket"), ("job", "a"), ("le", "+Inf")],
                    vec![(3.0, t)]
                ),
                (vec![("__name__", "x_sum"), ("job", "a")], vec![(4.5, t)]),
                (vec![("__name__", "x_count"), ("job", "a")], vec![(3.0, t)]),
            ]
        );
        assert!(request.timeseries.iter().all(|s| s.histograms.is_empty()));
    }

    #[test]
    fn an_otlp_explicit_histogram_point_is_sent_as_classic_series() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","description":"d","unit":"s","type":"histogram","metadata":null,
            "aggregationTemporality":null,"isMonotonic":null,"startTime":"2026-10-11T17:00:00Z","attributes":{{}},
            "flags":0,"value":null,"count":2.5,"sum":2,"min":0.5,"max":2,"bucketCounts":[1.5,1],
            "explicitBounds":[1],"scale":null,"zeroCount":null,"zeroThreshold":null,"positive":null,"negative":null,
            "quantileValues":null,"exemplars":[]}}"#
        ))
        .unwrap();
        let t = 1_791_741_600_000;
        assert_eq!(
            series(&sent(&[item])),
            [
                (vec![("__name__", "x_bucket"), ("le", "1")], vec![(1.5, t)]),
                (vec![("__name__", "x_bucket"), ("le", "+Inf")], vec![(2.5, t)]),
                (vec![("__name__", "x_sum")], vec![(2.0, t)]),
                (vec![("__name__", "x_count")], vec![(2.5, t)]),
            ]
        );
    }

    #[test]
    fn rows_of_one_classic_histogram_are_its_series_oldest_first() {
        let at = |t: &str, n: u32| {
            admit(&format!(
                r#"{{"time":"{t}","name":"x","type":"histogram","aggregationTemporality":2,"count":{n},"sum":1,
                "explicitBounds":[],"bucketCounts":[{n}]}}"#
            ))
            .unwrap()
        };
        let request = sent(&[at("2026-10-11T18:00:01Z", 2), at("2026-10-11T18:00:00Z", 1)]);
        let (t0, t1) = (1_791_741_600_000, 1_791_741_601_000);
        assert_eq!(
            series(&request),
            [
                (
                    vec![("__name__", "x_bucket"), ("le", "+Inf")],
                    vec![(1.0, t0), (2.0, t1)]
                ),
                (vec![("__name__", "x_sum")], vec![(1.0, t0), (1.0, t1)]),
                (vec![("__name__", "x_count")], vec![(1.0, t0), (2.0, t1)]),
            ]
        );
    }

    #[test]
    fn a_reset_hint_is_restored_from_the_row() {
        let hint = |rest: &str| {
            histogram(&format!(
                r#"{{{T},"name":"x","type":"exponentialHistogram","count":0,"sum":0,"scale":0{rest}}}"#
            ))
            .reset_hint
        };
        assert_eq!(
            hint(r#","aggregationTemporality":2,"resetHint":"yes""#),
            ResetHint::Yes as i32
        );
        assert_eq!(
            hint(r#","aggregationTemporality":2,"resetHint":"no""#),
            ResetHint::No as i32
        );
        assert_eq!(
            hint(r#","aggregationTemporality":2,"resetHint":null"#),
            ResetHint::Unknown as i32
        );
        assert_eq!(hint(r#","resetHint":null"#), ResetHint::Gauge as i32);
    }

    #[test]
    fn an_otlp_point_with_fields_beyond_the_record_is_sent() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","description":"d","unit":"s","type":"exponentialHistogram","metadata":null,
            "aggregationTemporality":2,"isMonotonic":null,"startTime":"2026-10-11T17:00:00Z","attributes":{{"a":"1"}},
            "flags":0,"value":null,"count":1,"sum":2,"min":2,"max":2,"bucketCounts":null,"explicitBounds":null,
            "scale":2,"zeroCount":0,"zeroThreshold":0,"positive":{{"offset":1,"bucketCounts":[1]}},"negative":null,
            "quantileValues":null,"exemplars":[]}}"#
        ))
        .unwrap();
        let SeriesPoint { labels, point, .. } = item.points.into_iter().next().unwrap();
        assert_eq!(labels, [("__name__".into(), "x".into()), ("a".into(), "1".into())]);
        let Some(Point::Histogram(h)) = point else {
            panic!("no histogram")
        };
        assert_eq!((h.positive_spans, h.positive_deltas), (vec![span(2, 1)], vec![1]));
    }

    /// Each entry's samples' and histograms' times, as sent.
    fn entries(request: &WriteRequest) -> Vec<(Vec<i64>, Vec<i64>)> {
        (request.timeseries.iter())
            .map(|s| {
                (
                    s.samples.iter().map(|s| s.timestamp).collect(),
                    s.histograms.iter().map(|h| h.timestamp).collect(),
                )
            })
            .collect()
    }

    fn exponential_at(t: &str) -> Item {
        admit(&format!(
            r#"{{"time":"{t}","name":"x","type":"exponentialHistogram","aggregationTemporality":2,"count":1,"sum":1,"scale":0,"zeroCount":1}}"#
        ))
        .unwrap()
    }

    fn gauge_at(t: &str) -> Item {
        admit(&format!(r#"{{"time":"{t}","name":"x","type":"gauge","value":1}}"#)).unwrap()
    }

    #[test]
    fn an_older_histogram_goes_before_a_newer_sample_of_its_series() {
        let request = sent(&[gauge_at("2026-10-11T18:00:01Z"), exponential_at("2026-10-11T18:00:00Z")]);
        assert_eq!(
            entries(&request),
            [(vec![], vec![1_791_741_600_000]), (vec![1_791_741_601_000], vec![])]
        );
        assert!(request.timeseries.iter().all(|s| labels(s) == [("__name__", "x")]));
    }

    #[test]
    fn a_series_of_both_kinds_goes_as_an_entry_for_each_run_oldest_first() {
        let request = sent(&[
            exponential_at("2026-10-11T18:00:02Z"),
            gauge_at("2026-10-11T18:00:04Z"),
            exponential_at("2026-10-11T18:00:01Z"),
            gauge_at("2026-10-11T18:00:00Z"),
            gauge_at("2026-10-11T18:00:03Z"),
        ]);
        assert_eq!(
            entries(&request),
            [
                (vec![1_791_741_600_000], vec![]),
                (vec![], vec![1_791_741_601_000, 1_791_741_602_000]),
                (vec![1_791_741_603_000, 1_791_741_604_000], vec![]),
            ]
        );
    }

    #[test]
    fn a_histogram_is_sized_by_its_buckets() {
        let small = admit(&format!(
            r#"{{{T},"name":"x","type":"exponentialHistogram","count":0,"sum":0,"scale":0}}"#
        ))
        .unwrap();
        let large = admit(&format!(
            r#"{{{T},"name":"x","type":"exponentialHistogram","count":3,"sum":0,"scale":0,
            "positive":{{"offset":0,"bucketCounts":[1,0,0,0,1,0,0,0,1]}}}}"#
        ))
        .unwrap();
        assert_eq!(protocol().size(&large) - protocol().size(&small), 9 * 6);
    }

    #[test]
    fn a_row_that_is_no_histogram_is_refused_naming_why() {
        // The fields of `rest`, a list after a comma, over those of `base`.
        let with = |base: String, rest: &str| {
            let mut row: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&base).unwrap();
            let rest: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(&format!("{{{}}}", &rest[1..])).unwrap();
            row.extend(rest);
            serde_json::Value::Object(row).to_string()
        };
        let e = |rest: &str| {
            let base = format!(
                r#"{{{T},"name":"x","type":"exponentialHistogram","aggregationTemporality":2,"count":1,"sum":1,"scale":0}}"#
            );
            with(base, rest)
        };
        let c = |rest: &str| {
            with(
                format!(r#"{{{T},"name":"x","type":"histogram","count":1,"sum":1}}"#),
                rest,
            )
        };
        for (json, why) in [
            (
                format!(r#"{{{T},"name":"x","type":"summary","count":1,"sum":1}}"#),
                "quantile, _sum and _count",
            ),
            (format!(r#"{{{T},"name":"x","type":"set","value":1}}"#), "\"set\""),
            (format!(r#"{{{T},"name":"x","type":1,"value":1}}"#), "type"),
            (e(r#","aggregationTemporality":1"#), "delta"),
            (e(r#","aggregationTemporality":0"#), "aggregationTemporality"),
            (e(r#","sum":null"#), "NaN"),
            (e(r#","count":null"#), "count"),
            (e(r#","count":-1"#), "negative"),
            (e(r#","scale":-5"#), "-5"),
            (e(r#","scale":null"#), "scale"),
            (
                e(r#","positive":{"offset":0,"bucketCounts":[1,null]}"#),
                "positive.bucketCounts",
            ),
            (e(r#","negative":{"offset":0,"bucketCounts":[-1]}"#), "negative"),
            (e(r#","positive":[1]"#), "offset and bucketCounts"),
            (e(r#","positive":{"offset":4294967296,"bucketCounts":[1]}"#), "32-bit"),
            (e(r#","positive":{"offset":2147483647,"bucketCounts":[1]}"#), "range"),
            (c(r#","bucketCounts":[1]"#), "explicitBounds"),
            (
                c(r#","explicitBounds":[1],"bucketCounts":[0,1],"attributes":{"le":"1"}"#),
                "attribute le",
            ),
            (
                c(r#","aggregationTemporality":1,"explicitBounds":[1],"bucketCounts":[0,1]"#),
                "delta",
            ),
            (c(r#","explicitBounds":[1],"bucketCounts":[0,1],"sum":null"#), "NaN"),
            (c(r#","explicitBounds":[1],"bucketCounts":[0,1],"count":null"#), "count"),
            (e(r#","resetHint":"maybe""#), "resetHint"),
            (e(r#","resetHint":true"#), "resetHint"),
            (
                e(r#","aggregationTemporality":null,"resetHint":"yes""#),
                "gauge histogram",
            ),
            (
                e(r#","aggregationTemporality":null,"resetHint":"no""#),
                "gauge histogram",
            ),
            (c(r#","explicitBounds":[1,1],"bucketCounts":[0,1,0]"#), "increase"),
            (c(r#","explicitBounds":[1,2],"bucketCounts":[0,1]"#), "one more"),
        ] {
            let refused = admit(&json).unwrap_err();
            assert!(refused.contains(why), "{json}: {refused}");
        }
    }

    fn exemplar(labels: &[(&str, &str)], value: f64, timestamp: i64) -> Exemplar {
        Exemplar {
            labels: (labels.iter())
                .map(|(name, value)| Label {
                    name: name.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            value,
            timestamp,
        }
    }

    #[test]
    fn a_sample_row_carries_its_exemplars_their_labels_sorted() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","value":1,"exemplars":[{{"time":"2026-10-11T17:59:59.5Z","value":0.25,
            "traceId":"t1","spanId":"s1","filteredAttributes":{{"user":"u","a":"b"}}}}]}}"#
        ))
        .unwrap();
        assert_eq!(item.points[0].point, Some(Point::Sample(1.0)));
        assert_eq!(
            item.points[0].exemplars,
            [exemplar(
                &[("a", "b"), ("span_id", "s1"), ("trace_id", "t1"), ("user", "u")],
                0.25,
                1_791_741_599_500
            )]
        );
    }

    /// An OTLP exemplar: an integer value, hex ids, attributes of every scalar type, and no time but the point's.
    #[test]
    fn an_otlp_exemplar_is_sent_at_its_point_s_time() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","type":"sum","value":3,"exemplars":[{{"value":7,
            "traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174",
            "filteredAttributes":{{"n":2,"ok":true,"none":null,"empty":""}}}}]}}"#
        ))
        .unwrap();
        assert_eq!(
            item.points[0].exemplars,
            [exemplar(
                &[
                    ("n", "2"),
                    ("ok", "true"),
                    ("span_id", "eee19b7ec3c1b174"),
                    ("trace_id", "5b8efff798038103d269b633813fc60c"),
                ],
                7.0,
                1_791_741_600_000
            )]
        );
    }

    #[test]
    fn a_native_histogram_row_carries_its_exemplars() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","type":"exponentialHistogram","aggregationTemporality":2,"count":1,"sum":3,"scale":0,
            "positive":{{"offset":1,"bucketCounts":[1]}},"exemplars":[{{"value":3,"traceId":"t"}}]}}"#
        ))
        .unwrap();
        let request = sent(&[item]);
        let series = &request.timeseries[0];
        assert_eq!(series.histograms.len(), 1);
        assert_eq!(
            series.exemplars,
            [exemplar(&[("trace_id", "t")], 3.0, 1_791_741_600_000)]
        );
    }

    /// As Prometheus exposes a classic histogram's exemplars: each on the bucket that holds its value.
    #[test]
    fn a_classic_histogram_s_exemplars_go_on_the_buckets_holding_them() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","type":"histogram","aggregationTemporality":2,"count":3,"sum":12,
            "explicitBounds":[1,5],"bucketCounts":[1,1,1],
            "exemplars":[{{"value":1}},{{"value":4}},{{"value":7}},{{"value":-2}}]}}"#
        ))
        .unwrap();
        // Each series' label values, and its exemplars' values.
        let by_series: Vec<(Vec<&str>, Vec<f64>)> = (item.points.iter())
            .map(|p| {
                (
                    p.labels.iter().map(|(_, v)| v.as_str()).collect(),
                    p.exemplars.iter().map(|e| e.value).collect(),
                )
            })
            .collect();
        assert_eq!(
            by_series,
            [
                (vec!["x_bucket", "1"], vec![1.0, -2.0]),
                (vec!["x_bucket", "5"], vec![4.0]),
                (vec!["x_bucket", "+Inf"], vec![7.0]),
                (vec!["x_sum"], vec![]),
                (vec!["x_count"], vec![]),
            ]
        );
    }

    /// A Prometheus receiver gives a series with exemplars and no sample as a row of no value.
    #[test]
    fn a_row_of_exemplars_alone_sends_no_sample() {
        let alone = admit(&format!(
            r#"{{{T},"name":"x","value":null,"exemplars":[{{"time":"2026-10-11T18:00:02Z","value":2}},
            {{"time":"2026-10-11T18:00:01Z","value":1}}]}}"#
        ))
        .unwrap();
        assert_eq!(alone.points[0].point, None);
        let gauge = admit(&format!(
            r#"{{{T},"name":"x","type":"gauge","exemplars":[{{"value":1}}]}}"#
        ))
        .unwrap();
        assert_eq!(gauge.points[0].point, None);
        let request = sent(&[alone]);
        let series = &request.timeseries[0];
        assert!(series.samples.is_empty() && series.histograms.is_empty());
        let times: Vec<i64> = series.exemplars.iter().map(|e| e.timestamp).collect();
        assert_eq!(times, [1_791_741_601_000, 1_791_741_602_000]);
    }

    /// Exemplars alone split no run: they ride in the entry beside them.
    #[test]
    fn exemplars_alone_join_the_entry_beside_them() {
        let at = |t: &str, body: &str| admit(&format!(r#"{{"time":"{t}","name":"x",{body}}}"#)).unwrap();
        let exemplars = r#""exemplars":[{"value":1}]"#;
        let histogram = r#""type":"exponentialHistogram","aggregationTemporality":2,"count":0,"sum":0,"scale":0"#;
        let request = sent(&[
            at("2026-10-11T18:00:00Z", exemplars),
            at("2026-10-11T18:00:01Z", r#""value":1"#),
            at("2026-10-11T18:00:02Z", exemplars),
            at("2026-10-11T18:00:03Z", r#""value":2"#),
            at("2026-10-11T18:00:04Z", histogram),
            at("2026-10-11T18:00:05Z", exemplars),
        ]);
        let shape: Vec<(usize, usize, usize)> = (request.timeseries.iter())
            .map(|s| (s.samples.len(), s.exemplars.len(), s.histograms.len()))
            .collect();
        assert_eq!(shape, [(2, 2, 0), (0, 1, 1)]);
    }

    #[test]
    fn a_series_holds_the_exemplars_of_its_rows_oldest_first() {
        let at = |t: &str, e: &str| {
            admit(&format!(
                r#"{{"time":"{t}","name":"x","value":1,"exemplars":[{{"time":"{e}","value":1}}]}}"#
            ))
            .unwrap()
        };
        let request = sent(&[
            at("2026-10-11T18:00:02Z", "2026-10-11T18:00:01.5Z"),
            at("2026-10-11T18:00:01Z", "2026-10-11T18:00:00.5Z"),
        ]);
        assert_eq!(request.timeseries.len(), 1);
        let times: Vec<i64> = request.timeseries[0].exemplars.iter().map(|e| e.timestamp).collect();
        assert_eq!(times, [1_791_741_600_500, 1_791_741_601_500]);
    }

    /// Prometheus drops an exemplar at its series' newest time whose value is lower than the newest one's.
    #[test]
    fn exemplars_at_one_time_go_lowest_value_first() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","value":1,"exemplars":[{{"value":7}},{{"value":3}}]}}"#
        ))
        .unwrap();
        let values: Vec<f64> = sent(&[item]).timeseries[0].exemplars.iter().map(|e| e.value).collect();
        assert_eq!(values, [3.0, 7.0]);
    }

    /// Prometheus's `labels.Hash`: xxhash64, seed 0, of each name and value followed by `0xff`.
    #[test]
    fn labels_hash_as_prometheus_has_it() {
        assert_eq!(labels_hash(&[]), 0xEF46_DB37_51D8_E999);
        let labels = exemplar(&[("a", "b"), ("trace_id", "t")], 0.0, 0).labels;
        assert_eq!(
            labels_hash(&labels),
            twox_hash::XxHash64::oneshot(0, b"a\xffb\xfftrace_id\xfft\xff")
        );
    }

    #[test]
    fn exemplars_at_one_time_and_value_go_in_their_labels_hash_order() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","value":1,"exemplars":[{{"value":1,"traceId":"a"}},{{"value":1,"traceId":"b"}},
            {{"value":1,"traceId":"c"}}]}}"#
        ))
        .unwrap();
        let hashes: Vec<u64> = (sent(&[item]).timeseries[0].exemplars.iter())
            .map(|e| labels_hash(&e.labels))
            .collect();
        assert!(hashes.is_sorted(), "{hashes:?}");
    }

    #[test]
    fn an_exemplar_is_sized_by_its_labels() {
        let bare = admit(&format!(r#"{{{T},"name":"x","value":1}}"#)).unwrap();
        let with = admit(&format!(
            r#"{{{T},"name":"x","value":1,"exemplars":[{{"value":1,"traceId":"abcd"}}]}}"#
        ))
        .unwrap();
        assert_eq!(
            protocol().size(&with) - protocol().size(&bare),
            24 + "trace_id".len() + 4 + 6
        );
    }

    #[test]
    fn an_exemplar_prometheus_would_not_store_is_left_out_and_its_row_sent() {
        let e = |exemplars: &str| format!(r#"{{{T},"name":"x","value":1,"exemplars":{exemplars}}}"#);
        for json in [
            e(r#"{"value":1}"#),
            e("[1]"),
            e("[{}]"),
            e(r#"[{"value":null}]"#),
            e(r#"[{"value":"1"}]"#),
            e(r#"[{"value":1,"time":17}]"#),
            e(r#"[{"value":1,"time":"soon"}]"#),
            e(&format!(r#"[{{"value":1,"traceId":"{}"}}]"#, "a".repeat(121))),
        ] {
            let item = admit(&json).unwrap();
            assert_eq!(item.points[0].point, Some(Point::Sample(1.0)), "{json}");
            assert!(item.points[0].exemplars.is_empty(), "{json}");
        }
    }

    #[test]
    fn an_attribute_that_is_no_label_is_left_out_of_its_exemplar() {
        let labels = |attributes: &str| {
            let item = admit(&format!(
                r#"{{{T},"name":"x","value":1,"exemplars":[{{"value":1,"traceId":"t","filteredAttributes":{attributes}}}]}}"#
            ))
            .unwrap();
            let e = &item.points[0].exemplars[0];
            e.labels
                .iter()
                .map(|l| format!("{}={}", l.name, l.value))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            labels(r#"{"a":[1],"b":{},"":"c","trace_id":"u","d":"e"}"#),
            ["d=e", "trace_id=t"]
        );
        assert_eq!(labels("[]"), ["trace_id=t"]);
        assert_eq!(labels("7"), ["trace_id=t"]);
    }

    /// Prometheus stores no exemplar whose labels pass 128 runes, so its attributes go first and its trace stays.
    #[test]
    fn an_exemplar_past_prometheus_s_label_limit_keeps_its_trace_and_span() {
        let trace = "5b8efff798038103d269b633813fc60c";
        let span = "eee19b7ec3c1b174";
        let at = |route: &str| {
            let item = admit(&format!(
                r#"{{{T},"name":"x","value":1,"exemplars":[{{"value":1,"traceId":"{trace}","spanId":"{span}",
                "filteredAttributes":{{"route":"{route}"}}}}]}}"#
            ))
            .unwrap();
            item.points[0].exemplars[0].labels.len()
        };
        // trace_id, span_id and route: 8 + 32 + 7 + 16 + 5 runes, and the rest of 128 for the route's value.
        assert_eq!(at(&"é".repeat(128 - 68)), 3);
        assert_eq!(at(&"é".repeat(128 - 67)), 2);
    }

    /// No value and no exemplars Prometheus would store is no point at all.
    #[test]
    fn a_row_of_no_value_and_no_exemplar_is_refused() {
        for exemplars in ["[]", "null", r#"[{"value":null}]"#] {
            let json = format!(r#"{{{T},"name":"x","exemplars":{exemplars}}}"#);
            assert!(admit(&json).unwrap_err().contains("NaN"), "{json}");
        }
    }

    /// The metadata a row carries, as it is sent.
    fn metadata_of(json: &str) -> MetricMetadata {
        admit(json).unwrap().metadata.expect("the row carries metadata")
    }

    #[test]
    fn a_family_s_type_is_its_row_s() {
        for (fields, kind) in [
            ("", MetricType::Unknown),
            (r#","type":null"#, MetricType::Unknown),
            (r#","type":"gauge""#, MetricType::Gauge),
            (r#","type":"sum","isMonotonic":true"#, MetricType::Counter),
            (r#","type":"sum","isMonotonic":false"#, MetricType::Gauge),
            (r#","type":"sum""#, MetricType::Gauge),
            (
                r#","type":"histogram","aggregationTemporality":2"#,
                MetricType::Histogram,
            ),
            (r#","type":"histogram""#, MetricType::GaugeHistogram),
            (
                r#","type":"exponentialHistogram","aggregationTemporality":2"#,
                MetricType::Histogram,
            ),
            (
                r#","type":"exponentialHistogram","aggregationTemporality":1"#,
                MetricType::GaugeHistogram,
            ),
            (r#","type":"summary""#, MetricType::Summary),
            (r#","type":"info""#, MetricType::Info),
            (r#","type":"stateset""#, MetricType::Stateset),
        ] {
            let json = format!(r#"{{"name":"x","time":null,"value":null{fields}}}"#);
            assert_eq!(metadata_of(&json).r#type, kind as i32, "{json}");
        }
    }

    #[test]
    fn a_family_of_a_type_prometheus_has_none_of_is_refused_naming_why() {
        let refused = admit(r#"{"name":"x","type":"set"}"#).unwrap_err();
        assert!(refused.contains("\"set\""), "{refused}");
        let refused = admit(r#"{"name":"x","type":1}"#).unwrap_err();
        assert!(refused.contains("type"), "{refused}");
        let refused = admit(r#"{"type":"gauge"}"#).unwrap_err();
        assert!(refused.contains("name"), "{refused}");
    }

    /// As a Prometheus receiver gives a family's metadata: its own record, of no time and no value.
    #[test]
    fn a_row_of_metadata_alone_sends_no_series() {
        let item = admit(
            r#"{"time":null,"value":null,"name":"http_requests","description":"Requests served.","unit":"requests",
            "type":"sum","isMonotonic":true,"aggregationTemporality":2,"exemplars":[]}"#,
        )
        .unwrap();
        assert!(item.points.is_empty());
        assert_eq!(item.time, METADATA_TIME);
        let request = sent(&[item]);
        assert!(request.timeseries.is_empty());
        assert_eq!(
            request.metadata,
            [MetricMetadata {
                r#type: MetricType::Counter as i32,
                metric_family_name: "http_requests".into(),
                help: "Requests served.".into(),
                unit: "requests".into(),
            }]
        );
        // With no description or unit, both go empty.
        let bare = metadata_of(r#"{"name":"x"}"#);
        assert_eq!((bare.help.as_str(), bare.unit.as_str()), ("", ""));
    }

    #[test]
    fn metadata_goes_before_every_point() {
        let order = protocol().order().unwrap();
        let alone = admit(r#"{"name":"x"}"#).unwrap();
        let point = admit(r#"{"time":"0001-01-01T00:00:00Z","name":"x","value":1}"#).unwrap();
        assert_eq!(order(&alone, &point), std::cmp::Ordering::Less);
    }

    #[test]
    fn a_point_with_a_description_or_unit_carries_its_family_s_metadata() {
        let item = admit(&format!(
            r#"{{{T},"name":"x","description":"d","type":"histogram","aggregationTemporality":2,"count":1,"sum":1,
            "explicitBounds":[],"bucketCounts":[1]}}"#
        ))
        .unwrap();
        let request = sent(&[item]);
        assert_eq!(request.timeseries.len(), 3);
        assert_eq!(
            request.metadata,
            [MetricMetadata {
                r#type: MetricType::Histogram as i32,
                metric_family_name: "x".into(),
                help: "d".into(),
                unit: String::new(),
            }]
        );
        let unit = metadata_of(&format!(r#"{{{T},"name":"y","unit":"s","value":1}}"#));
        assert_eq!((unit.unit.as_str(), unit.r#type), ("s", MetricType::Unknown as i32));
        for fields in ["", r#","description":"","unit":"""#, r#","description":7"#] {
            let json = format!(r#"{{{T},"name":"y","value":1{fields}}}"#);
            assert_eq!(admit(&json).unwrap().metadata, None, "{json}");
        }
    }

    /// A row of a time and nothing at it is a point missing its value, not metadata.
    #[test]
    fn a_row_of_a_time_and_no_value_is_no_metadata() {
        let refused = admit(&format!(r#"{{{T},"name":"x","description":"d"}}"#)).unwrap_err();
        assert!(refused.contains("NaN"), "{refused}");
    }

    #[test]
    fn a_request_sends_each_family_s_metadata_once_the_first_in_its_order() {
        let first = admit(r#"{"name":"x","description":"first","type":"gauge"}"#).unwrap();
        let second = admit(&format!(r#"{{{T},"name":"x","description":"second","value":1}}"#)).unwrap();
        let other = admit(r#"{"name":"y","unit":"s"}"#).unwrap();
        let request = sent(&[first, second, other]);
        let families: Vec<(&str, &str)> = (request.metadata.iter())
            .map(|m| (m.metric_family_name.as_str(), m.help.as_str()))
            .collect();
        assert_eq!(families, [("x", "first"), ("y", "")]);
        assert_eq!(request.timeseries.len(), 1);
    }

    #[test]
    fn metadata_is_sized_by_its_strings() {
        let bare = admit(&format!(r#"{{{T},"name":"x","value":1}}"#)).unwrap();
        let with = admit(&format!(
            r#"{{{T},"name":"x","value":1,"description":"help","unit":"s"}}"#
        ))
        .unwrap();
        assert_eq!(protocol().size(&with) - protocol().size(&bare), "x".len() + 4 + 1 + 10);
        let alone = admit(r#"{"name":"abc","description":"de"}"#).unwrap();
        assert_eq!(protocol().size(&alone), 3 + 2 + 10);
    }

    #[test]
    fn answers() {
        let read = |status| {
            protocol().read(
                &Response {
                    status,
                    headers: Vec::new(),
                    body: b"why".to_vec(),
                },
                &samples(2),
            )
        };
        assert!(read(204).unwrap().is_clean());
        for status in [429, 500, 503] {
            let outcome = read(status).unwrap();
            assert_eq!((outcome.retryable, outcome.throttled), (vec![0, 1], true), "{status}");
        }
        for status in [400, 409, 413, 422] {
            assert!(read(status).unwrap().split.is_some(), "{status}");
        }
        for status in [401, 403, 404] {
            assert!(read(status).is_err(), "{status}");
        }
    }

    /// Sending again what a 2.0 store wrote part of repeats points older than the newest it now holds.
    #[test]
    fn a_2_0_refusal_that_wrote_some_of_the_request_landed() {
        let read = |written: &str| {
            protocol_of(RemoteWriteVersion::V2).read(
                &Response {
                    status: 400,
                    headers: vec![(WRITTEN[0].to_string(), written.to_string())],
                    body: b"out of order sample".to_vec(),
                },
                &samples(2),
            )
        };
        assert!(read("1").unwrap().is_clean());
        assert!(read("0").unwrap().split.is_some());
    }

    /// A 2.0 request's series as each one's labels, samples as (value, time, start), and metadata, resolved from its
    /// table.
    type SentV2 = (
        Vec<(String, String)>,
        Vec<(f64, i64, i64)>,
        Option<(i32, String, String)>,
    );

    fn sent_v2(protocol: &RemoteWrite, items: &[Item]) -> (RequestV2, Vec<SentV2>) {
        let body = protocol.body(items);
        let raw = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
        let request = RequestV2::decode(raw.as_slice()).unwrap();
        assert_eq!(
            request.symbols.first().map(String::as_str),
            Some(""),
            "the table opens with the empty string"
        );
        let s = |at: u32| request.symbols[at as usize].clone();
        let series = (request.timeseries.iter())
            .map(|t| {
                let labels = t.labels_refs.chunks(2).map(|p| (s(p[0]), s(p[1]))).collect();
                let samples = (t.samples.iter())
                    .map(|x| (x.value, x.timestamp, x.start_timestamp))
                    .collect();
                let metadata = (t.metadata.as_ref()).map(|m| (m.r#type, s(m.help_ref), s(m.unit_ref)));
                (labels, samples, metadata)
            })
            .collect();
        (request, series)
    }

    #[test]
    fn a_2_0_request_names_each_string_once_and_carries_start_times() {
        let protocol = protocol_of(RemoteWriteVersion::V2);
        let items: Vec<Item> = [
            r#"{"time":"1970-01-01T00:00:02Z","startTime":"1970-01-01T00:00:01Z","name":"up","value":1,
            "attributes":{"job":"api"}}"#,
            r#"{"time":"1970-01-01T00:00:02Z","name":"temp","value":2,"attributes":{"job":"api"}}"#,
        ]
        .iter()
        .map(|json| protocol.admit(&row(json)).unwrap())
        .collect();
        let (request, series) = sent_v2(&protocol, &items);
        assert_eq!(request.symbols, ["", "__name__", "up", "job", "api", "temp"]);
        let pairs = |p: &[(&str, &str)]| {
            p.iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            series,
            [
                (
                    pairs(&[("__name__", "up"), ("job", "api")]),
                    vec![(1.0, 2_000, 1_000)],
                    None
                ),
                (
                    pairs(&[("__name__", "temp"), ("job", "api")]),
                    vec![(2.0, 2_000, 0)],
                    None
                ),
            ]
        );
        // 1.0 has no start time.
        assert_eq!(sent(&items).timeseries[0].samples[0].start_timestamp, 0);
    }

    #[test]
    fn a_2_0_series_carries_its_familys_latest_metadata_from_an_earlier_request() {
        let protocol = protocol_of(RemoteWriteVersion::V2);
        let admit = |json: &str| protocol.admit(&row(json)).unwrap();
        let metadata = [
            admit(r#"{"name":"requests_total","type":"sum","isMonotonic":true,"description":"Old."}"#),
            admit(r#"{"name":"requests_total","type":"sum","isMonotonic":true,"description":"Served."}"#),
            admit(r#"{"name":"latency","type":"histogram","aggregationTemporality":2,"unit":"seconds"}"#),
        ];
        let (alone, series) = sent_v2(&protocol, &metadata);
        assert!(
            alone.timeseries.is_empty() && series.is_empty(),
            "metadata alone sends no series"
        );
        let points: Vec<Item> = [
            r#"{"time":"1970-01-01T00:00:01Z","name":"requests_total","value":1}"#,
            r#"{"time":"1970-01-01T00:00:01Z","name":"requests_count","value":1}"#,
            r#"{"time":"1970-01-01T00:00:01Z","name":"latency","type":"histogram","aggregationTemporality":2,
            "count":1,"sum":0.5,"explicitBounds":[1],"bucketCounts":[1,0]}"#,
        ]
        .iter()
        .map(|json| admit(json))
        .collect();
        let (_, series) = sent_v2(&protocol, &points);
        let metadata: Vec<_> = (series.iter())
            .map(|(labels, _, m)| (labels[0].1.clone(), m.clone()))
            .collect();
        let counter = Some((MetricType::Counter as i32, "Served.".to_string(), String::new()));
        let histogram = Some((MetricType::Histogram as i32, String::new(), "seconds".to_string()));
        assert_eq!(
            metadata,
            [
                ("requests_total".to_string(), counter),
                ("requests_count".to_string(), None),
                ("latency_bucket".to_string(), histogram.clone()),
                ("latency_bucket".to_string(), histogram.clone()),
                ("latency_sum".to_string(), histogram.clone()),
                ("latency_count".to_string(), histogram),
            ],
            "a counter's family is not a _count series'; a classic histogram's series each carry its family's"
        );
    }

    #[test]
    fn a_2_0_destination_remembers_a_bounded_number_of_families() {
        let protocol = protocol_of(RemoteWriteVersion::V2);
        let items: Vec<Item> = (0..=FAMILIES)
            .map(|i| {
                protocol
                    .admit(&row(&format!(r#"{{"name":"f{i}","type":"gauge"}}"#)))
                    .unwrap()
            })
            .collect();
        protocol.body(&items);
        let families = protocol.families.lock().unwrap();
        assert_eq!(families.by_name.len(), FAMILIES);
        assert!(
            families.by_name.contains_key(&format!("f{FAMILIES}")),
            "the latest family is remembered"
        );
    }

    #[test]
    fn a_2_0_destination_remembers_a_bounded_number_of_bytes() {
        let protocol = protocol_of(RemoteWriteVersion::V2);
        let help = "h".repeat(1 << 20);
        let items: Vec<Item> = (0..20)
            .map(|i| {
                let json = format!(r#"{{"name":"f{i}","type":"gauge","description":"{help}"}}"#);
                protocol.admit(&row(&json)).unwrap()
            })
            .collect();
        protocol.body(&items);
        let families = protocol.families.lock().unwrap();
        assert!(families.bytes <= FAMILY_BYTES, "{}", families.bytes);
        assert!(families.by_name.len() < 20);
        assert!(families.by_name.contains_key("f19"), "the latest family is remembered");
        let held: usize = (families.by_name.values())
            .map(|m| m.metric_family_name.len() + m.help.len() + m.unit.len())
            .sum();
        assert_eq!(families.bytes, held);
    }

    #[test]
    fn a_2_0_request_s_own_metadata_goes_past_the_remembered_bounds() {
        let protocol = protocol_of(RemoteWriteVersion::V2);
        let help = "h".repeat(FAMILY_BYTES + 1);
        let json =
            format!(r#"{{"time":"1970-01-01T00:00:02Z","name":"up","value":1,"type":"gauge","description":"{help}"}}"#);
        let (_, series) = sent_v2(&protocol, &[protocol.admit(&row(&json)).unwrap()]);
        assert_eq!(series[0].2.as_ref().map(|m| m.1.len()), Some(FAMILY_BYTES + 1));
        assert!(
            protocol.families.lock().unwrap().by_name.is_empty(),
            "too large to remember"
        );
    }

    #[test]
    fn a_2_0_answer_that_says_nothing_written_stalls_the_destination_naming_1_0() {
        let answer = |headers: &[(&str, &str)]| Response {
            status: 204,
            headers: (headers.iter()).map(|(n, v)| (n.to_string(), v.to_string())).collect(),
            body: Vec::new(),
        };
        let v2 = protocol_of(RemoteWriteVersion::V2);
        let why = v2.read(&answer(&[]), &samples(1)).err().unwrap().to_string();
        assert!(why.contains("set this destination's version to 1.0"), "{why}");
        for header in WRITTEN {
            assert!(
                v2.read(&answer(&[(header, "0")]), &samples(1)).unwrap().is_clean(),
                "{header}"
            );
        }
        assert!(
            protocol().read(&answer(&[]), &samples(1)).unwrap().is_clean(),
            "1.0 has no such headers"
        );
    }

    #[test]
    fn a_2_0_store_writing_part_of_a_request_counts_the_rest_unwritten() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let unwritten = Unwritten::new(crate::metrics::MetricsHandle::new(registry.clone(), "egress-7"), 7, "d");
        let v2 = protocol_of(RemoteWriteVersion::V2).with_unwritten(unwritten);
        let mut items = samples(3);
        items.push(
            admit(&format!(
                r#"{{{T},"name":"x","value":1,"exemplars":[{{"value":1,"time":"2026-10-11T17:59:59.5Z"}}]}}"#
            ))
            .unwrap(),
        );
        let answer = Response {
            status: 204,
            headers: [(WRITTEN[0], "3"), (WRITTEN[1], "0"), (WRITTEN[2], "1")]
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .to_vec(),
            body: Vec::new(),
        };
        assert!(
            v2.read(&answer, &items).unwrap().is_clean(),
            "the rest is not sent again"
        );
        let counts: Vec<(String, crate::metrics::MetricValue)> = (registry.snapshot().into_iter())
            .filter(|s| s.key.name == "smeltr_destination_unwritten_total")
            .map(|s| (format!("{:?}", s.key.labels), s.value))
            .collect();
        assert_eq!(counts.len(), 1, "{counts:?}");
        assert!(counts[0].0.contains("sample"), "{counts:?}");
        assert!(
            matches!(counts[0].1, crate::metrics::MetricValue::Counter(1)),
            "{counts:?}"
        );
    }

    #[test]
    fn a_store_refusing_2_0_stalls_the_destination_naming_1_0() {
        let response = Response {
            status: 415,
            headers: Vec::new(),
            body: b"unsupported".to_vec(),
        };
        let why = protocol_of(RemoteWriteVersion::V2)
            .read(&response, &samples(1))
            .err()
            .unwrap()
            .to_string();
        assert!(why.contains("set this destination's version to 1.0"), "{why}");
        let why = protocol().read(&response, &samples(1)).err().unwrap().to_string();
        assert!(!why.contains("2.0"), "{why}");
    }
}
