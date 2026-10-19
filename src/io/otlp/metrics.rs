//! OTLP metrics: each data point of each `Metric` one record of `SourceMeta::otlp(Signal::Metrics)`, its metric's fields with
//! its own.

use std::sync::Arc;

use prost::Message as _;

use super::json::{self, Reader, each, enumerated, fields, oneof};
use super::{
    Cell, Cursor, Filed, InstrumentationScope, KeyValue, Number, Resource, Row, Shape, id, int, micros, nonzero, pairs,
    put_pairs, text, variant_of,
};
use crate::core::variant::{begin_array, begin_object, end, put_datetime, put_f64, put_i64, put_key, put_str};

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct ExportMetricsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ResourceMetrics {
    #[prost(message, optional, tag = "1")]
    resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ScopeMetrics {
    #[prost(message, optional, tag = "1")]
    scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    metrics: Vec<Metric>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Metric {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    description: String,
    #[prost(string, tag = "3")]
    unit: String,
    #[prost(oneof = "Data", tags = "5, 7, 9, 10, 11")]
    data: Option<Data>,
    #[prost(message, repeated, tag = "12")]
    metadata: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum Data {
    #[prost(message, tag = "5")]
    Gauge(Gauge),
    #[prost(message, tag = "7")]
    Sum(Sum),
    #[prost(message, tag = "9")]
    Histogram(Histogram),
    #[prost(message, tag = "10")]
    ExponentialHistogram(ExponentialHistogram),
    #[prost(message, tag = "11")]
    Summary(Summary),
}

#[derive(Clone, PartialEq, prost::Message)]
struct Gauge {
    #[prost(message, repeated, tag = "1")]
    data_points: Vec<NumberDataPoint>,
}

/// `aggregation_temporality` here and below is OTLP's enum, read as its number.
#[derive(Clone, PartialEq, prost::Message)]
struct Sum {
    #[prost(message, repeated, tag = "1")]
    data_points: Vec<NumberDataPoint>,
    #[prost(int32, tag = "2")]
    aggregation_temporality: i32,
    #[prost(bool, tag = "3")]
    is_monotonic: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Histogram {
    #[prost(message, repeated, tag = "1")]
    data_points: Vec<HistogramDataPoint>,
    #[prost(int32, tag = "2")]
    aggregation_temporality: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ExponentialHistogram {
    #[prost(message, repeated, tag = "1")]
    data_points: Vec<ExponentialHistogramDataPoint>,
    #[prost(int32, tag = "2")]
    aggregation_temporality: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Summary {
    #[prost(message, repeated, tag = "1")]
    data_points: Vec<SummaryDataPoint>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct NumberDataPoint {
    #[prost(message, repeated, tag = "7")]
    attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "2")]
    start_time_unix_nano: u64,
    #[prost(fixed64, tag = "3")]
    time_unix_nano: u64,
    #[prost(oneof = "PointValue", tags = "4, 6")]
    value: Option<PointValue>,
    #[prost(message, repeated, tag = "5")]
    exemplars: Vec<Exemplar>,
    #[prost(uint32, tag = "8")]
    flags: u32,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum PointValue {
    #[prost(double, tag = "4")]
    AsDouble(f64),
    #[prost(sfixed64, tag = "6")]
    AsInt(i64),
}

#[derive(Clone, PartialEq, prost::Message)]
struct HistogramDataPoint {
    #[prost(message, repeated, tag = "9")]
    attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "2")]
    start_time_unix_nano: u64,
    #[prost(fixed64, tag = "3")]
    time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    count: u64,
    #[prost(double, optional, tag = "5")]
    sum: Option<f64>,
    #[prost(fixed64, repeated, tag = "6")]
    bucket_counts: Vec<u64>,
    #[prost(double, repeated, tag = "7")]
    explicit_bounds: Vec<f64>,
    #[prost(message, repeated, tag = "8")]
    exemplars: Vec<Exemplar>,
    #[prost(uint32, tag = "10")]
    flags: u32,
    #[prost(double, optional, tag = "11")]
    min: Option<f64>,
    #[prost(double, optional, tag = "12")]
    max: Option<f64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ExponentialHistogramDataPoint {
    #[prost(message, repeated, tag = "1")]
    attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "2")]
    start_time_unix_nano: u64,
    #[prost(fixed64, tag = "3")]
    time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    count: u64,
    #[prost(double, optional, tag = "5")]
    sum: Option<f64>,
    #[prost(sint32, tag = "6")]
    scale: i32,
    #[prost(fixed64, tag = "7")]
    zero_count: u64,
    #[prost(message, optional, tag = "8")]
    positive: Option<Buckets>,
    #[prost(message, optional, tag = "9")]
    negative: Option<Buckets>,
    #[prost(uint32, tag = "10")]
    flags: u32,
    #[prost(message, repeated, tag = "11")]
    exemplars: Vec<Exemplar>,
    #[prost(double, optional, tag = "12")]
    min: Option<f64>,
    #[prost(double, optional, tag = "13")]
    max: Option<f64>,
    #[prost(double, tag = "14")]
    zero_threshold: f64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Buckets {
    #[prost(sint32, tag = "1")]
    offset: i32,
    #[prost(uint64, repeated, tag = "2")]
    bucket_counts: Vec<u64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SummaryDataPoint {
    #[prost(message, repeated, tag = "7")]
    attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "2")]
    start_time_unix_nano: u64,
    #[prost(fixed64, tag = "3")]
    time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    count: u64,
    #[prost(double, tag = "5")]
    sum: f64,
    #[prost(message, repeated, tag = "6")]
    quantile_values: Vec<ValueAtQuantile>,
    #[prost(uint32, tag = "8")]
    flags: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ValueAtQuantile {
    #[prost(double, tag = "1")]
    quantile: f64,
    #[prost(double, tag = "2")]
    value: f64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Exemplar {
    #[prost(message, repeated, tag = "7")]
    filtered_attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "2")]
    time_unix_nano: u64,
    #[prost(oneof = "ExemplarValue", tags = "3, 6")]
    value: Option<ExemplarValue>,
    #[prost(bytes = "vec", tag = "4")]
    span_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    trace_id: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum ExemplarValue {
    #[prost(double, tag = "3")]
    AsDouble(f64),
    #[prost(sfixed64, tag = "6")]
    AsInt(i64),
}

/// The messages of a metrics request, as `walk` reads them. A metric's data and an exponential histogram point's
/// buckets are singular fields, so inside their parent's struct. Bucket counts and bounds are repeated numbers.
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Request,
    ResourceMetrics,
    ScopeMetrics,
    Metric,
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    Buckets,
    SummaryDataPoint,
    ValueAtQuantile,
    Exemplar,
}

impl Shape for Kind {
    const ALL: &'static [Kind] = &[
        Kind::Request,
        Kind::ResourceMetrics,
        Kind::ScopeMetrics,
        Kind::Metric,
        Kind::Gauge,
        Kind::Sum,
        Kind::Histogram,
        Kind::ExponentialHistogram,
        Kind::Summary,
        Kind::NumberDataPoint,
        Kind::HistogramDataPoint,
        Kind::ExponentialHistogramDataPoint,
        Kind::Buckets,
        Kind::SummaryDataPoint,
        Kind::ValueAtQuantile,
        Kind::Exemplar,
    ];

    fn size(self) -> usize {
        match self {
            Kind::Request => size_of::<ExportMetricsServiceRequest>(),
            Kind::ResourceMetrics => size_of::<ResourceMetrics>(),
            Kind::ScopeMetrics => size_of::<ScopeMetrics>(),
            Kind::Metric => size_of::<Metric>(),
            Kind::Gauge => size_of::<Gauge>(),
            Kind::Sum => size_of::<Sum>(),
            Kind::Histogram => size_of::<Histogram>(),
            Kind::ExponentialHistogram => size_of::<ExponentialHistogram>(),
            Kind::Summary => size_of::<Summary>(),
            Kind::NumberDataPoint => size_of::<NumberDataPoint>(),
            Kind::HistogramDataPoint => size_of::<HistogramDataPoint>(),
            Kind::ExponentialHistogramDataPoint => size_of::<ExponentialHistogramDataPoint>(),
            Kind::Buckets => size_of::<Buckets>(),
            Kind::SummaryDataPoint => size_of::<SummaryDataPoint>(),
            Kind::ValueAtQuantile => size_of::<ValueAtQuantile>(),
            Kind::Exemplar => size_of::<Exemplar>(),
        }
    }

    fn field(self, tag: u32) -> Option<(super::Kind<Kind>, bool)> {
        use super::Kind as K;
        Some(match (self, tag) {
            (Kind::Request, 1) => (K::Signal(Kind::ResourceMetrics), true),
            (Kind::ResourceMetrics, 1) => (K::Resource, false),
            (Kind::ResourceMetrics, 2) => (K::Signal(Kind::ScopeMetrics), true),
            (Kind::ScopeMetrics, 1) => (K::Scope, false),
            (Kind::ScopeMetrics, 2) => (K::Signal(Kind::Metric), true),
            (Kind::Metric, 5) => (K::Signal(Kind::Gauge), false),
            (Kind::Metric, 7) => (K::Signal(Kind::Sum), false),
            (Kind::Metric, 9) => (K::Signal(Kind::Histogram), false),
            (Kind::Metric, 10) => (K::Signal(Kind::ExponentialHistogram), false),
            (Kind::Metric, 11) => (K::Signal(Kind::Summary), false),
            (Kind::Metric, 12) => (K::KeyValue, true),
            (Kind::Gauge | Kind::Sum, 1) => (K::Signal(Kind::NumberDataPoint), true),
            (Kind::Histogram, 1) => (K::Signal(Kind::HistogramDataPoint), true),
            (Kind::ExponentialHistogram, 1) => (K::Signal(Kind::ExponentialHistogramDataPoint), true),
            (Kind::Summary, 1) => (K::Signal(Kind::SummaryDataPoint), true),
            (Kind::NumberDataPoint, 7) => (K::KeyValue, true),
            (Kind::NumberDataPoint, 5) => (K::Signal(Kind::Exemplar), true),
            (Kind::HistogramDataPoint, 9) => (K::KeyValue, true),
            (Kind::HistogramDataPoint, 8) => (K::Signal(Kind::Exemplar), true),
            (Kind::ExponentialHistogramDataPoint, 1) => (K::KeyValue, true),
            (Kind::ExponentialHistogramDataPoint, 8 | 9) => (K::Signal(Kind::Buckets), false),
            (Kind::ExponentialHistogramDataPoint, 11) => (K::Signal(Kind::Exemplar), true),
            (Kind::SummaryDataPoint, 7) => (K::KeyValue, true),
            (Kind::SummaryDataPoint, 6) => (K::Signal(Kind::ValueAtQuantile), true),
            (Kind::Exemplar, 7) => (K::KeyValue, true),
            _ => return None,
        })
    }

    fn numbers(self, tag: u32) -> Option<Number> {
        match (self, tag) {
            (Kind::HistogramDataPoint, 6 | 7) => Some(Number::Fixed64),
            (Kind::Buckets, 2) => Some(Number::Varint),
            _ => None,
        }
    }
}

/// A decoded metrics request, its data points rendered a metric at a time as they are taken.
#[derive(Debug)]
pub struct Metrics {
    request: ExportMetricsServiceRequest,
    at: Cursor,
    /// The metric being read: its own fields, its points not yet taken, and what they are filed under.
    reading: Option<(Head, Points, Arc<Filed>)>,
}

impl Metrics {
    fn new(request: ExportMetricsServiceRequest) -> Metrics {
        Metrics {
            request,
            at: Cursor::default(),
            reading: None,
        }
    }

    pub(super) fn protobuf(body: &[u8]) -> Result<Metrics, prost::DecodeError> {
        ExportMetricsServiceRequest::decode(body).map(Metrics::new)
    }

    pub(super) fn json(r: &mut Reader) -> Result<Metrics, String> {
        let mut request = ExportMetricsServiceRequest::default();
        fields(r, "the request", |key, r| {
            match key {
                "resourceMetrics" | "resource_metrics" => {
                    request.resource_metrics = each(r, "resourceMetrics", resource_metrics)?
                }
                _ => return Ok(false),
            }
            Ok(true)
        })?;
        Ok(Metrics::new(request))
    }

    pub(super) fn encoded(&self) -> Vec<u8> {
        self.request.encode_to_vec()
    }

    pub(super) fn next_record(&mut self) -> Option<(Row, Arc<Filed>)> {
        loop {
            if let Some((head, points, filed)) = &mut self.reading
                && let Some(point) = points.next()
            {
                return Some((row(head, point), filed.clone()));
            }
            let (metric, filed) = self.at.next(
                &mut self.request.resource_metrics,
                |r| (r.resource.as_ref(), &mut r.scope_metrics),
                |s| (s.scope.as_ref(), &mut s.metrics),
            )?;
            self.reading = read(metric).map(|(head, points)| (head, points, filed));
        }
    }
}

/// What a metric gives each of its points' records.
#[derive(Debug)]
struct Head {
    name: Option<Cell>,
    description: Option<Cell>,
    unit: Option<Cell>,
    kind: &'static str,
    metadata: Option<Cell>,
    aggregation_temporality: Option<Cell>,
    is_monotonic: Option<Cell>,
}

/// What a data point gives its record; null where left `None`.
#[derive(Default)]
struct Point {
    start_time: Option<Cell>,
    time: Option<Cell>,
    attributes: Option<Cell>,
    flags: Option<Cell>,
    value: Option<Cell>,
    count: Option<Cell>,
    sum: Option<Cell>,
    min: Option<Cell>,
    max: Option<Cell>,
    bucket_counts: Option<Cell>,
    explicit_bounds: Option<Cell>,
    scale: Option<Cell>,
    zero_count: Option<Cell>,
    zero_threshold: Option<Cell>,
    positive: Option<Cell>,
    negative: Option<Cell>,
    quantile_values: Option<Cell>,
    exemplars: Option<Cell>,
}

/// A point's fields, in `SourceMeta::otlp(Signal::Metrics)`'s order.
fn row(head: &Head, point: Point) -> Row {
    Row(vec![
        head.name.clone(),
        head.description.clone(),
        head.unit.clone(),
        Some(Cell::Text(head.kind.to_string())),
        head.metadata.clone(),
        head.aggregation_temporality.clone(),
        head.is_monotonic.clone(),
        point.start_time,
        point.time,
        point.attributes,
        point.flags,
        point.value,
        point.count,
        point.sum,
        point.min,
        point.max,
        point.bucket_counts,
        point.explicit_bounds,
        point.scale,
        point.zero_count,
        point.zero_threshold,
        point.positive,
        point.negative,
        point.quantile_values,
        point.exemplars,
    ])
}

/// A metric's own fields and its points; none for a metric with no data.
fn read(metric: Metric) -> Option<(Head, Points)> {
    let data = metric.data?;
    let (kind, aggregation_temporality, is_monotonic) = match &data {
        Data::Gauge(_) => ("gauge", None, None),
        Data::Sum(s) => ("sum", int(s.aggregation_temporality), Some(Cell::Bool(s.is_monotonic))),
        Data::Histogram(h) => ("histogram", int(h.aggregation_temporality), None),
        Data::ExponentialHistogram(h) => ("exponentialHistogram", int(h.aggregation_temporality), None),
        Data::Summary(_) => ("summary", None, None),
    };
    let head = Head {
        name: text(metric.name),
        description: text(metric.description),
        unit: text(metric.unit),
        kind,
        metadata: pairs(&metric.metadata),
        aggregation_temporality,
        is_monotonic,
    };
    let points = match data {
        Data::Gauge(Gauge { data_points }) | Data::Sum(Sum { data_points, .. }) => {
            Points::Number(data_points.into_iter())
        }
        Data::Histogram(h) => Points::Histogram(h.data_points.into_iter()),
        Data::ExponentialHistogram(h) => Points::Exponential(h.data_points.into_iter()),
        Data::Summary(s) => Points::Summary(s.data_points.into_iter()),
    };
    Some((head, points))
}

/// A metric's points not yet taken, each rendered as it is taken, so a metric's fields are copied into one row at a
/// time rather than into all of its points' rows at once.
#[derive(Debug)]
enum Points {
    Number(std::vec::IntoIter<NumberDataPoint>),
    Histogram(std::vec::IntoIter<HistogramDataPoint>),
    Exponential(std::vec::IntoIter<ExponentialHistogramDataPoint>),
    Summary(std::vec::IntoIter<SummaryDataPoint>),
}

impl Iterator for Points {
    type Item = Point;

    fn next(&mut self) -> Option<Point> {
        match self {
            Points::Number(p) => p.next().map(number_point),
            Points::Histogram(p) => p.next().map(histogram_point),
            Points::Exponential(p) => p.next().map(exponential_point),
            Points::Summary(p) => p.next().map(summary_point),
        }
    }
}

fn number_point(p: NumberDataPoint) -> Point {
    Point {
        start_time: micros(p.start_time_unix_nano),
        time: micros(p.time_unix_nano),
        attributes: pairs(&p.attributes),
        flags: int(p.flags),
        value: p.value.map(|v| {
            Cell::Variant(variant_of(|out| match v {
                PointValue::AsInt(i) => put_i64(out, i),
                PointValue::AsDouble(d) => put_f64(out, d),
            }))
        }),
        exemplars: exemplars(&p.exemplars),
        ..Point::default()
    }
}

fn histogram_point(p: HistogramDataPoint) -> Point {
    Point {
        start_time: micros(p.start_time_unix_nano),
        time: micros(p.time_unix_nano),
        attributes: pairs(&p.attributes),
        flags: int(p.flags),
        count: Some(Cell::Float(count(p.count))),
        sum: p.sum.map(Cell::Float),
        min: p.min.map(Cell::Float),
        max: p.max.map(Cell::Float),
        bucket_counts: (!p.bucket_counts.is_empty())
            .then(|| Cell::Variant(variant_of(|out| put_counts(out, &p.bucket_counts)))),
        explicit_bounds: (!p.explicit_bounds.is_empty()).then(|| {
            Cell::Variant(variant_of(|out| {
                let open = begin_array(out);
                p.explicit_bounds.iter().for_each(|b| put_f64(out, *b));
                end(out, open, p.explicit_bounds.len());
            }))
        }),
        exemplars: exemplars(&p.exemplars),
        ..Point::default()
    }
}

fn exponential_point(p: ExponentialHistogramDataPoint) -> Point {
    Point {
        start_time: micros(p.start_time_unix_nano),
        time: micros(p.time_unix_nano),
        attributes: pairs(&p.attributes),
        flags: int(p.flags),
        count: Some(Cell::Float(count(p.count))),
        sum: p.sum.map(Cell::Float),
        min: p.min.map(Cell::Float),
        max: p.max.map(Cell::Float),
        scale: Some(Cell::Int(p.scale.into())),
        zero_count: Some(Cell::Float(count(p.zero_count))),
        zero_threshold: (p.zero_threshold != 0.0).then_some(Cell::Float(p.zero_threshold)),
        positive: p.positive.as_ref().map(buckets),
        negative: p.negative.as_ref().map(buckets),
        exemplars: exemplars(&p.exemplars),
        ..Point::default()
    }
}

fn summary_point(p: SummaryDataPoint) -> Point {
    Point {
        start_time: micros(p.start_time_unix_nano),
        time: micros(p.time_unix_nano),
        attributes: pairs(&p.attributes),
        flags: int(p.flags),
        count: Some(Cell::Float(count(p.count))),
        sum: Some(Cell::Float(p.sum)),
        quantile_values: (!p.quantile_values.is_empty()).then(|| {
            Cell::Variant(variant_of(|out| {
                let open = begin_array(out);
                for q in &p.quantile_values {
                    let object = begin_object(out);
                    put_key(out, "quantile");
                    put_f64(out, q.quantile);
                    put_key(out, "value");
                    put_f64(out, q.value);
                    end(out, object, 2);
                }
                end(out, open, p.quantile_values.len());
            }))
        }),
        ..Point::default()
    }
}

/// A count as the record holds it, a double, as a Prometheus histogram's count may be one. It is exact to 2^53, past
/// any real count.
fn count(n: u64) -> f64 {
    n as f64
}

fn put_counts(out: &mut Vec<u8>, counts: &[u64]) {
    let open = begin_array(out);
    counts.iter().for_each(|n| put_f64(out, count(*n)));
    end(out, open, counts.len());
}

/// An exponential histogram's buckets on one side of zero, as `{offset, bucketCounts}`.
fn buckets(b: &Buckets) -> Cell {
    Cell::Variant(variant_of(|out| {
        let open = begin_object(out);
        put_key(out, "offset");
        put_i64(out, b.offset.into());
        put_key(out, "bucketCounts");
        put_counts(out, &b.bucket_counts);
        end(out, open, 2);
    }))
}

/// `exemplars` as a list of objects, each leaving out what it does not carry; null for none.
fn exemplars(exemplars: &[Exemplar]) -> Option<Cell> {
    (!exemplars.is_empty()).then(|| {
        Cell::Variant(variant_of(|out| {
            let open = begin_array(out);
            for e in exemplars {
                put_exemplar(out, e);
            }
            end(out, open, exemplars.len());
        }))
    })
}

fn put_exemplar(out: &mut Vec<u8>, e: &Exemplar) {
    let open = begin_object(out);
    let mut count = 0;
    if let Some(micros) = nonzero((e.time_unix_nano / 1000) as i64) {
        put_key(out, "time");
        put_datetime(out, micros);
        count += 1;
    }
    if let Some(value) = &e.value {
        put_key(out, "value");
        match value {
            ExemplarValue::AsInt(i) => put_i64(out, *i),
            ExemplarValue::AsDouble(d) => put_f64(out, *d),
        }
        count += 1;
    }
    for (key, hex) in [("traceId", id(&e.trace_id, 16)), ("spanId", id(&e.span_id, 8))] {
        if let Some(hex) = hex {
            put_key(out, key);
            put_str(out, &hex);
            count += 1;
        }
    }
    if !e.filtered_attributes.is_empty() {
        put_key(out, "filteredAttributes");
        put_pairs(out, &e.filtered_attributes);
        count += 1;
    }
    end(out, open, count);
}

fn resource_metrics(r: &mut Reader) -> Result<ResourceMetrics, String> {
    let mut resource = ResourceMetrics::default();
    fields(r, "a `resourceMetrics` item", |key, r| {
        match key {
            "resource" => resource.resource = Some(json::resource(r)?),
            "scopeMetrics" | "scope_metrics" => resource.scope_metrics = each(r, "scopeMetrics", scope_metrics)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(resource)
}

fn scope_metrics(r: &mut Reader) -> Result<ScopeMetrics, String> {
    let mut scope = ScopeMetrics::default();
    fields(r, "a `scopeMetrics` item", |key, r| {
        match key {
            "scope" => scope.scope = Some(json::scope(r)?),
            "metrics" => scope.metrics = each(r, key, metric)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(scope)
}

fn metric(r: &mut Reader) -> Result<Metric, String> {
    let mut metric = Metric::default();
    let mut data = None;
    fields(r, "a `metrics` item", |key, r| {
        match key {
            "name" => metric.name = json::text(r, key)?,
            "description" => metric.description = json::text(r, key)?,
            "unit" => metric.unit = json::text(r, key)?,
            "metadata" => metric.metadata = each(r, key, json::key_value)?,
            "gauge" => oneof(&mut data, 0, r, |r| {
                let mut gauge = Gauge::default();
                fields(r, "`gauge`", |key, r| {
                    match key {
                        "dataPoints" | "data_points" => gauge.data_points = each(r, "dataPoints", number_data_point)?,
                        _ => return Ok(false),
                    }
                    Ok(true)
                })?;
                Ok(Data::Gauge(gauge))
            })?,
            "sum" => oneof(&mut data, 1, r, |r| {
                let mut sum = Sum::default();
                fields(r, "`sum`", |key, r| {
                    match key {
                        "dataPoints" | "data_points" => sum.data_points = each(r, "dataPoints", number_data_point)?,
                        "aggregationTemporality" | "aggregation_temporality" => {
                            sum.aggregation_temporality = temporality(r)?
                        }
                        "isMonotonic" | "is_monotonic" => sum.is_monotonic = json::bool(r, "isMonotonic")?,
                        _ => return Ok(false),
                    }
                    Ok(true)
                })?;
                Ok(Data::Sum(sum))
            })?,
            "histogram" => oneof(&mut data, 2, r, |r| {
                let mut histogram = Histogram::default();
                fields(r, "`histogram`", |key, r| {
                    match key {
                        "dataPoints" | "data_points" => {
                            histogram.data_points = each(r, "dataPoints", histogram_data_point)?
                        }
                        "aggregationTemporality" | "aggregation_temporality" => {
                            histogram.aggregation_temporality = temporality(r)?
                        }
                        _ => return Ok(false),
                    }
                    Ok(true)
                })?;
                Ok(Data::Histogram(histogram))
            })?,
            "exponentialHistogram" | "exponential_histogram" => oneof(&mut data, 3, r, |r| {
                let mut histogram = ExponentialHistogram::default();
                fields(r, "`exponentialHistogram`", |key, r| {
                    match key {
                        "dataPoints" | "data_points" => {
                            histogram.data_points = each(r, "dataPoints", exponential_histogram_data_point)?
                        }
                        "aggregationTemporality" | "aggregation_temporality" => {
                            histogram.aggregation_temporality = temporality(r)?
                        }
                        _ => return Ok(false),
                    }
                    Ok(true)
                })?;
                Ok(Data::ExponentialHistogram(histogram))
            })?,
            "summary" => oneof(&mut data, 4, r, |r| {
                let mut summary = Summary::default();
                fields(r, "`summary`", |key, r| {
                    match key {
                        "dataPoints" | "data_points" => {
                            summary.data_points = each(r, "dataPoints", summary_data_point)?
                        }
                        _ => return Ok(false),
                    }
                    Ok(true)
                })?;
                Ok(Data::Summary(summary))
            })?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    metric.data = data.map(|(_, data)| data);
    Ok(metric)
}

/// `aggregationTemporality`: its number, as OTLP/JSON writes an enum, or its name, as protobuf's JSON also reads one.
fn temporality(r: &mut Reader) -> Result<i32, String> {
    const NAMES: &[&str] = &[
        "AGGREGATION_TEMPORALITY_UNSPECIFIED",
        "AGGREGATION_TEMPORALITY_DELTA",
        "AGGREGATION_TEMPORALITY_CUMULATIVE",
    ];
    enumerated(r, "aggregationTemporality", NAMES)
}

fn number_data_point(r: &mut Reader) -> Result<NumberDataPoint, String> {
    let mut point = NumberDataPoint::default();
    let mut value = None;
    fields(r, "a number data point", |key, r| {
        match key {
            "attributes" => point.attributes = each(r, key, json::key_value)?,
            "startTimeUnixNano" | "start_time_unix_nano" => {
                point.start_time_unix_nano = json::int(r, "startTimeUnixNano")?
            }
            "timeUnixNano" | "time_unix_nano" => point.time_unix_nano = json::int(r, "timeUnixNano")?,
            "asDouble" | "as_double" => oneof(&mut value, 0, r, |r| {
                Ok(PointValue::AsDouble(json::double(r, "asDouble")?))
            })?,
            "asInt" | "as_int" => oneof(&mut value, 1, r, |r| Ok(PointValue::AsInt(json::int(r, "asInt")?)))?,
            "exemplars" => point.exemplars = each(r, key, exemplar)?,
            "flags" => point.flags = json::int(r, key)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    point.value = value.map(|(_, value)| value);
    Ok(point)
}

fn histogram_data_point(r: &mut Reader) -> Result<HistogramDataPoint, String> {
    let mut point = HistogramDataPoint::default();
    fields(r, "a histogram data point", |key, r| {
        match key {
            "attributes" => point.attributes = each(r, key, json::key_value)?,
            "startTimeUnixNano" | "start_time_unix_nano" => {
                point.start_time_unix_nano = json::int(r, "startTimeUnixNano")?
            }
            "timeUnixNano" | "time_unix_nano" => point.time_unix_nano = json::int(r, "timeUnixNano")?,
            "count" => point.count = json::int(r, key)?,
            "sum" => point.sum = Some(json::double(r, key)?),
            "bucketCounts" | "bucket_counts" => point.bucket_counts = json::ints(r, "bucketCounts")?,
            "explicitBounds" | "explicit_bounds" => point.explicit_bounds = json::doubles(r, "explicitBounds")?,
            "exemplars" => point.exemplars = each(r, key, exemplar)?,
            "flags" => point.flags = json::int(r, key)?,
            "min" => point.min = Some(json::double(r, key)?),
            "max" => point.max = Some(json::double(r, key)?),
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(point)
}

fn exponential_histogram_data_point(r: &mut Reader) -> Result<ExponentialHistogramDataPoint, String> {
    let mut point = ExponentialHistogramDataPoint::default();
    fields(r, "an exponential histogram data point", |key, r| {
        match key {
            "attributes" => point.attributes = each(r, key, json::key_value)?,
            "startTimeUnixNano" | "start_time_unix_nano" => {
                point.start_time_unix_nano = json::int(r, "startTimeUnixNano")?
            }
            "timeUnixNano" | "time_unix_nano" => point.time_unix_nano = json::int(r, "timeUnixNano")?,
            "count" => point.count = json::int(r, key)?,
            "sum" => point.sum = Some(json::double(r, key)?),
            "scale" => point.scale = json::int(r, key)?,
            "zeroCount" | "zero_count" => point.zero_count = json::int(r, "zeroCount")?,
            "positive" => point.positive = Some(json_buckets(r, key)?),
            "negative" => point.negative = Some(json_buckets(r, key)?),
            "flags" => point.flags = json::int(r, key)?,
            "exemplars" => point.exemplars = each(r, key, exemplar)?,
            "min" => point.min = Some(json::double(r, key)?),
            "max" => point.max = Some(json::double(r, key)?),
            "zeroThreshold" | "zero_threshold" => point.zero_threshold = json::double(r, "zeroThreshold")?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(point)
}

fn json_buckets(r: &mut Reader, name: &str) -> Result<Buckets, String> {
    let mut buckets = Buckets::default();
    fields(r, &format!("`{name}`"), |key, r| {
        match key {
            "offset" => buckets.offset = json::int(r, key)?,
            "bucketCounts" | "bucket_counts" => buckets.bucket_counts = json::ints(r, "bucketCounts")?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(buckets)
}

fn summary_data_point(r: &mut Reader) -> Result<SummaryDataPoint, String> {
    let mut point = SummaryDataPoint::default();
    fields(r, "a summary data point", |key, r| {
        match key {
            "attributes" => point.attributes = each(r, key, json::key_value)?,
            "startTimeUnixNano" | "start_time_unix_nano" => {
                point.start_time_unix_nano = json::int(r, "startTimeUnixNano")?
            }
            "timeUnixNano" | "time_unix_nano" => point.time_unix_nano = json::int(r, "timeUnixNano")?,
            "count" => point.count = json::int(r, key)?,
            "sum" => point.sum = json::double(r, key)?,
            "quantileValues" | "quantile_values" => point.quantile_values = each(r, "quantileValues", quantile)?,
            "flags" => point.flags = json::int(r, key)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(point)
}

fn quantile(r: &mut Reader) -> Result<ValueAtQuantile, String> {
    let mut quantile = ValueAtQuantile::default();
    fields(r, "a quantile value", |key, r| {
        match key {
            "quantile" => quantile.quantile = json::double(r, key)?,
            "value" => quantile.value = json::double(r, key)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(quantile)
}

fn exemplar(r: &mut Reader) -> Result<Exemplar, String> {
    let mut exemplar = Exemplar::default();
    let mut value = None;
    fields(r, "an exemplar", |key, r| {
        match key {
            "filteredAttributes" | "filtered_attributes" => {
                exemplar.filtered_attributes = each(r, "filteredAttributes", json::key_value)?
            }
            "timeUnixNano" | "time_unix_nano" => exemplar.time_unix_nano = json::int(r, "timeUnixNano")?,
            "asDouble" | "as_double" => oneof(&mut value, 0, r, |r| {
                Ok(ExemplarValue::AsDouble(json::double(r, "asDouble")?))
            })?,
            "asInt" | "as_int" => oneof(&mut value, 1, r, |r| Ok(ExemplarValue::AsInt(json::int(r, "asInt")?)))?,
            "spanId" | "span_id" => exemplar.span_id = json::id(r, "spanId")?,
            "traceId" | "trace_id" => exemplar.trace_id = json::id(r, "traceId")?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    exemplar.value = value.map(|(_, value)| value);
    Ok(exemplar)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::testing::{all, filed_as, kv, row_json, scope_of, shown, text};
    use super::super::{Any, Export, RESOURCE, Signal, decoded_size, variant};
    use super::*;

    fn read(request: &ExportMetricsServiceRequest) -> Vec<(Row, Arc<Filed>)> {
        all(Export::protobuf(Signal::Metrics, &request.encode_to_vec()).unwrap())
    }

    fn json(body: &str) -> Vec<(Row, Arc<Filed>)> {
        all(Export::json(Signal::Metrics, body.as_bytes()).unwrap())
    }

    fn shown_row(row: &Row) -> Value {
        row_json(Signal::Metrics, row)
    }

    /// A request of one resource and scope holding `metrics`.
    fn request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", text("api"))],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "lib".to_string(),
                        version: "1.2".to_string(),
                        ..Default::default()
                    }),
                    metrics,
                }],
            }],
        }
    }

    fn metric(name: &str, data: Data) -> Metric {
        Metric {
            name: name.to_string(),
            data: Some(data),
            ..Default::default()
        }
    }

    fn exemplar() -> Exemplar {
        Exemplar {
            filtered_attributes: vec![kv("user", text("u1"))],
            time_unix_nano: 1_700_000_000_000_003_000,
            value: Some(ExemplarValue::AsInt(7)),
            span_id: vec![0xab; 8],
            trace_id: (0..16).collect(),
        }
    }

    #[test]
    fn a_protobuf_request_reads_back_as_its_rows() {
        let start = 1_700_000_000_000_000_999;
        let time = 1_700_000_000_000_001_000;
        let read = read(&request(vec![
            Metric {
                name: "temp".to_string(),
                description: "how hot".to_string(),
                unit: "Cel".to_string(),
                data: Some(Data::Gauge(Gauge {
                    data_points: vec![NumberDataPoint {
                        attributes: vec![kv("room", text("a"))],
                        start_time_unix_nano: start,
                        time_unix_nano: time,
                        value: Some(PointValue::AsDouble(21.5)),
                        exemplars: vec![exemplar()],
                        flags: 1,
                    }],
                })),
                metadata: vec![kv("origin", text("probe"))],
            },
            metric(
                "requests",
                Data::Sum(Sum {
                    data_points: vec![NumberDataPoint {
                        time_unix_nano: time,
                        value: Some(PointValue::AsInt(-4)),
                        ..Default::default()
                    }],
                    aggregation_temporality: 2,
                    is_monotonic: true,
                }),
            ),
            metric(
                "latency",
                Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        time_unix_nano: time,
                        count: 6,
                        sum: Some(12.5),
                        bucket_counts: vec![1, 2, u64::MAX],
                        explicit_bounds: vec![1.0, 2.5],
                        exemplars: vec![Exemplar {
                            value: Some(ExemplarValue::AsDouble(1.5)),
                            ..Default::default()
                        }],
                        min: Some(0.5),
                        max: Some(9.0),
                        ..Default::default()
                    }],
                    aggregation_temporality: 1,
                }),
            ),
            metric(
                "size",
                Data::ExponentialHistogram(ExponentialHistogram {
                    data_points: vec![ExponentialHistogramDataPoint {
                        time_unix_nano: time,
                        count: 5,
                        sum: Some(3.0),
                        scale: -2,
                        zero_count: 1,
                        positive: Some(Buckets {
                            offset: -1,
                            bucket_counts: vec![2, 1],
                        }),
                        negative: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![1],
                        }),
                        min: Some(-1.0),
                        max: Some(4.0),
                        zero_threshold: 0.001,
                        ..Default::default()
                    }],
                    aggregation_temporality: 2,
                }),
            ),
            metric(
                "duration",
                Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        time_unix_nano: time,
                        count: 3,
                        sum: 4.5,
                        quantile_values: vec![
                            ValueAtQuantile {
                                quantile: 0.5,
                                value: 1.0,
                            },
                            ValueAtQuantile {
                                quantile: 1.0,
                                value: 2.5,
                            },
                        ],
                        flags: 1,
                        ..Default::default()
                    }],
                }),
            ),
        ]));
        let rows: Vec<Value> = read.iter().map(|(row, _)| shown_row(row)).collect();
        assert_eq!(
            rows,
            [
                json!({
                    "name": "temp", "description": "how hot", "unit": "Cel", "type": "gauge",
                    "metadata": {"origin": "probe"},
                    "startTime": 1_700_000_000_000_000_i64, "time": 1_700_000_000_000_001_i64,
                    "attributes": {"room": "a"}, "flags": 1, "value": 21.5,
                    "exemplars": [{
                        "time": "datetime:1700000000000003", "value": 7,
                        "traceId": "000102030405060708090a0b0c0d0e0f", "spanId": "abababababababab",
                        "filteredAttributes": {"user": "u1"},
                    }],
                }),
                json!({
                    "name": "requests", "type": "sum", "aggregationTemporality": 2, "isMonotonic": true,
                    "time": 1_700_000_000_000_001_i64, "value": -4,
                }),
                json!({
                    "name": "latency", "type": "histogram", "aggregationTemporality": 1,
                    "time": 1_700_000_000_000_001_i64, "count": 6.0, "sum": 12.5, "min": 0.5, "max": 9.0,
                    "bucketCounts": [1.0, 2.0, u64::MAX as f64], "explicitBounds": [1.0, 2.5],
                    "exemplars": [{"value": 1.5}],
                }),
                json!({
                    "name": "size", "type": "exponentialHistogram", "aggregationTemporality": 2,
                    "time": 1_700_000_000_000_001_i64, "count": 5.0, "sum": 3.0, "min": -1.0, "max": 4.0,
                    "scale": -2, "zeroCount": 1.0, "zeroThreshold": 0.001,
                    "positive": {"offset": -1, "bucketCounts": [2.0, 1.0]},
                    "negative": {"offset": 0, "bucketCounts": [1.0]},
                }),
                json!({
                    "name": "duration", "type": "summary", "time": 1_700_000_000_000_001_i64,
                    "flags": 1, "count": 3.0, "sum": 4.5,
                    "quantileValues": [{"quantile": 0.5, "value": 1.0}, {"quantile": 1.0, "value": 2.5}],
                }),
            ]
        );
        let Some(Cell::Variant(value)) = &read[1].0.0[11] else {
            panic!("a value is a variant")
        };
        assert!(
            matches!(variant::read(value), variant::Value::I64(-4)),
            "an int value stays an int"
        );
        for (_, filed) in &read {
            assert_eq!(filed_as(filed, RESOURCE), Some(json!({"service.name": "api"})));
            assert_eq!(scope_of(filed), Some(json!({"name": "lib", "version": "1.2"})));
        }
    }

    #[test]
    fn each_point_is_a_record_filed_under_its_scope() {
        let points = |values: &[i64]| {
            Data::Gauge(Gauge {
                data_points: (values.iter())
                    .map(|v| NumberDataPoint {
                        value: Some(PointValue::AsInt(*v)),
                        ..Default::default()
                    })
                    .collect(),
            })
        };
        let mut request = request(vec![
            metric("a", points(&[1, 2, 3])),
            Metric {
                name: "no data".to_string(),
                ..Default::default()
            },
            metric("no points", points(&[])),
            metric("b", points(&[4])),
        ]);
        request.resource_metrics[0].scope_metrics.push(ScopeMetrics {
            scope: Some(InstrumentationScope {
                name: "other".to_string(),
                ..Default::default()
            }),
            metrics: vec![metric("c", points(&[5, 6]))],
        });
        let read = read(&request);
        let named: Vec<(Value, Value)> = (read.iter())
            .map(|(row, _)| {
                let row = shown_row(row);
                (row["name"].clone(), row["value"].clone())
            })
            .collect();
        assert_eq!(
            named,
            [("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6)].map(|(n, v)| (n.into(), v.into()))
        );
        let shared: Vec<bool> = read.windows(2).map(|w| Arc::ptr_eq(&w[0].1, &w[1].1)).collect();
        assert_eq!(
            shared,
            [true, true, true, false, true],
            "the points of one scope share what they are filed under"
        );
        assert_eq!(scope_of(&read[4].1), Some(json!({"name": "other"})));
    }

    #[test]
    fn a_row_is_null_where_its_point_carries_nothing() {
        let read = read(&ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: [
                        Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 999,
                                exemplars: vec![Exemplar::default()],
                                ..Default::default()
                            }],
                        }),
                        Data::Sum(Sum {
                            data_points: vec![NumberDataPoint::default()],
                            ..Default::default()
                        }),
                        Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint::default()],
                            ..Default::default()
                        }),
                        Data::ExponentialHistogram(ExponentialHistogram {
                            data_points: vec![ExponentialHistogramDataPoint::default()],
                            ..Default::default()
                        }),
                        Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint::default()],
                        }),
                    ]
                    .map(|data| Metric {
                        data: Some(data),
                        ..Default::default()
                    })
                    .to_vec(),
                }],
            }],
        });
        let rows: Vec<Value> = read.iter().map(|(row, _)| shown_row(row)).collect();
        assert_eq!(
            rows,
            [
                json!({"type": "gauge", "exemplars": [{}]}),
                json!({"type": "sum", "isMonotonic": false}),
                json!({"type": "histogram", "count": 0.0}),
                json!({"type": "exponentialHistogram", "count": 0.0, "scale": 0, "zeroCount": 0.0}),
                json!({"type": "summary", "count": 0.0, "sum": 0.0}),
            ],
            "less than a microsecond is no time"
        );
        assert!(read[0].1.is_empty(), "an empty resource and scope file nothing");
    }

    #[test]
    fn json_reads_camel_case_ints_as_text_and_ids_as_hex() {
        let read = json(
            r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"host","value":{"stringValue":"h1"}}]},
            "scopeMetrics":[{"scope":{"name":"s"},"metrics":[
              {"name":"g","unit":"1","metadata":[{"key":"m","value":{"boolValue":true}}],
               "gauge":{"dataPoints":[{"timeUnixNano":"1700000000000000000","asInt":"9007199254740993",
                 "attributes":[{"key":"a","value":{"intValue":5}}],"flags":1,
                 "exemplars":[{"timeUnixNano":"2000","asDouble":"NaN","traceId":"5B8EFFF798038103D269B633813FC60C",
                   "spanId":"eee19b7ec3c1b174"}]},
                 {"asDouble":3}]}},
              {"name":"s","sum":{"aggregationTemporality":1,"isMonotonic":true,"dataPoints":[{"asDouble":1.5}]}},
              {"name":"h","histogram":{"aggregationTemporality":"2","dataPoints":[{"count":"3","sum":2,
                 "bucketCounts":["1",2],"explicitBounds":[0.5],"min":0,"max":"Infinity"}]}},
              {"name":"e","exponentialHistogram":{"dataPoints":[{"count":2,"scale":3,"zeroCount":"1",
                 "zeroThreshold":0.5,"positive":{"offset":-2,"bucketCounts":["1"]},"negative":{}}]}},
              {"name":"q","summary":{"dataPoints":[{"count":"2","sum":"3.5",
                 "quantileValues":[{"quantile":0.99,"value":3},{}]}]}}
            ]}]}]}"#,
        );
        let rows: Vec<Value> = read.iter().map(|(row, _)| shown_row(row)).collect();
        assert_eq!(
            rows,
            [
                json!({
                    "name": "g", "unit": "1", "type": "gauge", "metadata": {"m": true},
                    "time": 1_700_000_000_000_000_i64, "attributes": {"a": 5}, "flags": 1,
                    "value": 9007199254740993_i64,
                    "exemplars": [{"time": "datetime:2", "value": "f64:NaN",
                        "traceId": "5b8efff798038103d269b633813fc60c", "spanId": "eee19b7ec3c1b174"}],
                }),
                json!({"name": "g", "unit": "1", "type": "gauge", "metadata": {"m": true}, "value": 3.0}),
                json!({"name": "s", "type": "sum", "aggregationTemporality": 1, "isMonotonic": true, "value": 1.5}),
                json!({
                    "name": "h", "type": "histogram", "aggregationTemporality": 2, "count": 3.0, "sum": 2.0,
                    "min": 0.0, "max": "f64:inf", "bucketCounts": [1.0, 2.0], "explicitBounds": [0.5],
                }),
                json!({
                    "name": "e", "type": "exponentialHistogram", "count": 2.0, "scale": 3, "zeroCount": 1.0,
                    "zeroThreshold": 0.5, "positive": {"offset": -2, "bucketCounts": [1.0]},
                    "negative": {"offset": 0, "bucketCounts": []},
                }),
                json!({
                    "name": "q", "type": "summary", "count": 2.0, "sum": 3.5,
                    "quantileValues": [{"quantile": 0.99, "value": 3.0}, {"quantile": 0.0, "value": 0.0}],
                }),
            ]
        );
        for (_, filed) in &read {
            assert_eq!(filed_as(filed, RESOURCE), Some(json!({"host": "h1"})));
            assert_eq!(scope_of(filed), Some(json!({"name": "s"})));
        }
    }

    #[test]
    fn json_reads_protobuf_names_and_temporality_names_and_skips_unknown_fields() {
        let read = json(
            r#"{"resource_metrics":[{"scope_metrics":[{"metrics":[
              {"name":"s","sum":{"aggregation_temporality":"AGGREGATION_TEMPORALITY_CUMULATIVE","is_monotonic":true,
                 "data_points":[{"start_time_unix_nano":"5000","as_int":2,"future":[1]}]}},
              {"name":"h","histogram":{"aggregation_temporality":"AGGREGATION_TEMPORALITY_DELTA",
                 "data_points":[{"bucket_counts":[1],"explicit_bounds":[],"exemplars":[{"as_int":"3",
                   "filtered_attributes":[{"key":"k","value":{"string_value":"v"}}],"span_id":"0102030405060708"}]}]}},
              {"name":"e","exponential_histogram":{"aggregation_temporality":"AGGREGATION_TEMPORALITY_UNSPECIFIED",
                 "data_points":[{"zero_count":0,"zero_threshold":0,"positive":{"bucket_counts":[]}}]}},
              {"name":"q","summary":{"data_points":[{"quantile_values":[{"quantile":1,"value":1}]}]},"surprise":1},
              {"name":"none","newKind":{}}
            ],"schemaUrl":"u"}]}],"partialSuccess":{}}"#,
        );
        let rows: Vec<Value> = read.iter().map(|(row, _)| shown_row(row)).collect();
        assert_eq!(
            rows,
            [
                json!({"name": "s", "type": "sum", "aggregationTemporality": 2, "isMonotonic": true,
                       "startTime": 5, "value": 2}),
                json!({"name": "h", "type": "histogram", "aggregationTemporality": 1, "count": 0.0,
                       "bucketCounts": [1.0], "exemplars": [{"value": 3, "spanId": "0102030405060708",
                       "filteredAttributes": {"k": "v"}}]}),
                json!({"name": "e", "type": "exponentialHistogram", "count": 0.0, "scale": 0, "zeroCount": 0.0,
                       "positive": {"offset": 0, "bucketCounts": []}}),
                json!({"name": "q", "type": "summary", "count": 0.0, "sum": 0.0,
                       "quantileValues": [{"quantile": 1.0, "value": 1.0}]}),
            ],
            "a metric of a kind this module does not know is no record"
        );
    }

    #[test]
    fn a_request_with_no_points_reads_as_none() {
        assert!(all(Export::protobuf(Signal::Metrics, &[]).unwrap()).is_empty());
        assert!(json("{}").is_empty());
        assert!(json(r#"{"resourceMetrics":null}"#).is_empty());
        assert!(
            json(r#"{"resourceMetrics":[{"scopeMetrics":[{},{"metrics":[{"name":"x"},{"gauge":{}}]}]},{}]}"#)
                .is_empty()
        );
    }

    #[test]
    fn a_body_that_does_not_read_is_refused() {
        let protobuf = |body: &[u8]| Export::protobuf(Signal::Metrics, body).unwrap_err();
        assert!(protobuf(b"\x0a\xff\xff").contains("metrics"), "a length past the body");
        protobuf(b"\x0a\x02\x12\x00\xff");
        let refused = |body: &str| Export::json(Signal::Metrics, body.as_bytes()).unwrap_err();
        let point = |kind: &str, point: &str| {
            refused(&format!(
                r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[{{"{kind}":{{"dataPoints":[{point}]}}}}]}}]}}]}}"#
            ))
        };
        refused("");
        refused("not json");
        refused(r#"{"resourceMetrics":{}}"#);
        refused(r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"gauge":[]}]}]}]}"#);
        refused(r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":1}]}]}]}"#);
        assert!(
            refused(
                r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"sum":{"aggregationTemporality":"AGGREGATION_TEMPORALITY_SOMETIMES"}}]}]}]}"#
            )
            .contains("aggregationTemporality"),
            "an unknown temporality name"
        );
        refused(r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"sum":{"aggregationTemporality":"later"}}]}]}]}"#);
        refused(r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"sum":{"isMonotonic":1}}]}]}]}"#);
        point("gauge", r#"{"asInt":"soon"}"#);
        point("gauge", r#"{"asDouble":"lots"}"#);
        point("gauge", r#"{"timeUnixNano":-1}"#);
        point("gauge", r#"{"exemplars":[{"traceId":"xyz"}]}"#);
        point("histogram", r#"{"bucketCounts":[-1]}"#);
        point("histogram", r#"{"explicitBounds":["x"]}"#);
        point("histogram", r#"{"min":true}"#);
        point("exponentialHistogram", r#"{"scale":2147483648}"#);
        point("exponentialHistogram", r#"{"positive":[]}"#);
        point("summary", r#"{"quantileValues":[{"value":"x"}]}"#);
        let late = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"gauge":{"dataPoints":[{"asInt":1}]}}]}]},
            {"scopeMetrics":[{"metrics":[{"unit":3}]}]}]}"#;
        assert!(
            refused(late).contains("unit"),
            "a fault after a good point refuses the whole body"
        );
    }

    /// What a request holds decoded is read from its wire form: each repeated message its struct twice over, the first
    /// four times, down to every point, exemplar, attribute and quantile.
    #[test]
    fn a_request_holds_its_structs_decoded() {
        let attributes = vec![kv("k", Any::String("v".into()))];
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: attributes.clone(),
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "lib".into(),
                        ..Default::default()
                    }),
                    metrics: vec![
                        Metric {
                            metadata: attributes.clone(),
                            data: Some(Data::Gauge(Gauge {
                                data_points: vec![
                                    NumberDataPoint {
                                        attributes: attributes.clone(),
                                        exemplars: vec![Exemplar {
                                            filtered_attributes: attributes.clone(),
                                            ..Default::default()
                                        }],
                                        ..Default::default()
                                    },
                                    NumberDataPoint {
                                        value: Some(PointValue::AsInt(1)),
                                        ..Default::default()
                                    },
                                ],
                            })),
                            ..Default::default()
                        },
                        metric(
                            "h",
                            Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    bucket_counts: vec![1, 2, 3],
                                    explicit_bounds: vec![1.0, 2.0],
                                    exemplars: vec![exemplar(), exemplar()],
                                    ..Default::default()
                                }],
                                aggregation_temporality: 1,
                            }),
                        ),
                        metric(
                            "e",
                            Data::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint {
                                    attributes: attributes.clone(),
                                    positive: Some(Buckets {
                                        offset: 1,
                                        bucket_counts: vec![4, 5],
                                    }),
                                    ..Default::default()
                                }],
                                aggregation_temporality: 2,
                            }),
                        ),
                        metric(
                            "q",
                            Data::Summary(Summary {
                                data_points: vec![SummaryDataPoint {
                                    quantile_values: vec![ValueAtQuantile::default(); 3],
                                    ..Default::default()
                                }],
                            }),
                        ),
                    ],
                }],
            }],
        };
        let body = request.encode_to_vec();
        let size = decoded_size(Signal::Metrics, &body).unwrap();
        // A vector's first element takes room for four, and each after it two.
        let first = |n: usize| 4 + 2 * (n - 1);
        let structs = 4 * size_of::<ResourceMetrics>()
            + 4 * size_of::<ScopeMetrics>()
            + first(4) * size_of::<Metric>()
            + first(2) * size_of::<NumberDataPoint>()
            + 4 * size_of::<HistogramDataPoint>()
            + 4 * size_of::<ExponentialHistogramDataPoint>()
            + 4 * size_of::<SummaryDataPoint>()
            + first(3) * size_of::<ValueAtQuantile>()
            + (4 + first(2)) * size_of::<Exemplar>()
            // Seven lists of one: the resource's, the gauge's metadata, its point's and its exemplar's, the
            // exponential point's, and each of the two histogram exemplars'.
            + (4 * 7) * size_of::<KeyValue>();
        // Seven numbers in all: three bucket counts, two bounds and two exponential bucket counts, each eight bytes
        // and as much again of spare room.
        let numbers = 7 * 16;
        assert_eq!(size, body.len() + structs + numbers);
        assert_eq!(
            Export::protobuf(Signal::Metrics, &body).unwrap().decoded_size(),
            size,
            "a decoded request holds what its body said"
        );
        assert!(
            decoded_size(Signal::Metrics, &body[..body.len() - 1]).is_err(),
            "a cut-off body is refused"
        );
    }

    /// A packed bucket count of zero is one byte on the wire and eight decoded, so each is counted.
    #[test]
    fn a_packed_bucket_count_is_counted_at_its_decoded_size() {
        let counts = |n: usize| {
            request(vec![metric(
                "e",
                Data::ExponentialHistogram(ExponentialHistogram {
                    data_points: vec![ExponentialHistogramDataPoint {
                        positive: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![0; n],
                        }),
                        ..Default::default()
                    }],
                    aggregation_temporality: 0,
                }),
            )])
            .encode_to_vec()
        };
        let (none, many) = (counts(0), counts(1000));
        let size = |body: &[u8]| decoded_size(Signal::Metrics, body).unwrap();
        assert!(
            many.len() - none.len() < 1100,
            "a byte each, and the lengths around them"
        );
        assert_eq!(size(&many) - size(&none), (many.len() - none.len()) + 1000 * 16);
    }

    /// A metric's points are rendered as they are taken, not all at once.
    #[test]
    fn a_metric_renders_one_point_at_a_time() {
        let points = vec![NumberDataPoint::default(); 3];
        let mut export = Export::protobuf(
            Signal::Metrics,
            &request(vec![metric("g", Data::Gauge(Gauge { data_points: points }))]).encode_to_vec(),
        )
        .unwrap();
        let Export::Metrics(metrics) = &mut export else {
            unreachable!()
        };
        metrics.next_record().unwrap();
        let Some((_, Points::Number(left), _)) = &metrics.reading else {
            panic!("a gauge is being read")
        };
        assert_eq!(left.len(), 2, "two points are not rendered yet");
    }

    #[test]
    fn a_count_is_a_double() {
        assert_eq!(count(7), 7.0);
        assert_eq!(count(u64::MAX), u64::MAX as f64);
        let Cell::Variant(b) = buckets(&Buckets {
            offset: 3,
            bucket_counts: vec![2],
        }) else {
            unreachable!()
        };
        assert_eq!(shown(&b), json!({"offset": 3, "bucketCounts": [2.0]}));
    }
}
