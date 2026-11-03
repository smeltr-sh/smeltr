//! The OTLP destination, as a [`Protocol`] over the HTTP core (`io::http`): rows sent as OTLP/HTTP protobuf exports
//! of one signal, to the signal's path under the destination's URL (`http_path`), as an OpenTelemetry exporter sends
//! them to an endpoint.
//!
//! A row is the record an OTLP receiver of the signal gives (`SourceMeta::otlp`): a log record, a span, or one data
//! point with its metric's fields. Two fields of its own say what it came under, as a receiver's `__source` fields do:
//!
//! | field | holds | a receiver's |
//! |---|---|---|
//! | `resource` | the resource's attributes | `__source.resource` |
//! | `scope` | `name`, `version` and `attributes` | `__source.scope.*` |
//!
//! A row without them goes under an empty resource or scope. Fields OTLP has no place for are left out. A value keeps
//! its row's type (`rows`), so an attribute that was bytes arrives as its base64 text.
//!
//! A request carries each resource once, each scope of it once, and for metrics each metric of a scope once, with
//! every point of the request's rows under it, in the order the rows came. A metric is its name, description, unit,
//! metadata, type, and for a sum or a histogram its temporality and whether it is monotonic.
//!
//! What an answer means is the HTTP destination's (`http_post::answered`). A 2xx whose `partialSuccess` names items
//! the service refused lands every row, and counts those as unwritten.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use prost::Message as _;

use super::logs::LogRecord;
use super::metrics::{self, DataPoint, Metric, PointRow};
use super::traces::Span;
use super::{InstrumentationScope, Nested, Resource, http_path, logs, rows, traces};
use crate::io::http::{Outcome, Protocol, Response, Unwritten};
use crate::io::http_post::{answered, compressed};
use crate::io::{IoError, OwnedRecord};
use crate::plan::topology::{Compression, OtlpDestinationConfig, Signal};

/// The request and the answer.
#[derive(Clone, Debug)]
pub struct Otlp {
    signal: Signal,
    gzip: bool,
    /// What the last row admitted came under, which the next most often shares. Shared by every clone, though only
    /// the egress thread admits.
    last: Arc<Mutex<Last>>,
    /// Counts what the service says it refused, where the destination reports on itself.
    unwritten: Option<Unwritten>,
}

#[derive(Debug, Default)]
struct Last {
    scoped: Option<Arc<Scoped>>,
    metric: Option<Arc<Family>>,
}

impl Otlp {
    pub fn new(config: &OtlpDestinationConfig) -> Otlp {
        Otlp {
            signal: config.signal,
            gzip: config.http.compression == Compression::Gzip,
            last: Arc::default(),
            unwritten: None,
        }
    }

    pub fn with_unwritten(mut self, unwritten: Unwritten) -> Self {
        self.unwritten = Some(unwritten);
        self
    }

    /// What a row of `resource` and `scope` came under: the last row's where it is the same, so the rows of one
    /// resource and scope share it.
    fn scoped(&self, resource: Resource, scope: InstrumentationScope) -> Arc<Scoped> {
        let mut last = self.last.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(held) = &last.scoped
            && held.resource == resource
            && held.scope == scope
        {
            return held.clone();
        }
        let scoped = Arc::new(Scoped {
            resource_key: resource.encode_to_vec(),
            scope_key: scope.encode_to_vec(),
            resource,
            scope,
        });
        last.scoped = Some(scoped.clone());
        scoped
    }

    /// `metric`, which holds no points: the last row's where it is the same.
    fn family(&self, metric: Metric) -> Arc<Family> {
        let mut last = self.last.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(held) = &last.metric
            && held.metric == metric
        {
            return held.clone();
        }
        let family = Arc::new(Family {
            key: metric.encode_to_vec(),
            metric,
        });
        last.metric = Some(family.clone());
        family
    }
}

/// One row as its request carries it.
#[derive(Clone, Debug)]
pub struct Item {
    scoped: Arc<Scoped>,
    record: Record,
}

#[derive(Clone, Debug)]
enum Record {
    Log(Box<LogRecord>),
    Span(Box<Span>),
    Point(Arc<Family>, Box<DataPoint>),
}

/// A resource and a scope of it, each with its encoding, which tells it apart.
#[derive(Debug)]
struct Scoped {
    resource: Resource,
    scope: InstrumentationScope,
    resource_key: Vec<u8>,
    scope_key: Vec<u8>,
}

/// A metric holding no points, with its encoding, which tells it apart.
#[derive(Debug)]
struct Family {
    metric: Metric,
    key: Vec<u8>,
}

/// A row being read, before it is a record.
enum Reading {
    Log(LogRecord),
    Span(Span),
    Point(Box<PointRow>),
}

impl Protocol for Otlp {
    type Item = Item;

    fn name(&self) -> &'static str {
        "otlp"
    }

    fn path(&self) -> &str {
        http_path(self.signal)
    }

    fn admit(&self, row: &OwnedRecord) -> Result<Item, String> {
        let (mut resource, mut scope) = (Resource::default(), InstrumentationScope::default());
        let mut reading = match self.signal {
            Signal::Logs => Reading::Log(LogRecord::default()),
            Signal::Traces => Reading::Span(Span::default()),
            Signal::Metrics => Reading::Point(Box::default()),
        };
        rows::row(&row.value, |key, r| match key {
            "resource" => rows::resource(r).map(|read| resource = read).map(|()| true),
            "scope" => rows::scope(r).map(|read| scope = read).map(|()| true),
            _ => match &mut reading {
                Reading::Log(log) => log.field(key, r),
                Reading::Span(span) => span.field(key, r),
                Reading::Point(point) => point.field(key, r),
            },
        })?;
        let record = match reading {
            Reading::Log(log) => Record::Log(Box::new(log)),
            Reading::Span(span) => match span.refused() {
                Some(why) => return Err(why.into()),
                None => Record::Span(Box::new(span)),
            },
            Reading::Point(point) => {
                let (metric, point) = point.point()?;
                Record::Point(self.family(metric), Box::new(point))
            }
        };
        Ok(Item {
            scoped: self.scoped(resource, scope),
            record,
        })
    }

    /// The record as it is encoded, with its field's key and length, and the resource, scope and metric it came
    /// under, as if it were the first of a request under each: a request held to its limit is never larger than it.
    fn size(&self, item: &Item) -> usize {
        let len = match &item.record {
            Record::Log(log) => log.encoded_len(),
            Record::Span(span) => span.encoded_len(),
            Record::Point(family, point) => point.encoded_len() + family.key.len() + 6,
        };
        len + 6 + item.scoped.resource_key.len() + item.scoped.scope_key.len() + 12
    }

    fn body(&self, items: &[Item]) -> Vec<u8> {
        let request = match self.signal {
            Signal::Logs => logs::request(nested(items, |record| match record {
                Record::Log(log) => (**log).clone(),
                _ => unreachable!("a logs destination admits log records"),
            })),
            Signal::Traces => traces::request(nested(items, |record| match record {
                Record::Span(span) => (**span).clone(),
                _ => unreachable!("a traces destination admits spans"),
            })),
            Signal::Metrics => {
                let points = nested(items, |record| match record {
                    Record::Point(family, point) => (family.clone(), (**point).clone()),
                    _ => unreachable!("a metrics destination admits points"),
                });
                metrics::request(by_metric(points))
            }
        };
        compressed(request, self.gzip)
    }

    fn read(&self, response: &Response, items: &[Self::Item]) -> Result<Outcome, IoError> {
        if (200..300).contains(&response.status)
            && let Some(unwritten) = &self.unwritten
        {
            let partial = ExportResponse::decode(response.body.as_slice()).unwrap_or_default();
            if let Some(partial) = partial.partial_success.filter(|p| p.rejected > 0) {
                // The metric's label, and what a person reads.
                let (kind, items_of) = match self.signal {
                    Signal::Logs => ("logRecord", "log records"),
                    Signal::Metrics => ("dataPoint", "data points"),
                    Signal::Traces => ("span", "spans"),
                };
                // A request holds one item per row, and a service that names more refused than it was sent is held
                // to what it was.
                let rejected = (partial.rejected as u64).min(items.len() as u64);
                unwritten.note(kind, rejected, || {
                    format!(
                        "the service refused {rejected} of {} {items_of} in a request: {}",
                        items.len(),
                        partial.error_message
                    )
                });
            }
        }
        answered(response, items.len())
    }
}

/// `Export*ServiceResponse`, whatever its signal: each names what it refused by field 1 and why by field 2.
#[derive(Clone, PartialEq, prost::Message)]
struct ExportResponse {
    #[prost(message, optional, tag = "1")]
    partial_success: Option<PartialSuccess>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct PartialSuccess {
    #[prost(int64, tag = "1")]
    rejected: i64,
    #[prost(string, tag = "2")]
    error_message: String,
}

/// Where in a [`Nested`] each resource is, by its key, and each scope of it, by the scope's key.
type Places<'k> = HashMap<&'k [u8], (usize, HashMap<&'k [u8], usize>)>;

/// `items`' records, each made by `take`, under their scopes under their resources, each in the order its first row
/// came.
fn nested<T>(items: &[Item], take: impl Fn(&Record) -> T) -> Nested<T> {
    let mut resources: Nested<T> = Vec::new();
    let mut at = Places::new();
    for item in items {
        let s = &item.scoped;
        let (r, scopes) = at.entry(&s.resource_key).or_insert_with(|| {
            resources.push((s.resource.clone(), Vec::new()));
            (resources.len() - 1, HashMap::new())
        });
        let held = &mut resources[*r].1;
        let i = *scopes.entry(&s.scope_key).or_insert_with(|| {
            held.push((s.scope.clone(), Vec::new()));
            held.len() - 1
        });
        held[i].1.push(take(&item.record));
    }
    resources
}

/// Each scope's points as its metrics, each holding its points in the order they came.
fn by_metric(resources: Nested<(Arc<Family>, DataPoint)>) -> Nested<Metric> {
    (resources.into_iter())
        .map(|(resource, scopes)| {
            let scopes = (scopes.into_iter())
                .map(|(scope, points)| {
                    // Each point's metric, by where it first came.
                    let mut at: HashMap<&[u8], usize> = HashMap::new();
                    let places: Vec<usize> = (points.iter())
                        .map(|(family, _)| {
                            let next = at.len();
                            *at.entry(&family.key).or_insert(next)
                        })
                        .collect();
                    let mut families: Vec<(Arc<Family>, Vec<DataPoint>)> = Vec::with_capacity(at.len());
                    for ((family, point), i) in points.into_iter().zip(places) {
                        if i == families.len() {
                            families.push((family, Vec::new()));
                        }
                        families[i].1.push(point);
                    }
                    let metrics = (families.into_iter())
                        .map(|(family, points)| metrics::with_points(family.metric.clone(), points.into_iter()))
                        .collect();
                    (scope, metrics)
                })
                .collect();
            (resource, scopes)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::otlp::{Cell, Export};
    use crate::plan::config::Payload;
    use crate::plan::topology::HttpDestinationConfig;

    fn protocol(signal: Signal) -> Otlp {
        Otlp::new(&OtlpDestinationConfig {
            http: HttpDestinationConfig {
                url: "http://collector:4318".into(),
                zone_urls: Default::default(),
                body: Default::default(),
                payload: Payload::Record,
                compression: Compression::None,
                headers: Vec::new(),
                credentials: Vec::new(),
                tls: None,
                in_flight: 1,
                max_request_bytes: 1 << 20,
                buffer_bytes: 1 << 22,
                stall_after: std::time::Duration::from_secs(30),
            },
            signal,
        })
    }

    fn admitted(otlp: &Otlp, rows: &[&str]) -> Vec<Item> {
        (rows.iter())
            .map(|json| {
                let row = OwnedRecord {
                    value: json.as_bytes().to_vec(),
                    payload: Payload::Record,
                    ..OwnedRecord::default()
                };
                otlp.admit(&row).unwrap_or_else(|e| panic!("{json}: {e}"))
            })
            .collect()
    }

    /// Each record of a body, as the receiver reads it: its fields, and what it came under.
    fn read_back(signal: Signal, body: &[u8]) -> Vec<(Row, Filed)> {
        let mut export = Export::protobuf(signal, body).unwrap();
        std::iter::from_fn(|| export.next_record())
            .map(|(row, filed)| (row, (*filed).clone()))
            .collect()
    }

    use crate::io::otlp::{Filed, Row};

    /// What every signal's request nests: resources, each holding scopes, each holding items, by their field numbers.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Nesting {
        #[prost(message, repeated, tag = "1")]
        resources: Vec<Resourced>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Resourced {
        #[prost(message, repeated, tag = "2")]
        scopes: Vec<Scoping>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Scoping {
        #[prost(bytes = "vec", repeated, tag = "2")]
        items: Vec<Vec<u8>>,
    }

    /// How many items each scope of each resource of `body` holds.
    fn shape(body: &[u8]) -> Vec<Vec<usize>> {
        (Nesting::decode(body).unwrap().resources.iter())
            .map(|r| r.scopes.iter().map(|s| s.items.len()).collect())
            .collect()
    }

    #[test]
    fn a_path_is_its_signal_s() {
        assert_eq!(protocol(Signal::Logs).path(), "/v1/logs");
        assert_eq!(protocol(Signal::Metrics).path(), "/v1/metrics");
        assert_eq!(protocol(Signal::Traces).path(), "/v1/traces");
    }

    /// Rows of one resource and scope share them, whatever lies between: a request carries each once.
    #[test]
    fn a_request_carries_each_resource_and_scope_once() {
        let otlp = protocol(Signal::Logs);
        let items = admitted(
            &otlp,
            &[
                r#"{"body":"a","resource":{"service.name":"x"},"scope":{"name":"s"}}"#,
                r#"{"body":"b","resource":{"service.name":"y"}}"#,
                r#"{"body":"c","resource":{"service.name":"x"},"scope":{"name":"s"}}"#,
                r#"{"body":"d","resource":{"service.name":"x"},"scope":{"name":"t"}}"#,
            ],
        );
        let body = otlp.body(&items);
        assert_eq!(
            shape(&body),
            vec![vec![2, 1], vec![1]],
            "x under scopes s and t, then y"
        );
        let bodies: Vec<Option<Cell>> = (read_back(Signal::Logs, &body).into_iter())
            .map(|(row, _)| row.0[4].clone())
            .collect();
        let text = |s: &str| {
            Some(Cell::Variant(super::super::variant_of(|out| {
                crate::core::variant::put_str(out, s)
            })))
        };
        assert_eq!(
            bodies,
            vec![text("a"), text("c"), text("d"), text("b")],
            "each group in the order it came"
        );
    }

    /// Points of one metric go under it once, and a point of another type or temporality is another metric.
    #[test]
    fn points_of_one_metric_go_under_it_once() {
        let otlp = protocol(Signal::Metrics);
        let items = admitted(
            &otlp,
            &[
                r#"{"name":"up","type":"gauge","value":1,"time":"2026-11-03T00:00:00Z"}"#,
                r#"{"name":"up","type":"gauge","value":1.5,"time":"2026-11-03T00:00:01Z"}"#,
                r#"{"name":"up","type":"sum","aggregationTemporality":2,"isMonotonic":true,"value":3}"#,
                r#"{"name":"up","type":"sum","aggregationTemporality":1,"value":4}"#,
            ],
        );
        assert_eq!(shape(&otlp.body(&items)), vec![vec![3]], "three metrics");
        let values: Vec<_> = (read_back(Signal::Metrics, &otlp.body(&items)).into_iter())
            .map(|(row, _)| row.0[11].clone())
            .collect();
        let int = |i| {
            Some(Cell::Variant(super::super::variant_of(|out| {
                crate::core::variant::put_i64(out, i)
            })))
        };
        let double = Some(Cell::Variant(super::super::variant_of(|out| {
            crate::core::variant::put_f64(out, 1.5)
        })));
        assert_eq!(
            values,
            vec![int(1), double, int(3), int(4)],
            "an int stays an int, a double a double"
        );
    }

    #[test]
    fn a_row_otlp_cannot_take_is_refused_naming_why() {
        let row = |signal, json: &str| {
            protocol(signal).admit(&OwnedRecord {
                value: json.as_bytes().to_vec(),
                payload: Payload::Record,
                ..OwnedRecord::default()
            })
        };
        let refused = |signal, json: &str| row(signal, json).expect_err(json);
        assert!(refused(Signal::Metrics, r#"{"name":"up","value":1}"#).contains("type"));
        assert!(refused(Signal::Metrics, r#"{"name":"up","type":"stateset"}"#).contains("type"));
        assert!(refused(Signal::Metrics, r#"{"name":"h","type":"histogram","count":1.5}"#).contains("count"));
        assert!(refused(Signal::Traces, r#"{"name":"s"}"#).contains("traceId"));
        assert!(refused(Signal::Logs, r#"{"traceId":"abc"}"#).contains("traceId"));
        assert!(refused(Signal::Logs, r#"{"time":7}"#).contains("time"));
        assert!(refused(Signal::Logs, r#"{"resource":"x"}"#).contains("resource"));
        assert!(refused(Signal::Logs, "[1]").contains("row"));
        assert!(
            row(Signal::Logs, r#"{"anything":{"else":1},"body":null}"#).is_ok(),
            "a field OTLP has no place for is left out"
        );
    }

    /// A row carries bytes as base64 text, so an attribute that was bytes arrives as that text.
    #[test]
    fn bytes_arrive_as_their_base64_text() {
        let otlp = protocol(Signal::Logs);
        let items = admitted(&otlp, &[r#"{"attributes":{"raw":"AQI="}}"#]);
        let (row, _) = read_back(Signal::Logs, &otlp.body(&items)).remove(0);
        let text = super::super::variant_of(|out| {
            let open = crate::core::variant::begin_object(out);
            crate::core::variant::put_key(out, "raw");
            crate::core::variant::put_str(out, "AQI=");
            crate::core::variant::end(out, open, 1);
        });
        assert_eq!(row.0[5], Some(Cell::Variant(text)));
    }

    fn answer(status: u16, rejected: i64) -> Response {
        let body = ExportResponse {
            partial_success: Some(PartialSuccess {
                rejected,
                error_message: "too old".into(),
            }),
        };
        Response {
            status,
            headers: Vec::new(),
            body: body.encode_to_vec(),
        }
    }

    /// A partial success lands its request whole, counting what the service refused as unwritten.
    #[test]
    fn a_partial_success_lands_and_counts_what_was_refused() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let otlp = protocol(Signal::Traces).with_unwritten(Unwritten::new(
            crate::metrics::MetricsHandle::new(registry.clone(), "egress-7"),
            7,
            "otlp",
        ));
        let items = admitted(
            &otlp,
            &[r#"{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174"}"#],
        );
        assert_eq!(otlp.read(&answer(200, 1), &items).unwrap(), Outcome::default());
        let counts: Vec<(String, crate::metrics::MetricValue)> = (registry.snapshot().into_iter())
            .filter(|s| s.key.name == "smeltr_destination_unwritten_total")
            .map(|s| (format!("{:?}", s.key.labels), s.value))
            .collect();
        assert_eq!(counts.len(), 1, "{counts:?}");
        assert!(counts[0].0.contains("span"), "{counts:?}");
        assert!(
            matches!(counts[0].1, crate::metrics::MetricValue::Counter(1)),
            "{counts:?}"
        );
        assert_eq!(otlp.read(&answer(503, 0), &items).unwrap().retryable, vec![0]);
        assert!(otlp.read(&answer(400, 0), &items).unwrap().split.is_some());
        assert!(otlp.read(&answer(401, 0), &items).is_err());
    }
}
