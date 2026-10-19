//! OTLP traces: each `Span` one record of `SourceMeta::OTLP_TRACES`.

use std::sync::Arc;

use prost::Message as _;

use super::json::{self, Reader, each, enumerated, fields};
use super::{
    Cell, Cursor, Filed, InstrumentationScope, KeyValue, Resource, Row, Shape, id, int, micros, nonzero, pairs,
    put_pairs, text, variant_of,
};
use crate::core::variant::{begin_array, begin_object, end, put_datetime, put_i64, put_key, put_str};

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct ExportTraceServiceRequest {
    #[prost(message, repeated, tag = "1")]
    resource_spans: Vec<ResourceSpans>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ResourceSpans {
    #[prost(message, optional, tag = "1")]
    resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    scope_spans: Vec<ScopeSpans>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ScopeSpans {
    #[prost(message, optional, tag = "1")]
    scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    spans: Vec<Span>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Span {
    #[prost(bytes = "vec", tag = "1")]
    trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    span_id: Vec<u8>,
    #[prost(string, tag = "3")]
    trace_state: String,
    #[prost(bytes = "vec", tag = "4")]
    parent_span_id: Vec<u8>,
    #[prost(fixed32, tag = "16")]
    flags: u32,
    #[prost(string, tag = "5")]
    name: String,
    /// OTLP's span kind enum, read as its number.
    #[prost(int32, tag = "6")]
    kind: i32,
    #[prost(fixed64, tag = "7")]
    start_time_unix_nano: u64,
    #[prost(fixed64, tag = "8")]
    end_time_unix_nano: u64,
    #[prost(message, repeated, tag = "9")]
    attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "10")]
    dropped_attributes_count: u32,
    #[prost(message, repeated, tag = "11")]
    events: Vec<Event>,
    #[prost(uint32, tag = "12")]
    dropped_events_count: u32,
    #[prost(message, repeated, tag = "13")]
    links: Vec<Link>,
    #[prost(uint32, tag = "14")]
    dropped_links_count: u32,
    #[prost(message, optional, tag = "15")]
    status: Option<Status>,
}

/// `Span.Event`.
#[derive(Clone, PartialEq, prost::Message)]
struct Event {
    #[prost(fixed64, tag = "1")]
    time_unix_nano: u64,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(message, repeated, tag = "3")]
    attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "4")]
    dropped_attributes_count: u32,
}

/// `Span.Link`.
#[derive(Clone, PartialEq, prost::Message)]
struct Link {
    #[prost(bytes = "vec", tag = "1")]
    trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    span_id: Vec<u8>,
    #[prost(string, tag = "3")]
    trace_state: String,
    #[prost(message, repeated, tag = "4")]
    attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "5")]
    dropped_attributes_count: u32,
    #[prost(fixed32, tag = "6")]
    flags: u32,
}

/// A span's `Status`, less its deprecated code.
#[derive(Clone, PartialEq, prost::Message)]
struct Status {
    #[prost(string, tag = "2")]
    message: String,
    /// OTLP's status code enum, read as its number.
    #[prost(int32, tag = "3")]
    code: i32,
}

/// OTLP's span kind names, each at its number.
const SPAN_KINDS: &[&str] = &[
    "SPAN_KIND_UNSPECIFIED",
    "SPAN_KIND_INTERNAL",
    "SPAN_KIND_SERVER",
    "SPAN_KIND_CLIENT",
    "SPAN_KIND_PRODUCER",
    "SPAN_KIND_CONSUMER",
];

/// `StatusCode`'s names, each at its number.
const STATUS_CODES: &[&str] = &["STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR"];

/// The messages of a traces request, as `walk` reads them.
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Request,
    ResourceSpans,
    ScopeSpans,
    Span,
    Event,
    Link,
}

impl Shape for Kind {
    const ALL: &'static [Kind] = &[
        Kind::Request,
        Kind::ResourceSpans,
        Kind::ScopeSpans,
        Kind::Span,
        Kind::Event,
        Kind::Link,
    ];

    fn size(self) -> usize {
        match self {
            Kind::Request => size_of::<ExportTraceServiceRequest>(),
            Kind::ResourceSpans => size_of::<ResourceSpans>(),
            Kind::ScopeSpans => size_of::<ScopeSpans>(),
            Kind::Span => size_of::<Span>(),
            Kind::Event => size_of::<Event>(),
            Kind::Link => size_of::<Link>(),
        }
    }

    // `Status` holds no message and no repeated field, so it adds nothing past its parent's struct.
    fn field(self, tag: u32) -> Option<(super::Kind<Kind>, bool)> {
        use super::Kind as K;
        Some(match (self, tag) {
            (Kind::Request, 1) => (K::Signal(Kind::ResourceSpans), true),
            (Kind::ResourceSpans, 1) => (K::Resource, false),
            (Kind::ResourceSpans, 2) => (K::Signal(Kind::ScopeSpans), true),
            (Kind::ScopeSpans, 1) => (K::Scope, false),
            (Kind::ScopeSpans, 2) => (K::Signal(Kind::Span), true),
            (Kind::Span, 9) | (Kind::Event, 3) | (Kind::Link, 4) => (K::KeyValue, true),
            (Kind::Span, 11) => (K::Signal(Kind::Event), true),
            (Kind::Span, 13) => (K::Signal(Kind::Link), true),
            _ => return None,
        })
    }
}

/// A decoded traces request, its spans rendered one at a time as they are taken.
#[derive(Debug)]
pub struct Traces {
    request: ExportTraceServiceRequest,
    at: Cursor,
}

impl Traces {
    fn new(request: ExportTraceServiceRequest) -> Traces {
        Traces {
            request,
            at: Cursor::default(),
        }
    }

    pub(super) fn protobuf(body: &[u8]) -> Result<Traces, prost::DecodeError> {
        ExportTraceServiceRequest::decode(body).map(Traces::new)
    }

    pub(super) fn json(r: &mut Reader) -> Result<Traces, String> {
        let mut request = ExportTraceServiceRequest::default();
        fields(r, "the request", |key, r| {
            match key {
                "resourceSpans" | "resource_spans" => {
                    request.resource_spans = each(r, "resourceSpans", resource_spans)?
                }
                _ => return Ok(false),
            }
            Ok(true)
        })?;
        Ok(Traces::new(request))
    }

    pub(super) fn encoded(&self) -> Vec<u8> {
        self.request.encode_to_vec()
    }

    pub(super) fn next_record(&mut self) -> Option<(Row, Arc<Filed>)> {
        let (span, filed) = self.at.next(
            &mut self.request.resource_spans,
            |r| (r.resource.as_ref(), &mut r.scope_spans),
            |s| (s.scope.as_ref(), &mut s.spans),
        )?;
        Some((row(span), filed))
    }
}

/// A span's fields, in `SourceMeta::OTLP_TRACES`'s order.
fn row(span: Span) -> Row {
    let status = span.status.unwrap_or_default();
    Row(vec![
        id(&span.trace_id, 16).map(Cell::Text),
        id(&span.span_id, 8).map(Cell::Text),
        id(&span.parent_span_id, 8).map(Cell::Text),
        text(span.trace_state),
        int(span.flags),
        text(span.name),
        int(span.kind),
        micros(span.start_time_unix_nano),
        micros(span.end_time_unix_nano),
        pairs(&span.attributes),
        int(span.dropped_attributes_count),
        list(&span.events, put_event),
        int(span.dropped_events_count),
        list(&span.links, put_link),
        int(span.dropped_links_count),
        int(status.code),
        text(status.message),
    ])
}

/// `items` as an array, each written by `put`, or null for none.
fn list<T>(items: &[T], put: impl Fn(&mut Vec<u8>, &T)) -> Option<Cell> {
    (!items.is_empty()).then(|| {
        Cell::Variant(variant_of(|out| {
            let open = begin_array(out);
            for item in items {
                put(out, item);
            }
            end(out, open, items.len());
        }))
    })
}

/// An event as an object, each of its fields left out where it carries nothing, as a row's are null.
fn put_event(out: &mut Vec<u8>, event: &Event) {
    let open = begin_object(out);
    let mut count = 0;
    if let Some(time) = nonzero((event.time_unix_nano / 1000) as i64) {
        put_key(out, "time");
        put_datetime(out, time);
        count += 1;
    }
    count += put_text(out, "name", &event.name);
    count += put_attributes(out, &event.attributes);
    count += put_count(out, "droppedAttributesCount", event.dropped_attributes_count.into());
    end(out, open, count);
}

/// A link as an object, each of its fields left out where it carries nothing, as a row's are null.
fn put_link(out: &mut Vec<u8>, link: &Link) {
    let open = begin_object(out);
    let mut count = 0;
    count += put_text(out, "traceId", &id(&link.trace_id, 16).unwrap_or_default());
    count += put_text(out, "spanId", &id(&link.span_id, 8).unwrap_or_default());
    count += put_text(out, "traceState", &link.trace_state);
    count += put_attributes(out, &link.attributes);
    count += put_count(out, "droppedAttributesCount", link.dropped_attributes_count.into());
    count += put_count(out, "flags", link.flags.into());
    end(out, open, count);
}

// Each writes one entry of an object, or none for an empty value, and returns how many it wrote.

fn put_text(out: &mut Vec<u8>, key: &str, text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    put_key(out, key);
    put_str(out, text);
    1
}

fn put_attributes(out: &mut Vec<u8>, attributes: &[KeyValue]) -> usize {
    if attributes.is_empty() {
        return 0;
    }
    put_key(out, "attributes");
    put_pairs(out, attributes);
    1
}

fn put_count(out: &mut Vec<u8>, key: &str, n: i64) -> usize {
    let Some(n) = nonzero(n) else { return 0 };
    put_key(out, key);
    put_i64(out, n);
    1
}

fn resource_spans(r: &mut Reader) -> Result<ResourceSpans, String> {
    let mut resource = ResourceSpans::default();
    fields(r, "a `resourceSpans` item", |key, r| {
        match key {
            "resource" => resource.resource = Some(json::resource(r)?),
            "scopeSpans" | "scope_spans" => resource.scope_spans = each(r, "scopeSpans", scope_spans)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(resource)
}

fn scope_spans(r: &mut Reader) -> Result<ScopeSpans, String> {
    let mut scope = ScopeSpans::default();
    fields(r, "a `scopeSpans` item", |key, r| {
        match key {
            "scope" => scope.scope = Some(json::scope(r)?),
            "spans" => scope.spans = each(r, key, span)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(scope)
}

fn span(r: &mut Reader) -> Result<Span, String> {
    let mut span = Span::default();
    fields(r, "a `spans` item", |key, r| {
        match key {
            "traceId" | "trace_id" => span.trace_id = json::id(r, "traceId")?,
            "spanId" | "span_id" => span.span_id = json::id(r, "spanId")?,
            "traceState" | "trace_state" => span.trace_state = json::text(r, "traceState")?,
            "parentSpanId" | "parent_span_id" => span.parent_span_id = json::id(r, "parentSpanId")?,
            "flags" => span.flags = json::int(r, key)?,
            "name" => span.name = json::text(r, key)?,
            "kind" => span.kind = enumerated(r, key, SPAN_KINDS)?,
            "startTimeUnixNano" | "start_time_unix_nano" => {
                span.start_time_unix_nano = json::int(r, "startTimeUnixNano")?
            }
            "endTimeUnixNano" | "end_time_unix_nano" => span.end_time_unix_nano = json::int(r, "endTimeUnixNano")?,
            "attributes" => span.attributes = each(r, key, json::key_value)?,
            "droppedAttributesCount" | "dropped_attributes_count" => {
                span.dropped_attributes_count = json::int(r, "droppedAttributesCount")?
            }
            "events" => span.events = each(r, key, event)?,
            "droppedEventsCount" | "dropped_events_count" => {
                span.dropped_events_count = json::int(r, "droppedEventsCount")?
            }
            "links" => span.links = each(r, key, link)?,
            "droppedLinksCount" | "dropped_links_count" => {
                span.dropped_links_count = json::int(r, "droppedLinksCount")?
            }
            "status" => span.status = Some(status(r)?),
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(span)
}

fn event(r: &mut Reader) -> Result<Event, String> {
    let mut event = Event::default();
    fields(r, "an `events` item", |key, r| {
        match key {
            "timeUnixNano" | "time_unix_nano" => event.time_unix_nano = json::int(r, "timeUnixNano")?,
            "name" => event.name = json::text(r, key)?,
            "attributes" => event.attributes = each(r, key, json::key_value)?,
            "droppedAttributesCount" | "dropped_attributes_count" => {
                event.dropped_attributes_count = json::int(r, "droppedAttributesCount")?
            }
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(event)
}

fn link(r: &mut Reader) -> Result<Link, String> {
    let mut link = Link::default();
    fields(r, "a `links` item", |key, r| {
        match key {
            "traceId" | "trace_id" => link.trace_id = json::id(r, "traceId")?,
            "spanId" | "span_id" => link.span_id = json::id(r, "spanId")?,
            "traceState" | "trace_state" => link.trace_state = json::text(r, "traceState")?,
            "attributes" => link.attributes = each(r, key, json::key_value)?,
            "droppedAttributesCount" | "dropped_attributes_count" => {
                link.dropped_attributes_count = json::int(r, "droppedAttributesCount")?
            }
            "flags" => link.flags = json::int(r, key)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(link)
}

fn status(r: &mut Reader) -> Result<Status, String> {
    let mut status = Status::default();
    fields(r, "`status`", |key, r| {
        match key {
            "message" => status.message = json::text(r, key)?,
            "code" => status.code = enumerated(r, key, STATUS_CODES)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(status)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::testing::{all, filed_as, kv, row_json, scope_of, shown, text};
    use super::super::{Any, Export, RESOURCE, Signal, decoded_size};
    use super::*;

    fn read(request: &ExportTraceServiceRequest) -> Vec<(Row, Arc<Filed>)> {
        all(Export::protobuf(Signal::Traces, &request.encode_to_vec()).unwrap())
    }

    fn json(body: &str) -> Vec<(Row, Arc<Filed>)> {
        all(Export::json(Signal::Traces, body.as_bytes()).unwrap())
    }

    fn shown_row(row: &Row) -> Value {
        row_json(Signal::Traces, row)
    }

    fn one_span(spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![ScopeSpans { scope: None, spans }],
            }],
        }
    }

    #[test]
    fn a_protobuf_request_reads_back_as_its_rows() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", text("api"))],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "lib".to_string(),
                        version: "1.2".to_string(),
                        ..Default::default()
                    }),
                    spans: vec![Span {
                        trace_id: (0..16).collect(),
                        span_id: vec![0xab; 8],
                        trace_state: "a=b".to_string(),
                        parent_span_id: vec![0xcd; 8],
                        flags: 0x101,
                        name: "GET /".to_string(),
                        kind: 2,
                        start_time_unix_nano: 1_700_000_000_000_001_999,
                        end_time_unix_nano: 1_700_000_000_000_009_000,
                        attributes: vec![kv("n", Any::Int(-3)), kv("ok", Any::Bool(true))],
                        dropped_attributes_count: 1,
                        events: vec![
                            Event {
                                time_unix_nano: 1_700_000_000_000_005_000,
                                name: "retry".to_string(),
                                attributes: vec![kv("attempt", Any::Int(2))],
                                dropped_attributes_count: 3,
                            },
                            Event::default(),
                        ],
                        dropped_events_count: 4,
                        links: vec![
                            Link {
                                trace_id: vec![1; 16],
                                span_id: vec![2; 8],
                                trace_state: "c=d".to_string(),
                                attributes: vec![kv("why", text("batch"))],
                                dropped_attributes_count: 5,
                                flags: 1,
                            },
                            Link {
                                trace_id: vec![0; 16],
                                ..Default::default()
                            },
                        ],
                        dropped_links_count: 6,
                        status: Some(Status {
                            message: "boom".to_string(),
                            code: 2,
                        }),
                    }],
                }],
            }],
        };
        let mut read = read(&request);
        assert_eq!(read.len(), 1);
        let (row, filed) = read.remove(0);
        assert_eq!(
            shown_row(&row),
            json!({
                "traceId": "000102030405060708090a0b0c0d0e0f",
                "spanId": "abababababababab",
                "parentSpanId": "cdcdcdcdcdcdcdcd",
                "traceState": "a=b",
                "flags": 0x101,
                "name": "GET /",
                "kind": 2,
                "startTime": 1_700_000_000_000_001_i64,
                "endTime": 1_700_000_000_000_009_i64,
                "attributes": {"n": -3, "ok": true},
                "droppedAttributesCount": 1,
                "events": [
                    {
                        "time": "datetime:1700000000000005",
                        "name": "retry",
                        "attributes": {"attempt": 2},
                        "droppedAttributesCount": 3,
                    },
                    {},
                ],
                "droppedEventsCount": 4,
                "links": [
                    {
                        "traceId": "01".repeat(16),
                        "spanId": "02".repeat(8),
                        "traceState": "c=d",
                        "attributes": {"why": "batch"},
                        "droppedAttributesCount": 5,
                        "flags": 1,
                    },
                    {},
                ],
                "droppedLinksCount": 6,
                "statusCode": 2,
                "statusMessage": "boom",
            })
        );
        assert_eq!(filed_as(&filed, RESOURCE), Some(json!({"service.name": "api"})));
        assert_eq!(scope_of(&filed), Some(json!({"name": "lib", "version": "1.2"})));
    }

    #[test]
    fn a_row_is_null_where_its_span_carries_nothing() {
        let request = one_span(vec![
            Span {
                status: Some(Status::default()),
                ..Default::default()
            },
            Span {
                start_time_unix_nano: 999,
                events: vec![Event {
                    time_unix_nano: 999,
                    ..Default::default()
                }],
                ..Default::default()
            },
        ]);
        let read = read(&request);
        assert_eq!(shown_row(&read[0].0), json!({}));
        assert_eq!(read[0].0.size(), 0);
        assert!(read[0].1.is_empty(), "an empty resource and no scope file nothing");
        assert_eq!(
            shown_row(&read[1].0),
            json!({"events": [{}]}),
            "less than a microsecond is no time"
        );
    }

    #[test]
    fn json_reads_ints_as_text_or_numbers_ids_as_hex_and_enums_by_name() {
        let read = json(
            r#"{"resourceSpans":[{"resource":{"attributes":[{"key":"host","value":{"stringValue":"h1"}}]},
            "scopeSpans":[{"scope":{"name":"s"},
            "spans":[
              {"traceId":"5B8EFFF798038103D269B633813FC60C","spanId":"eee19b7ec3c1b174",
               "parentSpanId":"0102030405060708","name":"op","kind":"SPAN_KIND_SERVER","flags":"1",
               "startTimeUnixNano":"1700000000000000000","endTimeUnixNano":1700000000000001000,
               "attributes":[{"key":"a","value":{"intValue":"5"}}],"droppedAttributesCount":"2",
               "events":[{"timeUnixNano":"1700000000000000500","name":"e","droppedAttributesCount":1}],
               "droppedEventsCount":3,
               "links":[{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708",
                         "attributes":[{"key":"l","value":{"boolValue":true}}],"flags":256}],
               "status":{"code":"STATUS_CODE_ERROR","message":"bad"}},
              {"kind":3,"status":{"code":"1"}}
            ]}]}]}"#,
        );
        assert_eq!(read.len(), 2);
        assert_eq!(
            shown_row(&read[0].0),
            json!({
                "traceId": "5b8efff798038103d269b633813fc60c",
                "spanId": "eee19b7ec3c1b174",
                "parentSpanId": "0102030405060708",
                "flags": 1,
                "name": "op",
                "kind": 2,
                "startTime": 1_700_000_000_000_000_i64,
                "endTime": 1_700_000_000_000_001_i64,
                "attributes": {"a": 5},
                "droppedAttributesCount": 2,
                "events": [{"time": "datetime:1700000000000000", "name": "e", "droppedAttributesCount": 1}],
                "droppedEventsCount": 3,
                "links": [{
                    "traceId": "0102030405060708090a0b0c0d0e0f10",
                    "spanId": "0102030405060708",
                    "attributes": {"l": true},
                    "flags": 256,
                }],
                "statusCode": 2,
                "statusMessage": "bad",
            })
        );
        assert_eq!(shown_row(&read[1].0), json!({"kind": 3, "statusCode": 1}));
        for (_, filed) in &read {
            assert_eq!(filed_as(filed, RESOURCE), Some(json!({"host": "h1"})));
            assert_eq!(scope_of(filed), Some(json!({"name": "s"})));
        }
    }

    #[test]
    fn json_reads_protobuf_names_and_skips_unknown_fields() {
        let read = json(
            r#"{"resource_spans":[{"scope_spans":[{"spans":[
                {"trace_id":"0102030405060708090a0b0c0d0e0f10","parent_span_id":"0807060504030201",
                 "trace_state":"t","start_time_unix_nano":"5000","dropped_links_count":7,
                 "events":[{"time_unix_nano":"2000","dropped_attributes_count":"1","future":true}],
                 "links":[{"trace_state":"x","dropped_attributes_count":2}],
                 "status":{"message":"m","deprecated_code":1},
                 "future":{"nested":[1,2]}}
               ],"schemaUrl":"u"}],"surprise":1}],"partialSuccess":{}}"#,
        );
        assert_eq!(
            shown_row(&read[0].0),
            json!({
                "traceId": "0102030405060708090a0b0c0d0e0f10",
                "parentSpanId": "0807060504030201",
                "traceState": "t",
                "startTime": 5,
                "events": [{"time": "datetime:2", "droppedAttributesCount": 1}],
                "links": [{"traceState": "x", "droppedAttributesCount": 2}],
                "droppedLinksCount": 7,
                "statusMessage": "m",
            })
        );
        assert!(read[0].1.is_empty());
    }

    #[test]
    fn an_event_time_is_a_datetime() {
        let read = read(&one_span(vec![Span {
            events: vec![Event {
                time_unix_nano: 3_000,
                ..Default::default()
            }],
            ..Default::default()
        }]));
        let Some(Cell::Variant(events)) = &read[0].0.0[11] else {
            panic!("events are a variant")
        };
        assert_eq!(shown(events), json!([{"time": "datetime:3"}]));
    }

    #[test]
    fn records_come_in_the_order_of_their_resources_and_scopes() {
        let request = ExportTraceServiceRequest {
            resource_spans: ["r1", "r2"]
                .map(|r| ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![kv("r", text(r))],
                        dropped_attributes_count: 0,
                    }),
                    scope_spans: ["s1", "s2"]
                        .map(|s| ScopeSpans {
                            scope: Some(InstrumentationScope {
                                name: s.to_string(),
                                ..Default::default()
                            }),
                            spans: ["a", "b"]
                                .map(|n| Span {
                                    name: format!("{r}{s}{n}"),
                                    ..Default::default()
                                })
                                .to_vec(),
                        })
                        .to_vec(),
                })
                .to_vec(),
        };
        let read = read(&request);
        let names: Vec<Value> = read.iter().map(|(r, _)| shown_row(r)["name"].clone()).collect();
        assert_eq!(
            names,
            ["r1s1a", "r1s1b", "r1s2a", "r1s2b", "r2s1a", "r2s1b", "r2s2a", "r2s2b"]
        );
        let filed: Vec<(Value, Value)> = (read.iter())
            .map(|(_, f)| {
                (
                    filed_as(f, RESOURCE).unwrap()["r"].clone(),
                    scope_of(f).unwrap()["name"].clone(),
                )
            })
            .collect();
        assert_eq!(filed[2], ("r1".into(), "s2".into()));
        assert_eq!(filed[5], ("r2".into(), "s1".into()));
        assert_eq!(filed[7], ("r2".into(), "s2".into()));
        let shared: Vec<bool> = read.windows(2).map(|w| Arc::ptr_eq(&w[0].1, &w[1].1)).collect();
        assert_eq!(
            shared,
            [true, false, true, false, true, false, true],
            "the records of one scope share what they are filed under"
        );
    }

    #[test]
    fn a_request_with_no_spans_reads_as_none() {
        assert!(all(Export::protobuf(Signal::Traces, &[]).unwrap()).is_empty());
        assert!(json("{}").is_empty());
        assert!(json(r#"{"resourceSpans":null}"#).is_empty());
        assert!(json(r#"{"resourceSpans":[{"scopeSpans":[{},{"spans":[]}]},{}]}"#).is_empty());
    }

    #[test]
    fn a_body_that_does_not_read_is_refused() {
        let protobuf = |body: &[u8]| Export::protobuf(Signal::Traces, body).unwrap_err();
        assert!(protobuf(b"\x0a\xff\xff").contains("traces"), "a length past the body");
        protobuf(b"\x0a\x02\x12\x00\xff");
        let refused = |body: &str| Export::json(Signal::Traces, body.as_bytes()).unwrap_err();
        let span = |fields: &str| format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{{fields}}}]}}]}}]}}"#);
        refused("");
        refused("not json");
        refused("[]");
        refused(r#"{"resourceSpans":{}}"#);
        refused(&span(r#""startTimeUnixNano":"soon""#));
        refused(&span(r#""endTimeUnixNano":-1"#));
        refused(&span(r#""spanId":"xyz""#));
        refused(&span(r#""flags":4294967296"#));
        refused(&span(r#""name":1"#));
        refused(&span(r#""events":{}"#));
        refused(&span(r#""events":[{"timeUnixNano":"later"}]"#));
        refused(&span(r#""links":[{"traceId":"zz"}]"#));
        refused(&span(r#""status":"ok""#));
        assert!(
            refused(&span(r#""kind":"SPAN_KIND_SIDEWAYS""#)).contains("kind"),
            "an unknown enum name"
        );
        refused(&span(r#""status":{"code":"STATUS_CODE_MAYBE"}"#));
        refused(&span(r#""kind":2147483648"#));
        let late = r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"name":"ok"}]}]},
            {"scopeSpans":[{"spans":[{"traceState":3}]}]}]}"#;
        assert!(
            refused(late).contains("traceState"),
            "a fault after a good span refuses the whole body"
        );
    }

    /// What a request holds decoded is read from its wire form: each repeated message its struct twice over, the first
    /// four times, events, links and their attributes among them.
    #[test]
    fn a_request_holds_its_structs_decoded() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", text("api"))],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "lib".into(),
                        ..Default::default()
                    }),
                    spans: vec![
                        Span {
                            attributes: vec![kv("a", Any::Int(1)), kv("b", Any::Int(2))],
                            events: vec![Event {
                                attributes: vec![kv("e", Any::Int(3))],
                                ..Default::default()
                            }],
                            links: vec![
                                Link {
                                    attributes: vec![kv("l", Any::Int(4))],
                                    ..Default::default()
                                },
                                Link::default(),
                            ],
                            status: Some(Status {
                                message: "m".into(),
                                code: 1,
                            }),
                            ..Default::default()
                        },
                        Span::default(),
                    ],
                }],
            }],
        };
        let body = request.encode_to_vec();
        let size = decoded_size(Signal::Traces, &body).unwrap();
        // A vector's first element takes room for four, and each after it two.
        let structs = 4 * size_of::<ResourceSpans>()
            + 4 * size_of::<ScopeSpans>()
            + (4 + 2) * size_of::<Span>()
            + 4 * size_of::<Event>()
            + (4 + 2) * size_of::<Link>()
            + (4 + (4 + 2) + 4 + 4) * size_of::<KeyValue>();
        assert_eq!(size, body.len() + structs);
        assert_eq!(
            Export::protobuf(Signal::Traces, &body).unwrap().decoded_size(),
            size,
            "a decoded request holds what its body said"
        );
        assert!(
            decoded_size(Signal::Traces, &body[..body.len() - 1]).is_err(),
            "a cut-off body is refused"
        );
    }
}
