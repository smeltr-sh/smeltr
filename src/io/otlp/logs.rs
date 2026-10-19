//! OTLP logs: each `LogRecord` one record of `SourceMeta::OTLP_LOGS`.

use std::sync::Arc;

use prost::Message as _;

use super::json::{self, Reader, each, fields};
use super::{
    AnyValue, Cell, Cursor, Filed, InstrumentationScope, KeyValue, Resource, Row, Shape, id, int, micros, pairs,
    put_any, text, variant_of,
};

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct ExportLogsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    resource_logs: Vec<ResourceLogs>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ResourceLogs {
    #[prost(message, optional, tag = "1")]
    resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    scope_logs: Vec<ScopeLogs>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ScopeLogs {
    #[prost(message, optional, tag = "1")]
    scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    log_records: Vec<LogRecord>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct LogRecord {
    #[prost(fixed64, tag = "1")]
    time_unix_nano: u64,
    #[prost(fixed64, tag = "11")]
    observed_time_unix_nano: u64,
    /// OTLP's severity number enum, read as its number.
    #[prost(int32, tag = "2")]
    severity_number: i32,
    #[prost(string, tag = "3")]
    severity_text: String,
    #[prost(message, optional, tag = "5")]
    body: Option<AnyValue>,
    #[prost(message, repeated, tag = "6")]
    attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "7")]
    dropped_attributes_count: u32,
    #[prost(fixed32, tag = "8")]
    flags: u32,
    #[prost(bytes = "vec", tag = "9")]
    trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "10")]
    span_id: Vec<u8>,
    #[prost(string, tag = "12")]
    event_name: String,
}

/// The messages of a logs request, as `walk` reads them.
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Request,
    ResourceLogs,
    ScopeLogs,
    LogRecord,
}

impl Shape for Kind {
    const ALL: &'static [Kind] = &[Kind::Request, Kind::ResourceLogs, Kind::ScopeLogs, Kind::LogRecord];

    fn size(self) -> usize {
        match self {
            Kind::Request => size_of::<ExportLogsServiceRequest>(),
            Kind::ResourceLogs => size_of::<ResourceLogs>(),
            Kind::ScopeLogs => size_of::<ScopeLogs>(),
            Kind::LogRecord => size_of::<LogRecord>(),
        }
    }

    fn field(self, tag: u32) -> Option<(super::Kind<Kind>, bool)> {
        use super::Kind as K;
        Some(match (self, tag) {
            (Kind::Request, 1) => (K::Signal(Kind::ResourceLogs), true),
            (Kind::ResourceLogs, 1) => (K::Resource, false),
            (Kind::ResourceLogs, 2) => (K::Signal(Kind::ScopeLogs), true),
            (Kind::ScopeLogs, 1) => (K::Scope, false),
            (Kind::ScopeLogs, 2) => (K::Signal(Kind::LogRecord), true),
            (Kind::LogRecord, 5) => (K::AnyValue, false),
            (Kind::LogRecord, 6) => (K::KeyValue, true),
            _ => return None,
        })
    }
}

/// A decoded logs request, its log records rendered one at a time as they are taken.
#[derive(Debug)]
pub struct Logs {
    request: ExportLogsServiceRequest,
    at: Cursor,
}

impl Logs {
    fn new(request: ExportLogsServiceRequest) -> Logs {
        Logs {
            request,
            at: Cursor::default(),
        }
    }

    pub(super) fn protobuf(body: &[u8]) -> Result<Logs, prost::DecodeError> {
        ExportLogsServiceRequest::decode(body).map(Logs::new)
    }

    pub(super) fn json(r: &mut Reader) -> Result<Logs, String> {
        let mut request = ExportLogsServiceRequest::default();
        fields(r, "the request", |key, r| {
            match key {
                "resourceLogs" | "resource_logs" => request.resource_logs = each(r, "resourceLogs", resource_logs)?,
                _ => return Ok(false),
            }
            Ok(true)
        })?;
        Ok(Logs::new(request))
    }

    pub(super) fn encoded(&self) -> Vec<u8> {
        self.request.encode_to_vec()
    }

    pub(super) fn next_record(&mut self) -> Option<(Row, Arc<Filed>)> {
        let (record, filed) = self.at.next(
            &mut self.request.resource_logs,
            |r| (r.resource.as_ref(), &mut r.scope_logs),
            |s| (s.scope.as_ref(), &mut s.log_records),
        )?;
        Some((row(record), filed))
    }
}

/// A log record's fields, in `SourceMeta::OTLP_LOGS`'s order.
fn row(log: LogRecord) -> Row {
    Row(vec![
        micros(log.time_unix_nano),
        micros(log.observed_time_unix_nano),
        int(log.severity_number),
        text(log.severity_text),
        // A body with no value is null, as no body is.
        (log.body)
            .filter(|b| b.value.is_some())
            .map(|b| Cell::Variant(variant_of(|out| put_any(out, &b)))),
        pairs(&log.attributes),
        id(&log.trace_id, 16).map(Cell::Text),
        id(&log.span_id, 8).map(Cell::Text),
        int(log.flags),
        text(log.event_name),
        int(log.dropped_attributes_count),
    ])
}

fn resource_logs(r: &mut Reader) -> Result<ResourceLogs, String> {
    let mut resource = ResourceLogs::default();
    fields(r, "a `resourceLogs` item", |key, r| {
        match key {
            "resource" => resource.resource = Some(json::resource(r)?),
            "scopeLogs" | "scope_logs" => resource.scope_logs = each(r, "scopeLogs", scope_logs)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(resource)
}

fn scope_logs(r: &mut Reader) -> Result<ScopeLogs, String> {
    let mut scope = ScopeLogs::default();
    fields(r, "a `scopeLogs` item", |key, r| {
        match key {
            "scope" => scope.scope = Some(json::scope(r)?),
            "logRecords" | "log_records" => scope.log_records = each(r, "logRecords", log_record)?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(scope)
}

fn log_record(r: &mut Reader) -> Result<LogRecord, String> {
    let mut log = LogRecord::default();
    fields(r, "a `logRecords` item", |key, r| {
        match key {
            "timeUnixNano" | "time_unix_nano" => log.time_unix_nano = json::int(r, "timeUnixNano")?,
            "observedTimeUnixNano" | "observed_time_unix_nano" => {
                log.observed_time_unix_nano = json::int(r, "observedTimeUnixNano")?
            }
            "severityNumber" | "severity_number" => log.severity_number = json::int(r, "severityNumber")?,
            "severityText" | "severity_text" => log.severity_text = json::text(r, "severityText")?,
            "body" => log.body = Some(json::any_value(r)?),
            "attributes" => log.attributes = each(r, key, json::key_value)?,
            "droppedAttributesCount" | "dropped_attributes_count" => {
                log.dropped_attributes_count = json::int(r, "droppedAttributesCount")?
            }
            "flags" => log.flags = json::int(r, key)?,
            "traceId" | "trace_id" => log.trace_id = json::id(r, "traceId")?,
            "spanId" | "span_id" => log.span_id = json::id(r, "spanId")?,
            "eventName" | "event_name" => log.event_name = json::text(r, "eventName")?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(log)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::testing::{all, filed_as, kv, row_json, scope_of, shown, text};
    use super::super::{Any, Export, RESOURCE, Signal, decoded_size, variant, variant_of};
    use super::*;

    fn read(request: &ExportLogsServiceRequest) -> Vec<(Row, Arc<Filed>)> {
        all(Export::protobuf(Signal::Logs, &request.encode_to_vec()).unwrap())
    }

    fn json(body: &str) -> Vec<(Row, Arc<Filed>)> {
        all(Export::json(Signal::Logs, body.as_bytes()).unwrap())
    }

    fn shown_row(row: &Row) -> Value {
        row_json(Signal::Logs, row)
    }

    #[test]
    fn a_protobuf_request_reads_back_as_its_rows() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", text("api"))],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "lib".to_string(),
                        version: "1.2".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_001_999,
                        observed_time_unix_nano: 1_700_000_000_000_002_000,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(text("hello")),
                        }),
                        attributes: vec![
                            kv("n", Any::Int(-3)),
                            kv("ok", Any::Bool(true)),
                            kv("bytes", Any::Bytes(vec![0, 255])),
                            kv("nan", Any::Double(f64::NAN)),
                            kv("whole", Any::Double(2.0)),
                        ],
                        dropped_attributes_count: 2,
                        flags: 1,
                        trace_id: (0..16).collect(),
                        span_id: vec![0xab; 8],
                        event_name: "login".to_string(),
                    }],
                }],
            }],
        };
        let mut read = read(&request);
        assert_eq!(read.len(), 1);
        let (row, filed) = read.remove(0);
        let Some(Cell::Variant(attributes)) = &row.0[5] else {
            panic!("attributes are a variant")
        };
        assert!(
            matches!(
                variant::read(variant::member(attributes, "whole").unwrap()),
                variant::Value::F64(2.0)
            ),
            "a whole double stays a double"
        );
        assert_eq!(
            shown_row(&row),
            json!({
                "time": 1_700_000_000_000_001_i64,
                "observedTime": 1_700_000_000_000_002_i64,
                "severityNumber": 9,
                "severityText": "INFO",
                "body": "hello",
                "attributes": {"n": -3, "ok": true, "bytes": "bytes:00ff", "nan": "f64:NaN", "whole": 2.0},
                "traceId": "000102030405060708090a0b0c0d0e0f",
                "spanId": "abababababababab",
                "flags": 1,
                "eventName": "login",
                "droppedAttributesCount": 2,
            })
        );
        assert_eq!(filed_as(&filed, RESOURCE), Some(json!({"service.name": "api"})));
        assert_eq!(scope_of(&filed), Some(json!({"name": "lib", "version": "1.2"})));
    }

    #[test]
    fn a_row_is_null_where_its_record_carries_nothing() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource::default()),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    log_records: vec![
                        LogRecord {
                            body: Some(AnyValue { value: None }),
                            ..Default::default()
                        },
                        LogRecord {
                            time_unix_nano: 999,
                            ..Default::default()
                        },
                    ],
                }],
            }],
        };
        let read = read(&request);
        assert_eq!(shown_row(&read[0].0), json!({}));
        assert_eq!(read[0].0.size(), 0);
        assert!(read[0].1.is_empty(), "an empty resource and scope file nothing");
        assert_eq!(shown_row(&read[1].0), json!({}), "less than a microsecond is no time");
    }

    #[test]
    fn json_reads_ints_as_text_or_numbers_and_ids_as_hex() {
        let read = json(
            r#"{"resourceLogs":[{"resource":{"attributes":[{"key":"host","value":{"stringValue":"h1"}}]},
            "scopeLogs":[{"scope":{"name":"s","attributes":[{"key":"k","value":{"intValue":"7"}}]},
            "logRecords":[
              {"timeUnixNano":"1700000000000000000","observedTimeUnixNano":1700000000000001000,
               "severityNumber":17,"traceId":"5B8EFFF798038103D269B633813FC60C","spanId":"eee19b7ec3c1b174",
               "body":{"intValue":"9007199254740993"},
               "attributes":[{"key":"a","value":{"intValue":5}},{"key":"d","value":{"doubleValue":"Infinity"}},
                             {"key":"b","value":{"bytesValue":"AP8"}},{"key":"f","value":{"doubleValue":3}}]},
              {"body":{"doubleValue":1.5}}
            ]}]}]}"#,
        );
        assert_eq!(read.len(), 2);
        assert_eq!(
            shown_row(&read[0].0),
            json!({
                "time": 1_700_000_000_000_000_i64,
                "observedTime": 1_700_000_000_000_001_i64,
                "severityNumber": 17,
                "traceId": "5b8efff798038103d269b633813fc60c",
                "spanId": "eee19b7ec3c1b174",
                "body": 9007199254740993_i64,
                "attributes": {"a": 5, "d": "f64:inf", "b": "bytes:00ff", "f": 3.0},
            })
        );
        assert_eq!(
            read[1].0.0[4],
            Some(Cell::Variant(variant_of(|out| variant::put_f64(out, 1.5))))
        );
        for (_, filed) in &read {
            assert_eq!(filed_as(filed, RESOURCE), Some(json!({"host": "h1"})));
            assert_eq!(scope_of(filed), Some(json!({"name": "s", "attributes": {"k": 7}})));
        }
    }

    #[test]
    fn json_reads_protobuf_names_and_skips_unknown_fields() {
        let read = json(
            r#"{"resource_logs":[{"scope_logs":[{"log_records":[
                {"time_unix_nano":"5000","severity_text":"WARN","trace_id":"0102030405060708090a0b0c0d0e0f10","event_name":"e",
                 "body":{"kvlist_value":{"values":[{"key":"x","value":{"bool_value":false}}]}},
                 "future":{"nested":[1,2]}}
               ],"schemaUrl":"u"}],"surprise":1}],"partialSuccess":{}}"#,
        );
        assert_eq!(
            shown_row(&read[0].0),
            json!({
                "time": 5,
                "severityText": "WARN",
                "traceId": "0102030405060708090a0b0c0d0e0f10",
                "eventName": "e",
                "body": {"x": false},
            })
        );
        assert!(read[0].1.is_empty());
    }

    #[test]
    fn json_reads_a_oneof_the_same_in_any_order_and_a_null_field_as_absent() {
        for body in [
            r#"{"intValue":"7","stringValue":"s"}"#,
            r#"{"stringValue":"s","intValue":"7"}"#,
            r#"{"stringValue":"s","boolValue":null}"#,
        ] {
            let read = json(&format!(
                r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[{{"body":{body},"severityText":null}}]}}]}}]}}"#
            ));
            assert_eq!(shown_row(&read[0].0), json!({"body": "s"}), "{body}");
        }
    }

    #[test]
    fn json_refuses_what_follows_the_request() {
        Export::json(Signal::Logs, br#"{"resourceLogs":[]} {}"#).unwrap_err();
        Export::json(Signal::Logs, b"{\"resourceLogs\":[]}\n").unwrap();
    }

    #[test]
    fn nested_values_keep_their_types_and_the_last_of_a_repeated_key_wins() {
        let read = json(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"kvlistValue":{"values":[
                {"key":"list","value":{"arrayValue":{"values":[{"stringValue":"a"},{},{"doubleValue":1},
                    {"kvlistValue":{"values":[{"key":"deep","value":{"arrayValue":{}}}]}}]}}},
                {"key":"dup","value":{"intValue":"1"}},
                {"key":"none"},
                {"key":"dup","value":{"intValue":"2"}}
            ]}}}]}]}]}"#,
        );
        let Some(Cell::Variant(body)) = &read[0].0.0[4] else {
            panic!("a body is a variant")
        };
        assert_eq!(shown(body), json!({"list": ["a", null, 1.0, {"deep": []}], "dup": 2}));
        let keys: Vec<&str> = match variant::read(body) {
            variant::Value::Object(entries) => entries.map(|(k, _)| k).collect(),
            _ => unreachable!(),
        };
        assert_eq!(keys, ["list", "dup"], "a repeated key keeps its first place");
    }

    #[test]
    fn records_come_in_the_order_of_their_resources_and_scopes() {
        let request = ExportLogsServiceRequest {
            resource_logs: ["r1", "r2"]
                .map(|r| ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![kv("r", text(r))],
                        dropped_attributes_count: 0,
                    }),
                    scope_logs: ["s1", "s2"]
                        .map(|s| ScopeLogs {
                            scope: Some(InstrumentationScope {
                                name: s.to_string(),
                                ..Default::default()
                            }),
                            log_records: ["a", "b"]
                                .map(|n| LogRecord {
                                    event_name: format!("{r}{s}{n}"),
                                    ..Default::default()
                                })
                                .to_vec(),
                        })
                        .to_vec(),
                })
                .to_vec(),
        };
        let read = read(&request);
        let names: Vec<Value> = read.iter().map(|(r, _)| shown_row(r)["eventName"].clone()).collect();
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
    fn a_request_with_no_records_reads_as_none() {
        assert!(all(Export::protobuf(Signal::Logs, &[]).unwrap()).is_empty());
        assert!(json("{}").is_empty());
        assert!(json(r#"{"resourceLogs":null}"#).is_empty());
        assert!(json(r#"{"resourceLogs":[{"scopeLogs":[{},{"logRecords":[]}]},{}]}"#).is_empty());
    }

    #[test]
    fn a_body_that_does_not_read_is_refused() {
        let protobuf = |body: &[u8]| Export::protobuf(Signal::Logs, body).unwrap_err();
        assert!(protobuf(b"\x0a\xff\xff").contains("logs"), "a length past the body");
        protobuf(b"\x0a\x02\x12\x00\xff");
        let refused = |body: &str| Export::json(Signal::Logs, body.as_bytes()).unwrap_err();
        refused("");
        refused("not json");
        refused("[]");
        refused(r#"{"resourceLogs":{}}"#);
        refused(r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"soon"}]}]}]}"#);
        refused(r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":-1}]}]}]}"#);
        refused(r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"traceId":"xyz"}]}]}]}"#);
        refused(r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"flags":4294967296}]}]}]}"#);
        refused(r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":1}}]}]}]}"#);
        refused(r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"bytesValue":"!!"}}]}]}]}"#);
        let late = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"ok"}}]}]},
            {"scopeLogs":[{"logRecords":[{"severityText":3}]}]}]}"#;
        assert!(
            refused(late).contains("severityText"),
            "a fault after a good record refuses the whole body"
        );
    }

    /// What a request holds decoded is read from its wire form: each repeated message its struct twice over, the first
    /// four times.
    #[test]
    fn a_request_holds_its_structs_decoded() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", Any::String("api".into()))],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "lib".into(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord::default(); 3],
                }],
            }],
        };
        let body = request.encode_to_vec();
        let size = decoded_size(Signal::Logs, &body).unwrap();
        // A vector's first element takes room for four, and each after it two.
        let structs = 4 * size_of::<ResourceLogs>()
            + 4 * size_of::<ScopeLogs>()
            + (4 + 2 + 2) * size_of::<LogRecord>()
            + 4 * size_of::<KeyValue>();
        assert_eq!(size, body.len() + structs);
        assert_eq!(
            Export::protobuf(Signal::Logs, &body).unwrap().decoded_size(),
            size,
            "a decoded request holds what its body said"
        );
        assert!(
            decoded_size(Signal::Logs, &body[..body.len() - 1]).is_err(),
            "a cut-off body is refused"
        );
    }
}
