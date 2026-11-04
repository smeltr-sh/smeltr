//! A schema of one OpenTelemetry signal's records, as an OTLP receiver gives them and an OTLP destination reads them
//! (`plan::topology::SourceMeta::otlp`, `io::otlp::export`): a log record, a metric's data point or a span, with the
//! `resource` and `scope` it came under. A struct is an object row with its fields as rows of their own under it. A
//! field is required where the destination refuses a record without it, and optional otherwise, as OTLP leaves
//! every other field unset at will.

use crate::config_file::{Requirement, SchemaFieldDto};
use crate::core::column::DataType;
use crate::plan::topology::{Signal, SourceMeta};

/// The fields of `signal`'s records.
pub fn read(signal: Signal) -> Vec<SchemaFieldDto> {
    let required: &[&str] = match signal {
        Signal::Logs => &[],
        Signal::Metrics => &["type"],
        Signal::Traces => &["traceId", "spanId"],
    };
    let meta = SourceMeta::otlp(signal);
    let mut fields = Vec::new();
    for (name, ty) in meta.record().iter() {
        let requirement = match required.contains(&name.as_ref()) {
            true => Requirement::Required,
            false => Requirement::Optional,
        };
        rows(&mut fields, name, ty, requirement);
    }
    let object = DataType::Struct(Vec::new(), true);
    let scope = DataType::Struct(
        vec![
            ("name".to_string(), DataType::Str),
            ("version".to_string(), DataType::Str),
            ("attributes".to_string(), object.clone()),
        ],
        false,
    );
    rows(&mut fields, "resource", &object, Requirement::Optional);
    rows(&mut fields, "scope", &scope, Requirement::Optional);
    fields
}

/// `ty` at `path`, and, where it is a struct or an array of them, each of its fields under it.
fn rows(fields: &mut Vec<SchemaFieldDto>, path: &str, ty: &DataType, requirement: Requirement) {
    let (own, members) = match ty {
        DataType::Struct(members, _) => (DataType::Struct(Vec::new(), true), Some(members)),
        DataType::Array(element) => match element.as_ref() {
            DataType::Struct(members, _) => (
                DataType::Array(Box::new(DataType::Struct(Vec::new(), true))),
                Some(members),
            ),
            _ => (ty.clone(), None),
        },
        _ => (ty.clone(), None),
    };
    fields.push(SchemaFieldDto {
        field: path.to_string(),
        ty: own.spell(),
        requirement,
        description: String::new(),
        example: String::new(),
        elasticsearch: None,
    });
    for (name, member) in members.into_iter().flatten() {
        rows(fields, &format!("{path}.{name}"), member, Requirement::Optional);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(signal: Signal) -> Vec<(String, String, Requirement)> {
        read(signal)
            .into_iter()
            .map(|f| (f.field, f.ty, f.requirement))
            .collect()
    }

    /// Each signal's schema reads as a bundle's would, and holds every field its receiver's record has.
    #[test]
    fn each_signal_reads_as_a_schema_holding_its_record() {
        for signal in [Signal::Logs, Signal::Metrics, Signal::Traces] {
            let fields = read(signal);
            let dto = crate::config_file::SchemaDto {
                id: format!("otlp-{}", signal.name()),
                fields: fields.clone(),
                ..Default::default()
            };
            crate::schema::read(&dto).unwrap_or_else(|e| panic!("{}: {e}", signal.name()));
            for (name, _) in SourceMeta::otlp(signal).record().iter() {
                assert!(fields.iter().any(|f| f.field == *name), "{}: {name}", signal.name());
            }
        }
    }

    /// A receiver's record, passed through as it came, keeps its signal's schema: no field misfits.
    #[test]
    fn a_receivers_record_keeps_its_signals_schema() {
        for signal in [Signal::Logs, Signal::Metrics, Signal::Traces] {
            let dto = crate::config_file::SchemaDto {
                id: "otlp".into(),
                fields: read(signal),
                ..Default::default()
            };
            let schema = crate::schema::read(&dto).unwrap();
            let meta = SourceMeta::otlp(signal);
            let record = DataType::Struct(
                meta.record_fields()
                    .into_iter()
                    .map(|(n, t)| (n.to_string(), t))
                    .collect(),
                false,
            );
            assert_eq!(schema.check(&record), [], "{}", signal.name());
        }
    }

    /// What the destination refuses a record without is required, and the resource and scope it came under follow.
    #[test]
    fn a_field_the_destination_needs_is_required() {
        let traces = named(Signal::Traces);
        assert!(traces.contains(&("traceId".into(), "str".into(), Requirement::Required)));
        assert!(traces.contains(&("spanId".into(), "str".into(), Requirement::Required)));
        assert!(traces.contains(&("name".into(), "str".into(), Requirement::Optional)));
        let metrics = named(Signal::Metrics);
        assert!(metrics.contains(&("type".into(), "str".into(), Requirement::Required)));
        let logs = named(Signal::Logs);
        assert!(logs.iter().all(|(_, _, r)| *r == Requirement::Optional));
        let tail: Vec<_> = logs[logs.len() - 5..]
            .iter()
            .map(|(f, t, _)| format!("{f}: {t}"))
            .collect();
        assert_eq!(
            tail,
            [
                "resource: {...}",
                "scope: {...}",
                "scope.name: str",
                "scope.version: str",
                "scope.attributes: {...}"
            ]
        );
    }

    /// A struct is an object row with its fields under it, and an array of them an array of objects.
    #[test]
    fn a_struct_is_an_object_row_and_its_fields() {
        let metrics = named(Signal::Metrics);
        let at = |path: &str| metrics.iter().find(|(f, _, _)| f == path).map(|(_, t, _)| t.as_str());
        assert_eq!(at("positive"), Some("{...}"));
        assert_eq!(at("positive.offset"), Some("i64"));
        assert_eq!(at("positive.bucketCounts"), Some("array<f64>"));
        assert_eq!(at("quantileValues"), Some("array<{...}>"));
        assert_eq!(at("quantileValues.quantile"), Some("f64"));
    }
}
