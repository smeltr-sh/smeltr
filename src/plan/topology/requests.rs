//! The payloads of an OTLP or remote_write receiver, each one request, decoded into the records it holds.

use std::sync::Arc;

use super::*;
use crate::io::{RecordChunk, SourceRecord, hec, otlp, prometheus};

/// What one request decodes within at most, as a receiver's inbox holds at most that of what it takes.
const REQUEST_BYTES: usize = crate::io::receiver::INBOX_BYTES;

impl SourceMeta {
    /// `payloads` as the records they hold, where this source's payloads are requests and these are payloads rather
    /// than a sample's records: those records, their rows of one chunk, each filed under its resource and scope where
    /// its protocol has them; each one's payload; and what was noted. A payload that does not decode is one record of
    /// nulls, noted `Unparsable` with why.
    pub fn decode_requests(
        &self,
        payloads: &[SourceRecord],
        pool: &mut crate::core::buffer::BufferPool,
    ) -> Option<(Vec<SourceRecord>, Vec<u32>, Vec<Noted>)> {
        if !matches!(self.format, Format::Otlp(_) | Format::RemoteWrite) || payloads.first()?.bytes().is_none() {
            return None;
        }
        // A request's records copy what they share, as a series' labels into each of its samples, so they may hold
        // far more than the request: they are held to the budget as they are made.
        let _scratch = crate::core::buffer::Scratch::begin(pool.budget());
        let (mut rows, mut filed, mut payload, mut noted) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let unfiled: Arc<hec::Filed> = Arc::default();
        for (at, record) in payloads.iter().enumerate() {
            let body = record.bytes().unwrap_or_default();
            let content_type = super::filed(record, hec::CONTENT_TYPE).and_then(|v| std::str::from_utf8(v).ok());
            let before = rows.len();
            let read = match self.format {
                Format::Otlp(signal) => otlp::Encoding::of(content_type)
                    .ok_or_else(|| format!("an export is not sent as {}", content_type.unwrap_or("nothing")))
                    .and_then(|encoding| otlp::Export::read(signal, encoding, body, REQUEST_BYTES))
                    .map(|mut export| {
                        while let Some((row, under)) = export.next_record() {
                            rows.push(row);
                            filed.push(under);
                            crate::core::buffer::check();
                        }
                    }),
                _ => prometheus::readable(content_type)
                    .and_then(|version| prometheus::Samples::read(body, version, REQUEST_BYTES))
                    .map(|mut samples| {
                        while let Some(row) = samples.next_record() {
                            rows.push(row);
                            crate::core::buffer::check();
                        }
                    }),
            };
            if let Err(why) = read {
                rows.push(otlp::Row(vec![None; self.record.len()]));
                self.note_read(&mut noted, before as u32, Some(&why), std::iter::empty());
            }
            // Only OTLP files its records, under their resource and scope.
            if matches!(self.format, Format::Otlp(_)) {
                filed.resize(rows.len(), unfiled.clone());
            }
            payload.resize(rows.len(), at as u32);
        }
        let mut chunk = RecordChunk::new(otlp::columns(&rows, &self.record, pool), Vec::new());
        if !filed.is_empty() {
            chunk = chunk.filed_each(filed);
        }
        let chunk = Arc::new(chunk);
        let records = (payload.iter().enumerate())
            .map(|(row, &at)| {
                let from = &payloads[at as usize];
                SourceRecord {
                    chunk: chunk.clone(),
                    row: row as u32,
                    offset: from.offset,
                    partition: from.partition,
                    topic: from.topic.clone(),
                    headers: None,
                }
            })
            .collect();
        Some((records, payload, noted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A payload of `body`, sent as `content_type`.
    fn payload(body: &str, content_type: &str) -> SourceRecord {
        SourceRecord {
            chunk: RecordChunk::of_bytes(body),
            row: 0,
            offset: 0,
            partition: 0,
            topic: "10.0.0.1".into(),
            headers: Some(vec![(hec::CONTENT_TYPE.into(), content_type.as_bytes().to_vec())]),
        }
    }

    /// Each request decodes into its records, each filed under its own resource and named by its payload; one that
    /// does not decode is one record that says why.
    #[test]
    fn each_request_decodes_into_its_records_filed_under_their_resources() {
        let resource = |r| {
            format!(
                r#"{{"resource": {{"attributes": [{{"key": "r", "value": {{"stringValue": "{r}"}}}}]}},
                    "scopeLogs": [{{"scope": {{"name": "s"}}, "logRecords": [{{}}, {{}}]}}]}}"#
            )
        };
        let body = format!(r#"{{"resourceLogs": [{}, {}]}}"#, resource("r1"), resource("r2"));
        let json = otlp::Encoding::Json.content_type();
        let payloads = [payload(&body, json), payload("not an export", json)];
        let meta = SourceMeta::OTLP_LOGS;
        let mut pool = crate::core::buffer::BufferPool::new();
        let decoded = meta.decode(&payloads, None::<&[&str]>, None, None, &mut pool);
        assert_eq!(decoded.payload, [0, 0, 0, 0, 1]);
        let at = |name| meta.source_fields().iter().position(|(n, _)| *n == name).unwrap();
        let resource = decoded.batch.peek(meta.source_slot(at("__source.resource")));
        let resources = resource.varlen().unwrap();
        assert_eq!(resources.get(0), resources.get(1));
        assert_ne!(resources.get(1), resources.get(2));
        assert!(!resource.is_valid(4), "an export that does not decode has no resource");
        let [(row, _, ExceptionKind::Unparsable, Some(_))] = &decoded.noted[..] else {
            panic!("{:?}", decoded.noted)
        };
        assert_eq!(*row, 4);
        decoded.batch.reclaim(&mut pool);
    }

    #[test]
    fn a_remote_write_request_that_does_not_decode_is_one_record_that_says_why() {
        let sent = payload("\u{ff}", "application/x-protobuf");
        let meta = SourceMeta::prometheus_receiver();
        let mut pool = crate::core::buffer::BufferPool::new();
        let decoded = meta.decode(&[sent], None::<&[&str]>, None, None, &mut pool);
        assert_eq!(decoded.payload, [0]);
        assert!(matches!(
            decoded.noted[..],
            [(0, _, ExceptionKind::Unparsable, Some(_))]
        ));
        decoded.batch.reclaim(&mut pool);
    }

    /// A data point's arrays and structs are columns of their declared types, holding what the decoder wrote.
    #[test]
    fn a_metric_point_s_arrays_and_structs_are_typed_columns() {
        use crate::core::column::DataType;
        let body = r#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [
            {"name": "h", "histogram": {"dataPoints": [{"bucketCounts": ["1", "2"], "explicitBounds": [1.5]}]}},
            {"name": "e", "exponentialHistogram": {"dataPoints": [{"positive": {"offset": 1, "bucketCounts": ["3"]}}]}},
            {"name": "s", "summary": {"dataPoints": [{"quantileValues": [{"quantile": 0.5, "value": 2}]}]}}
        ]}]}]}"#;
        let meta = SourceMeta::otlp(Signal::Metrics);
        let mut export = otlp::Export::json(Signal::Metrics, body.as_bytes()).unwrap();
        let rows: Vec<otlp::Row> = std::iter::from_fn(|| export.next_record())
            .map(|(row, _)| row)
            .collect();
        let mut pool = crate::core::buffer::BufferPool::new();
        let sent = payload(body, otlp::Encoding::Json.content_type());
        let decoded = meta.decode(&[sent], None::<&[&str]>, None, None, &mut pool);
        assert_eq!(decoded.rows(), 3);
        let mut typed = 0;
        for (at, (name, ty)) in meta.record().iter().enumerate() {
            let column = decoded.batch.peek(record_slot(at));
            assert_eq!(&column.data_type(), ty, "{name}");
            if !matches!(ty, DataType::Array(_) | DataType::Struct(..)) {
                continue;
            }
            let as_variant = crate::format::tag::to_variant(column, &mut pool);
            let shown = as_variant.variant().unwrap();
            for (row, cells) in rows.iter().enumerate() {
                match &cells.0[at] {
                    Some(otlp::Cell::Variant(wrote)) => {
                        typed += 1;
                        assert_eq!(shown.get(row).bytes(), wrote.as_slice(), "{name} at row {row}");
                    }
                    None => assert!(!shown.validity.is_valid(row), "{name} at row {row}"),
                    Some(other) => panic!("{name} is {other:?}"),
                }
            }
            as_variant.reclaim(&mut pool);
        }
        assert_eq!(typed, 4, "bucket counts, bounds, positive and quantiles");
        decoded.batch.reclaim(&mut pool);
    }
}
