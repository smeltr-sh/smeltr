//! What brings a file of an older format version to the current one, so a file nobody has saved since still loads.
//! Each version after the first has one step from the version before it, and a file takes every step after its own
//! version, in order. A step leaves what is already current as it is.

use super::CURRENT_VERSION;
use serde_json::Value;

/// The oldest `"version"` this build reads.
const OLDEST_VERSION: u32 = 1;

/// Whether this build reads a file of `version`.
pub(super) fn reads(version: u32) -> bool {
    (OLDEST_VERSION..=CURRENT_VERSION).contains(&version)
}

/// Brings sources written at `version` to the current version.
pub(super) fn sources(version: u32, sources: &mut [Value]) {
    if version < 2 {
        sources.iter_mut().for_each(syslog_is_a_socket_receiver);
    }
}

/// Brings destinations written at `version` to the current version.
pub(super) fn destinations(version: u32, destinations: &mut [Value]) {
    if version < 3 {
        destinations.iter_mut().for_each(smeltr_gzips_at_most_3);
    }
}

/// Brings a whole file to the current version. One of a version this build does not read is left as it is, for the
/// load to refuse by its version.
pub(super) fn file(file: &mut Value) {
    let Some(version) = file
        .get("version")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
    else {
        return;
    };
    if version == CURRENT_VERSION || !reads(version) {
        return;
    }
    if let Some(Value::Array(written)) = file.get_mut("sources") {
        sources(version, written);
    }
    if let Some(Value::Array(written)) = file.get_mut("destinations") {
        destinations(version, written);
    }
    file["version"] = CURRENT_VERSION.into();
}

/// Version 2: a syslog receiver is a socket receiver that frames syslog's messages, over TCP and over UDP alike.
fn syslog_is_a_socket_receiver(source: &mut Value) {
    let Some(source) = source.as_object_mut() else {
        return;
    };
    if source.get("type").and_then(Value::as_str) != Some("syslogReceiver") {
        return;
    }
    source.insert("type".into(), "socketReceiver".into());
    source.insert("framing".into(), "syslog".into());
}

/// Version 3: smeltr's own gzip takes levels 0 to 3 (`CompressionDto::levels`), so a gzip level above 3 is lowered
/// to 3. Kafka, Event Hubs, Delta Lake, Iceberg and parquet files are compressed by librdkafka and the parquet writer,
/// and keep their levels.
fn smeltr_gzips_at_most_3(destination: &mut Value) {
    let Some(destination) = destination.as_object_mut() else {
        return;
    };
    let by_library = matches!(
        destination.get("type").and_then(Value::as_str),
        Some("kafka" | "eventHubs" | "delta" | "iceberg")
    ) || destination.get("format").and_then(Value::as_str) == Some("parquet");
    if by_library || destination.get("compression").and_then(Value::as_str) != Some("gzip") {
        return;
    }
    if let Some(level) = destination.get_mut("compressionLevel")
        && level.as_u64().is_some_and(|l| l > 3)
    {
        *level = 3.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_version_1_syslog_receiver_is_a_socket_receiver_framing_syslog() {
        let mut v1 = json!({"version": 1, "sources": [
            {"id": "tcp", "type": "syslogReceiver", "port": 6514, "lineBreaker": "(\\x00)"},
            {"id": "udp", "type": "syslogReceiver", "port": 514, "transport": "udp"},
            {"id": "other", "type": "httpReceiver", "port": 8080}]});
        file(&mut v1);
        assert_eq!(
            v1,
            json!({"version": CURRENT_VERSION, "sources": [
                {"id": "tcp", "type": "socketReceiver", "framing": "syslog", "port": 6514, "lineBreaker": "(\\x00)"},
                {"id": "udp", "type": "socketReceiver", "framing": "syslog", "port": 514, "transport": "udp"},
                {"id": "other", "type": "httpReceiver", "port": 8080}]})
        );
    }

    #[test]
    fn a_version_2_gzip_level_smeltr_compresses_above_3_is_3() {
        let gzip = |kind: &str, format: Option<&str>, level: u8| {
            let mut d = json!({"id": kind, "type": kind, "compression": "gzip", "compressionLevel": level});
            if let Some(f) = format {
                d["format"] = f.into();
            }
            d
        };
        let mut v2 = json!({"version": 2, "destinations": [
            gzip("http", None, 9), gzip("otlp", None, 2), gzip("s3", Some("ndjson"), 6),
            gzip("s3", Some("parquet"), 9), gzip("kafka", None, 9), gzip("eventHubs", None, 9),
            gzip("delta", None, 9), gzip("iceberg", None, 9),
            {"id": "z", "type": "http", "compression": "zstd", "compressionLevel": 19}]});
        file(&mut v2);
        assert_eq!(
            v2,
            json!({"version": CURRENT_VERSION, "destinations": [
                gzip("http", None, 3), gzip("otlp", None, 2), gzip("s3", Some("ndjson"), 3),
                gzip("s3", Some("parquet"), 9), gzip("kafka", None, 9), gzip("eventHubs", None, 9),
                gzip("delta", None, 9), gzip("iceberg", None, 9),
                {"id": "z", "type": "http", "compression": "zstd", "compressionLevel": 19}]})
        );
    }

    #[test]
    fn a_version_this_build_does_not_read_is_left_for_the_load_to_refuse() {
        for version in [0, CURRENT_VERSION + 1] {
            let mut unread = json!({"version": version, "sources": [{"id": "s", "type": "syslogReceiver"}]});
            let before = unread.clone();
            file(&mut unread);
            assert_eq!(unread, before);
        }
    }
}
