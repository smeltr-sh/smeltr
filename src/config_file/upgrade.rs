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
    fn a_version_this_build_does_not_read_is_left_for_the_load_to_refuse() {
        for version in [0, CURRENT_VERSION + 1] {
            let mut unread = json!({"version": version, "sources": [{"id": "s", "type": "syslogReceiver"}]});
            let before = unread.clone();
            file(&mut unread);
            assert_eq!(unread, before);
        }
    }
}
