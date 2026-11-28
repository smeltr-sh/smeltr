//! A MaxMind DB file written from records, so a pipeline's GeoIP database and the company's networks are data of our
//! own that every engine's reader takes. IPv4 only, with 32-bit records, laid out as the format's specification
//! gives it (<https://maxmind.github.io/MaxMind-DB/>):
//!
//! ```mermaid
//! flowchart LR
//!   tree[search tree: a node per bit] --> sep[16 zero bytes] --> data[data section] --> meta[metadata marker and map]
//! ```

use std::net::Ipv4Addr;

use serde_json::{Value, json};

/// The file holding each of `networks`, an address, its prefix length and the record an address in it finds. No two
/// networks overlap.
pub fn write(database_type: &str, description: &str, networks: &[(Ipv4Addr, u8, Value)]) -> Vec<u8> {
    let mut data = Vec::new();
    let mut nodes: Vec<[Record; 2]> = vec![[Record::Empty; 2]];
    for (address, len, record) in networks {
        assert!((1..=32).contains(len), "{address}/{len}: a prefix of 1 to 32 bits");
        let at = u32::try_from(data.len()).expect("a data section under 4 GiB");
        encode(record, &mut data);
        let bits = u32::from(*address);
        let mut node = 0;
        for depth in 0..*len {
            let bit = usize::from(bits >> (31 - depth) & 1 == 1);
            if depth + 1 == *len {
                assert!(
                    matches!(nodes[node][bit], Record::Empty),
                    "{address}/{len} overlaps another network"
                );
                nodes[node][bit] = Record::Data(at);
                break;
            }
            node = match nodes[node][bit] {
                Record::Node(next) => next as usize,
                Record::Empty => {
                    nodes.push([Record::Empty; 2]);
                    let next = nodes.len() - 1;
                    nodes[node][bit] = Record::Node(u32::try_from(next).expect("under 2^32 nodes"));
                    next
                }
                Record::Data(_) => panic!("{address}/{len} lies inside another network"),
            };
        }
    }
    let count = u32::try_from(nodes.len()).expect("under 2^32 nodes");
    let mut out = Vec::with_capacity(nodes.len() * 8 + 16 + data.len() + 512);
    for node in &nodes {
        for record in node {
            let value = match *record {
                Record::Empty => count,
                Record::Node(n) => n,
                // A pointer to data counts from the start of the data section, past the node count and the separator.
                Record::Data(at) => count + 16 + at,
            };
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
    out.extend_from_slice(&[0; 16]);
    out.extend_from_slice(&data);
    out.extend_from_slice(b"\xAB\xCD\xEFMaxMind.com");
    let metadata = json!({
        "binary_format_major_version": 2,
        "binary_format_minor_version": 0,
        "build_epoch": 1_790_000_000u64,
        "database_type": database_type,
        "description": {"en": description},
        "ip_version": 4,
        "languages": ["en"],
        "node_count": count,
        "record_size": 32,
    });
    let widths = |key: &str| match key {
        "binary_format_major_version" | "binary_format_minor_version" | "ip_version" | "record_size" => UINT16,
        "build_epoch" => UINT64,
        _ => UINT32,
    };
    let fields = metadata.as_object().unwrap();
    control(&mut out, MAP, fields.len());
    for (key, value) in fields {
        string(&mut out, key);
        match value {
            Value::Number(n) => uint(&mut out, widths(key), n.as_u64().unwrap()),
            v => encode(v, &mut out),
        }
    }
    out
}

/// One of a node's two records: a node further down, a network's data, or nothing.
#[derive(Clone, Copy)]
enum Record {
    Empty,
    Node(u32),
    /// An offset into the data section.
    Data(u32),
}

const STRING: u8 = 2;
const DOUBLE: u8 = 3;
const UINT16: u8 = 5;
const UINT32: u8 = 6;
const MAP: u8 = 7;
const INT32: u8 = 8;
const UINT64: u8 = 9;
const ARRAY: u8 = 11;
const BOOLEAN: u8 = 14;

/// A field's control byte: its type in the top three bits, an extended type in the byte after, and its size.
fn control(out: &mut Vec<u8>, kind: u8, size: usize) {
    let (top, extended) = if kind <= 7 { (kind, None) } else { (0, Some(kind - 7)) };
    let (low, extra): (u8, Vec<u8>) = match size {
        0..29 => (size as u8, Vec::new()),
        29..285 => (29, vec![(size - 29) as u8]),
        285..65_821 => (30, ((size - 285) as u16).to_be_bytes().to_vec()),
        _ => (
            31,
            (u32::try_from(size - 65_821).expect("a field under 16 MiB")).to_be_bytes()[1..].to_vec(),
        ),
    };
    out.push(top << 5 | low);
    out.extend(extended);
    out.extend(extra);
}

fn string(out: &mut Vec<u8>, s: &str) {
    control(out, STRING, s.len());
    out.extend_from_slice(s.as_bytes());
}

/// An unsigned integer of `kind`, in as few big-endian bytes as hold it.
fn uint(out: &mut Vec<u8>, kind: u8, n: u64) {
    let bytes = n.to_be_bytes();
    let first = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len());
    control(out, kind, bytes.len() - first);
    out.extend_from_slice(&bytes[first..]);
}

/// `value` in the data section's encoding: a map, a string, a double for a number with a fraction, an unsigned
/// integer for one without, a signed one for one below zero.
fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(fields) => {
            control(out, MAP, fields.len());
            for (key, v) in fields {
                string(out, key);
                encode(v, out);
            }
        }
        Value::Array(items) => {
            control(out, ARRAY, items.len());
            for v in items {
                encode(v, out);
            }
        }
        Value::String(s) => string(out, s),
        Value::Bool(b) => control(out, BOOLEAN, usize::from(*b)),
        Value::Number(n) => match (n.as_u64(), n.as_i64()) {
            (Some(u), _) => uint(out, if u <= u64::from(u32::MAX) { UINT32 } else { UINT64 }, u),
            (None, Some(i)) => {
                control(out, INT32, 4);
                out.extend_from_slice(&i32::try_from(i).expect("an int32").to_be_bytes());
            }
            (None, None) => {
                control(out, DOUBLE, 8);
                out.extend_from_slice(&n.as_f64().unwrap().to_be_bytes());
            }
        },
        Value::Null => panic!("a MaxMind DB holds no null"),
    }
}

/// What the file holds is what was written: each network's record is found at its first, last and middle addresses,
/// and an address outside every network finds nothing.
#[test]
fn a_written_file_reads_back() {
    let networks = vec![
        (
            Ipv4Addr::new(10, 1, 0, 0),
            16,
            json!({"site": "lon", "tier": "general", "n": 7}),
        ),
        (
            Ipv4Addr::new(81, 2, 69, 0),
            24,
            json!({"city": {"names": {"en": "London"}}, "location": {"latitude": 51.5072, "longitude": -0.1276}}),
        ),
        (
            Ipv4Addr::new(10, 2, 0, 0),
            24,
            json!({"list": ["a", "b"], "on": true, "big": 1u64 << 40}),
        ),
    ];
    let reader = maxminddb::Reader::from_source(write("Smeltr-Test", "a test", &networks)).unwrap();
    assert_eq!(reader.metadata().database_type, "Smeltr-Test");
    for (address, len, record) in &networks {
        let first = u32::from(*address);
        let last = first | (u32::MAX >> len);
        for ip in [first, first + (last - first) / 2, last] {
            let found: Option<Value> = (reader.lookup(Ipv4Addr::from(ip).into())).unwrap().decode().unwrap();
            assert_eq!(found.as_ref(), Some(record), "{}", Ipv4Addr::from(ip));
        }
    }
    let nothing: Option<Value> = reader.lookup("10.3.0.1".parse().unwrap()).unwrap().decode().unwrap();
    assert_eq!(nothing, None);
}
