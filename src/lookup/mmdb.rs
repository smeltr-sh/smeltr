//! MaxMind DB (MMDB) lookup support — `plan::config::LookupSource::Mmdb`. GeoIP-style binary databases: keyed by their
//! own IP/CIDR trie (so `MatchKind` must be `Cidr`, and `Multiplicity` is moot — the trie already returns at most one
//! record by construction, the same "most specific wins" a plain CSV CIDR table's `Multiplicity::Best` computes, just
//! inherent here rather than something this engine has to do), with a record structure that's arbitrarily nested and
//! varies by vendor/edition (GeoLite2-City, GeoLite2-ASN, a commercial ISP database, ...).
//!
//! Rather than try to enumerate a database's schema up front — which would mean probing sample records, and still
//! guessing at a database this codebase has never seen — an output's `source` is a dot-separated path a pipeline author
//! types by hand (`"country.iso_code"`, `"city.names.en"`), the same shape MaxMind's own `mmdbinspect` tooling already
//! prints a record as.

use std::cell::RefCell;
use std::fmt;
use std::net::IpAddr;
use std::rc::Rc;

use maxminddb::LookupResult;
use serde::de::{Deserialize, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::LookupError;

/// The record an address fell in.
pub type Record<'a> = LookupResult<'a, memmap2::Mmap>;

#[derive(Debug)]
pub struct MmdbTable {
    reader: maxminddb::Reader<memmap2::Mmap>,
}

impl MmdbTable {
    /// Maps the file once, at compile time -- the same "no filesystem access once compiled" guarantee every other
    /// lookup source gets. A corrupt or unreadable file is a `CompileError` here, never a row-time surprise.
    pub fn open(path: &std::path::Path) -> Result<MmdbTable, LookupError> {
        let shown = path.display().to_string();
        let map =
            super::store::map(path).map_err(|e| LookupError(format!("couldn't open MMDB file `{shown}`: {e}")))?;
        MmdbTable::from_map(&shown, map)
    }

    /// `open` over a file already mapped, naming the file only for its errors.
    pub fn from_map(shown: &str, map: memmap2::Mmap) -> Result<MmdbTable, LookupError> {
        let reader = maxminddb::Reader::from_source(map)
            .map_err(|e| LookupError(format!("couldn't open MMDB file `{shown}`: {e}")))?;
        Ok(MmdbTable { reader })
    }

    /// The record `ip` falls in, undecoded, or `None` on a miss (no network in the database contains `ip`). `Paths`
    /// reads its leaves in one walk, never the whole record: a City record carries names in a dozen languages and a
    /// pipeline reads a handful of leaves. Its `offset` names it, so rows in one network share one read.
    pub fn find(&self, ip: IpAddr) -> Option<Record<'_>> {
        let result = self.reader.lookup(ip).ok()?;
        result.has_data().then_some(result)
    }

    pub fn metadata(&self) -> &maxminddb::Metadata {
        self.reader.metadata()
    }

    /// How many networks carry a record. An IPv4 network reachable under several IPv6 aliases counts once. This
    /// walks the whole trie, which is seconds on a full-size database.
    pub fn count_networks(&self) -> Result<u64, LookupError> {
        let failed = |e: maxminddb::MaxMindDbError| LookupError(format!("couldn't walk the MMDB file: {e}"));
        let mut networks = self.reader.networks(Default::default()).map_err(failed)?;
        networks.try_fold(0, |count, network| network.map(|_| count + 1).map_err(failed))
    }
}

/// The leaves a lookup reads from a record, its dot-separated paths (`"country.iso_code"`) as one tree. A read walks
/// the record once: a key on the tree is entered, every other value is stepped over, and a leaf's text lands at
/// the index its path was given at. Built once per batch. Cheap to clone, since a read hands a clone to the decoder.
#[derive(Clone)]
pub struct Paths(Rc<Tree>);

struct Tree {
    root: Node,
    count: usize,
}

struct Node {
    key: String,
    children: Vec<Node>,
    /// The outputs whose path ends here. More than one when two outputs read the same path.
    leaves: Vec<usize>,
}

impl Paths {
    pub fn new<'a>(paths: impl IntoIterator<Item = &'a str>) -> Paths {
        let mut root = Node {
            key: String::new(),
            children: Vec::new(),
            leaves: Vec::new(),
        };
        let mut count = 0;
        for (i, path) in paths.into_iter().enumerate() {
            let mut node = &mut root;
            for segment in path.split('.') {
                let at = match node.children.iter().position(|c| c.key == segment) {
                    Some(at) => at,
                    None => {
                        node.children.push(Node {
                            key: segment.to_string(),
                            children: Vec::new(),
                            leaves: Vec::new(),
                        });
                        node.children.len() - 1
                    }
                };
                node = &mut node.children[at];
            }
            node.leaves.push(i);
            count = i + 1;
        }
        Paths(Rc::new(Tree { root, count }))
    }

    /// Every leaf of `record`, in the order the paths were given: the text `path_value` gives, or `None` where the
    /// path misses at any segment or lands on a map or an array. That is an ordinary miss for the one output, never
    /// an error. The walk steps over every value it does not want, so a record with one key or value the decoder
    /// cannot read, wanted or not, is a miss for every output.
    pub fn read(&self, record: &Record<'_>) -> Vec<Option<String>> {
        WANTED.with(|w| *w.borrow_mut() = Some(self.clone()));
        let leaves = record.decode::<Leaves>();
        WANTED.with(|w| *w.borrow_mut() = None);
        match leaves {
            Ok(Some(Leaves(values))) => values,
            _ => vec![None; self.0.count],
        }
    }
}

thread_local! {
    /// The tree a `Leaves` decode reads. `LookupResult::decode` takes a type and no value, so the tree reaches the
    /// visitor by this door: set for the one call, cleared after it.
    static WANTED: RefCell<Option<Paths>> = const { RefCell::new(None) };
}

/// One record's leaves, decoded against the tree in `WANTED`.
struct Leaves(Vec<Option<String>>);

impl<'de> Deserialize<'de> for Leaves {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Leaves, D::Error> {
        let paths = WANTED
            .with(|w| w.borrow().clone())
            .expect("a read sets the tree before it decodes");
        let mut out = vec![None; paths.0.count];
        deserializer.deserialize_any(NodeVisitor {
            node: &paths.0.root,
            out: &mut out,
        })?;
        Ok(Leaves(out))
    }
}

/// The value under one tree node. A map is walked for the node's children; a scalar is the text of the node's own
/// leaves; anything else is stepped over and leaves them `None`. The narrower scalars (16- and 32-bit integers, single
/// floats) reach the wider methods here through serde's own defaults, the way a `serde_json` value does.
struct NodeVisitor<'a> {
    node: &'a Node,
    out: &'a mut [Option<String>],
}

impl NodeVisitor<'_> {
    fn leaf(self, text: String) {
        let Some((last, rest)) = self.node.leaves.split_last() else {
            return;
        };
        for &i in rest {
            self.out[i] = Some(text.clone());
        }
        self.out[*last] = Some(text);
    }
}

impl<'de> Visitor<'de> for NodeVisitor<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an MMDB value")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(child) = map.next_key_seed(KeySeed { node: self.node })? {
            match child {
                Some(child) => map.next_value_seed(NodeVisitor {
                    node: child,
                    out: &mut *self.out,
                })?,
                None => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(())
    }

    fn visit_bool<E>(self, v: bool) -> Result<(), E> {
        self.leaf(v.to_string());
        Ok(())
    }

    fn visit_i64<E>(self, v: i64) -> Result<(), E> {
        self.leaf(v.to_string());
        Ok(())
    }

    fn visit_u64<E>(self, v: u64) -> Result<(), E> {
        self.leaf(v.to_string());
        Ok(())
    }

    /// Past `u64` a JSON number cannot hold it, and `path_value` gives none; the same here.
    fn visit_u128<E>(self, v: u128) -> Result<(), E> {
        if let Ok(v) = u64::try_from(v) {
            self.leaf(v.to_string());
        }
        Ok(())
    }

    /// The text `serde_json` gives the number, as `path_value` has it, written by the same `zmij`. Not a number (NaN,
    /// an infinity) is no leaf.
    fn visit_f64<E>(self, v: f64) -> Result<(), E> {
        if v.is_finite() {
            self.leaf(zmij::Buffer::new().format_finite(v).to_string());
        }
        Ok(())
    }

    fn visit_str<E>(self, v: &str) -> Result<(), E> {
        self.leaf(v.to_string());
        Ok(())
    }

    fn visit_bytes<E>(self, _v: &[u8]) -> Result<(), E> {
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for NodeVisitor<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

/// A map key, answered as the child of `node` it names, without copying the key out of the record.
struct KeySeed<'a> {
    node: &'a Node,
}

impl<'de, 'a> DeserializeSeed<'de> for KeySeed<'a> {
    type Value = Option<&'a Node>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de, 'a> Visitor<'de> for KeySeed<'a> {
    type Value = Option<&'a Node>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a map key")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        Ok(self.node.children.iter().find(|c| c.key == v))
    }
}

/// Walks a dot-separated path (`"country.iso_code"`) into a whole decoded record, returning the leaf's stringified
/// value: the oracle `Paths::read` is checked against, not a path the pipeline takes. Missing at any segment, or a
/// non-scalar leaf (a path that only reaches partway into a nested object/array), is `None`.
#[cfg(test)]
pub fn path_value(record: &serde_json::Value, path: &str) -> Option<String> {
    let mut v = record;
    for segment in path.split('.') {
        v = v.get(segment)?;
    }
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(_) | serde_json::Value::Number(_) => Some(v.to_string()),
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl MmdbTable {
        /// The whole record as a JSON value, or `None` on a miss: the oracle `Paths::read` is checked against.
        fn lookup(&self, ip: IpAddr) -> Option<serde_json::Value> {
            self.find(ip)?.decode::<serde_json::Value>().ok().flatten()
        }
    }

    /// One read gives every leaf `path_value` reads off the whole record: a path that misses, one that lands on a
    /// map, and two outputs on one path included.
    #[test]
    fn a_read_agrees_with_the_whole_record() {
        let table = MmdbTable::open(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/lookup/test-data/test-city.mmdb"
        )))
        .unwrap();
        let ip: IpAddr = "10.1.5.5".parse().unwrap();
        let whole = table.lookup(ip).unwrap();
        let record = table.find(ip).unwrap();
        let paths = [
            "country.iso_code",
            "country.names.en",
            "country",
            "nosuch.x",
            "location.latitude",
            "country.iso_code",
            "city.names",
            "",
        ];
        let read = Paths::new(paths.iter().copied()).read(&record);
        let expected: Vec<Option<String>> = paths.iter().map(|p| path_value(&whole, p)).collect();
        assert_eq!(read, expected);
        assert_eq!(read[0].as_deref(), Some("US"), "the fixture has a country");
        assert_eq!(
            read[5].as_deref(),
            Some("US"),
            "the second output on the path reads it too"
        );
        assert!(table.find("8.8.8.8".parse().unwrap()).is_none());
    }

    /// Every value type the format has, in one record, read to the text it gives or a miss. The database is the
    /// fixture's own search tree with every record pointed at one hand-built data record, since `lookup` cannot be
    /// the oracle here: a JSON value holds neither bytes nor an integer past 64 bits.
    #[test]
    fn a_read_gives_every_type_its_text_or_a_miss() {
        let table = MmdbTable::open(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/lookup/test-data/every-type.mmdb"
        )))
        .unwrap();
        let record = table.find("10.1.5.5".parse().unwrap()).unwrap();
        let cases: [(&str, Option<&str>); 26] = [
            ("s", Some("str")),
            ("e", Some("")),
            ("bt", Some("true")),
            ("bf", Some("false")),
            ("i", Some("-5")),
            ("ip", Some("7")),
            ("u16", Some("65535")),
            ("u32", Some("4294967295")),
            ("u64", Some("18446744073709551615")),
            ("u128s", Some("42")),
            ("u128b", None),
            ("f32", Some("1.5")),
            ("f64", Some("-97.822")),
            ("nan", None),
            ("inf", None),
            ("whole", Some("3.0")),
            ("by", None),
            ("arr", None),
            ("arr.0", None),
            ("m", None),
            ("m.k", Some("v")),
            ("m.n.d", Some("1")),
            ("m.k.x", None),
            ("nosuch", None),
            ("", None),
            ("s.x", None),
        ];
        let read = Paths::new(cases.iter().map(|(p, _)| *p)).read(&record);
        let want: Vec<Option<String>> = cases.iter().map(|(_, v)| v.map(str::to_string)).collect();
        assert_eq!(read, want);
    }

    /// A value the decoder rejects anywhere in the record is a miss for every output, the wanted ones before it
    /// included. The record here is a country map and then a double declaring seven bytes.
    #[test]
    fn a_value_the_decoder_rejects_is_a_miss_for_every_output() {
        let table = MmdbTable::open(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/lookup/test-data/bad-double.mmdb"
        )))
        .unwrap();
        let ip: IpAddr = "10.1.5.5".parse().unwrap();
        let record = table.find(ip).unwrap();
        assert_eq!(Paths::new(["country.iso_code", "bad"]).read(&record), vec![None, None]);
        assert!(table.lookup(ip).is_none(), "the whole-record read rejects it too");
    }

    /// The same, over a real GeoLite2-City database when `SMELTR_GEOLITE_CITY` names one: its records carry the
    /// floats, integers and pointers the fixture lacks.
    #[test]
    fn a_read_of_a_city_record_agrees_with_the_whole_record() {
        let Ok(file) = std::env::var("SMELTR_GEOLITE_CITY") else {
            return;
        };
        let table = MmdbTable::open(std::path::Path::new(&file)).unwrap();
        let paths = [
            "country.iso_code",
            "country.names.en",
            "city.names.en",
            "continent.code",
            "location.latitude",
            "location.longitude",
            "location.accuracy_radius",
            "location.metro_code",
            "postal.code",
            "location.time_zone",
            "country.is_in_european_union",
            "subdivisions.0.names.en",
            "location",
        ];
        let reader = Paths::new(paths.iter().copied());
        let mut with_latitude = 0;
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "81.2.69.142",
            "2001:4860:4860::8888",
            "89.160.20.128",
            "216.160.83.56",
        ] {
            let ip: IpAddr = ip.parse().unwrap();
            let Some(record) = table.find(ip) else {
                continue;
            };
            let whole = table.lookup(ip).unwrap();
            let expected: Vec<Option<String>> = paths.iter().map(|p| path_value(&whole, p)).collect();
            assert_eq!(reader.read(&record), expected, "{ip}");
            with_latitude += usize::from(expected[4].is_some());
        }
        assert!(with_latitude > 0, "no address had a latitude, so no float was compared");
    }

    #[test]
    fn open_a_missing_file_is_an_error() {
        assert!(MmdbTable::open(std::path::Path::new("/does/not/exist/smeltr-lookup.mmdb")).is_err());
    }
}
