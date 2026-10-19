use std::sync::Arc;

use apache_avro::Schema;
use apache_avro::types::Value as Avro;
use apache_avro::writer::datum::GenericDatumWriter;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType as Arrow, Field};

use super::*;
use crate::io::RecordChunkBuilder;
use crate::plan::topology::{SecretRef, SourceMeta};

/// The integration tests' fake registry.
#[allow(dead_code)]
#[path = "../../../tests/harness/registry.rs"]
mod fake;
use fake::Registry as Fake;

fn registry(url: &str) -> SchemaRegistry {
    SchemaRegistry {
        url: url.to_string(),
        basic: None,
    }
}

/// A reader of `fields`, from `url`.
fn reader(url: &str, fields: &[(&str, DataType)]) -> (AvroReader, SourceMeta) {
    let meta = SourceMeta::KAFKA
        .declaring(fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect())
        .unwrap();
    let config = AvroRead {
        registry: Some(registry(url)),
        meta: meta.clone(),
    };
    (AvroReader::new(&config, None, &HeldSecrets::default()).unwrap(), meta)
}

/// `payloads` as one poll of a Kafka source.
fn poll(payloads: &[Vec<u8>]) -> Vec<SourceRecord> {
    let mut out = RecordChunkBuilder::new();
    for (i, p) in payloads.iter().enumerate() {
        out.push_stamped(p, i as i64, 0, Arc::from("t"), None, Some(7));
    }
    out.finish()
}

/// `value` written with `schema`, framed as `id`.
fn written(id: u32, schema: &Schema, value: Avro) -> Vec<u8> {
    let bytes = GenericDatumWriter::builder(schema)
        .build()
        .unwrap()
        .write_value_to_vec(value)
        .unwrap();
    frame(id, &bytes)
}

fn rows(meta: &SourceMeta, records: &[SourceRecord]) -> Vec<serde_json::Value> {
    records
        .iter()
        .map(|r| serde_json::from_slice(&meta.record_json(r, "")).unwrap())
        .collect()
}

#[test]
fn a_framed_payload_is_its_id_and_its_value() {
    assert_eq!(unframe(&frame(258, b"abc")), Ok((258, &b"abc"[..])));
    assert!(unframe(b"").unwrap_err().contains("empty"));
    assert!(unframe(b"\x01\0\0\0\x01x").unwrap_err().contains("0x01"));
    assert!(unframe(b"\0\0\0").unwrap_err().contains("3 bytes"));
}

#[test]
fn a_decimal_reads_exactly() {
    assert_eq!(decimal(&[0x01, 0x00], 2), "2.56");
    assert_eq!(decimal(&[0xff], 0), "-1");
    assert_eq!(decimal(&[0xff], 3), "-0.001");
    assert_eq!(decimal(&[], 0), "0");
    assert_eq!(decimal(&[0x00], 2), "0.00");
    // 2^64, past any machine integer.
    assert_eq!(decimal(&[0x01, 0, 0, 0, 0, 0, 0, 0, 0], 1), "1844674407370955161.6");
}

#[test]
fn a_name_avro_refuses_is_refused() {
    assert_eq!(refuse_name("_a1"), None);
    assert!(refuse_name("1a").is_some());
    assert!(refuse_name("a-b").is_some());
    assert!(refuse_name("").is_some());
}

#[test]
fn a_payload_reads_into_the_declared_fields_by_name() {
    let fake = Fake::default();
    let address = r#"{"type":"record","name":"Address","namespace":"x","fields":[{"name":"city","type":"string"}]}"#;
    fake.hold("address", address, serde_json::json!([]));
    let row = r#"{"type":"record","name":"Row","namespace":"x","fields":[
        {"name":"id","type":"long"},
        {"name":"home","type":["null","x.Address"]},
        {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"day","type":{"type":"int","logicalType":"date"}},
        {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},
        {"name":"kind","type":{"type":"enum","name":"Kind","symbols":["A","B"]}},
        {"name":"labels","type":{"type":"map","values":"long"}},
        {"name":"tags","type":{"type":"array","items":"string"}},
        {"name":"extra","type":"string"}
    ]}"#;
    let id = fake.hold(
        "row-value",
        row,
        serde_json::json!([{ "name": "x.Address", "subject": "address", "version": 1 }]),
    );
    let url = fake.serve();
    let (mut reader, meta) = reader(
        &url,
        &[
            ("id", DataType::I64),
            ("home.city", DataType::Str),
            ("at", DataType::DateTime),
            ("day", DataType::DateTime),
            ("price", DataType::Str),
            ("kind", DataType::Str),
            ("labels", DataType::Variant),
            ("tags", DataType::Array(Box::new(DataType::Str))),
            ("absent", DataType::Str),
        ],
    );
    let parsed = Parsed::new(row, &[address.to_string()]).unwrap();
    let value = Avro::Record(vec![
        ("id".into(), Avro::Long(5)),
        (
            "home".into(),
            Avro::Union(
                1,
                Box::new(Avro::Record(vec![("city".into(), Avro::String("Oslo".into()))])),
            ),
        ),
        ("at".into(), Avro::TimestampMillis(1_000)),
        ("day".into(), Avro::Date(1)),
        (
            "price".into(),
            Avro::Decimal(apache_avro::Decimal::from(vec![0x04, 0xd2])),
        ),
        ("kind".into(), Avro::Enum(1, "B".into())),
        (
            "labels".into(),
            Avro::Map([("b".to_string(), Avro::Long(2)), ("a".to_string(), Avro::Long(1))].into()),
        ),
        ("tags".into(), Avro::Array(vec![Avro::String("p".into())])),
        ("extra".into(), Avro::String("kept nowhere".into())),
    ]);
    let resolved = parsed.resolved().unwrap();
    let bytes = GenericDatumWriter::builder(&parsed.schema)
        .resolved_schemata(resolved)
        .build()
        .unwrap()
        .write_value_to_vec(value)
        .unwrap();
    let payload = frame(id, &bytes);
    assert_eq!(reader.unasked(&payload), Some(id));
    reader.ask(id).unwrap();
    assert_eq!(reader.unasked(&payload), None);
    let read = reader.read(poll(&[payload]));
    assert_eq!(read[0].chunk.time(0), Some(7), "the source's stamp is kept");
    assert_eq!(read[0].offset, 0);
    assert_eq!(
        rows(&meta, &read),
        vec![serde_json::json!({
            "id": 5,
            "home": { "city": "Oslo" },
            "at": "1970-01-01T00:00:01.000000Z",
            "day": "1970-01-02T00:00:00.000000Z",
            "price": "12.34",
            "kind": "B",
            "labels": { "a": 1, "b": 2 },
            "tags": ["p"],
        })]
    );
}

#[test]
fn a_payload_that_does_not_read_is_a_row_of_nulls_with_the_reason() {
    let fake = Fake::default();
    let schema = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"string"}]}"#;
    let id = fake.hold("r-value", schema, serde_json::json!([]));
    let not_a_record = fake.hold("s-value", r#""long""#, serde_json::json!([]));
    let url = fake.serve();
    let (mut reader, meta) = reader(&url, &[("id", DataType::I64)]);
    reader.ask(id).unwrap();
    reader.ask(not_a_record).unwrap();
    reader.ask(99).unwrap();
    let parsed = Schema::parse_str(schema).unwrap();
    let payloads = [
        written(
            id,
            &parsed,
            Avro::Record(vec![("id".into(), Avro::String("seven".into()))]),
        ),
        b"{\"id\":1}".to_vec(),
        frame(99, b"\x02"),
        written(not_a_record, &Schema::Long, Avro::Long(1)),
        frame(id, b"\x02a\x00"),
        frame(id, b"\x7f"),
    ];
    let read = reader.read(poll(&payloads));
    let chunk = &read[0].chunk;
    assert_eq!(chunk.unread(0), None);
    assert_eq!(
        chunk.failed(),
        &[(0, 0, "seven".to_string())][..],
        "not its field's type"
    );
    assert!(chunk.unread(1).unwrap().contains("0x7b"));
    assert!(
        chunk.unread(2).unwrap().contains("Schema not found"),
        "{:?}",
        chunk.unread(2)
    );
    assert!(chunk.unread(3).unwrap().contains("no record"));
    assert!(chunk.unread(4).unwrap().contains("1 bytes after it"));
    assert!(chunk.unread(5).unwrap().contains("does not read"));
    assert_eq!(rows(&meta, &read)[1], serde_json::json!({}));
}

#[test]
fn a_registry_that_does_not_answer_leaves_the_schema_unasked() {
    let fake = Fake::default();
    let id = fake.hold("r-value", r#""long""#, serde_json::json!([]));
    fake.fail(1);
    let (mut reader, _) = reader(&fake.serve(), &[("id", DataType::I64)]);
    let payload = frame(id, b"\x02");
    assert!(reader.ask(id).unwrap_err().message.contains("busy"));
    assert_eq!(reader.unasked(&payload), Some(id));
    reader.ask(id).unwrap();
    assert_eq!(reader.unasked(&payload), None);
}

#[test]
fn a_registry_password_signs_in_and_one_not_held_is_an_error() {
    let fake = Fake::default();
    let id = fake.hold("r-value", r#""long""#, serde_json::json!([]));
    let mut config = registry(&format!("{}/", fake.serve()));
    config.basic = Some((
        "u".into(),
        SecretRef::LeaderStored {
            name: "registry".into(),
            version: 1,
        },
    ));
    let mut secrets = HeldSecrets::default();
    assert!(Registry::new(&config, None, &secrets).is_err(), "a password not held");
    secrets.set_stored([("registry".to_string(), 1, "p".to_string())]);
    Registry::new(&config, None, &secrets).unwrap().schema(id).unwrap();
    let seen = fake.seen();
    assert_eq!(
        seen,
        vec![(format!("GET /schemas/ids/{id} HTTP/1.1"), Some("Basic dTpw".into()))]
    );
}

/// A batch of `id: i64`, `ok: bool` and `who: {name: str}`, its second row all null.
fn batch() -> RecordBatch {
    let name: ArrayRef = Arc::new(StringArray::from(vec![Some("ann"), None]));
    let who = StructArray::from(vec![(Arc::new(Field::new("name", Arrow::Utf8, true)), name)]);
    RecordBatch::try_from_iter_with_nullable(vec![
        ("id", Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef, true),
        (
            "ok",
            Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
            true,
        ),
        ("who", Arc::new(who) as ArrayRef, true),
    ])
    .unwrap()
}

#[test]
fn a_row_written_reads_back_as_it_was() {
    let fake = Fake::default();
    let url = fake.serve();
    let mut writer = AvroWriter::new(&registry(&url), None, &HeldSecrets::default()).unwrap();
    let batch = batch();
    writer.register("t", batch.schema_ref()).unwrap();
    writer.register("t", batch.schema_ref()).unwrap();
    let mut encoder = writer.encoder();
    let payloads: Vec<Vec<u8>> = (0..2).map(|row| encoder.encode("t", &batch, row).unwrap()).collect();
    {
        let schemas = fake.schemas();
        assert_eq!(fake.seen().len(), 1, "registered once");
        assert_eq!(schemas[0].0, "t-value");
        assert!(
            schemas[0].1.contains("\"default\":null"),
            "as written, not in canonical form"
        );
    }
    let (mut reader, meta) = reader(
        &url,
        &[
            ("id", DataType::I64),
            ("ok", DataType::Bool),
            ("who.name", DataType::Str),
        ],
    );
    reader.ask(1).unwrap();
    assert_eq!(
        rows(&meta, &reader.read(poll(&payloads))),
        vec![
            serde_json::json!({ "id": 1, "ok": true, "who": { "name": "ann" } }),
            serde_json::json!({}),
        ]
    );
}

#[test]
fn a_schema_the_subject_refuses_refuses_its_rows_there() {
    let fake = Fake::default();
    fake.refuse("no-value");
    let url = fake.serve();
    let mut writer = AvroWriter::new(&registry(&url), None, &HeldSecrets::default()).unwrap();
    let batch = batch();
    writer.register("no", batch.schema_ref()).unwrap();
    writer.register("yes", batch.schema_ref()).unwrap();
    let mut encoder = writer.encoder();
    assert!(encoder.encode("no", &batch, 0).unwrap_err().contains("incompatible"));
    assert!(encoder.encode("yes", &batch, 0).is_ok());
}

#[test]
fn a_registry_that_does_not_answer_a_registration_is_asked_again() {
    let fake = Fake::default();
    fake.fail(1);
    let url = fake.serve();
    let mut writer = AvroWriter::new(&registry(&url), None, &HeldSecrets::default()).unwrap();
    let batch = batch();
    assert!(writer.register("t", batch.schema_ref()).is_err());
    writer.register("t", batch.schema_ref()).unwrap();
    assert!(writer.encoder().encode("t", &batch, 0).is_ok());
}

#[test]
fn a_field_avro_cannot_name_is_refused() {
    let arrow = ArrowSchema::new(vec![Field::new("a b", Arrow::Int64, true)]);
    assert!(schema_of(&arrow).unwrap_err().contains("`a b`"));
    let inner = Fields::from(vec![Field::new("x", Arrow::Int64, true)]);
    let outer = Fields::from(vec![Field::new("b", Arrow::Struct(inner.clone()), true)]);
    let arrow = ArrowSchema::new(vec![
        Field::new("a", Arrow::Struct(outer), true),
        Field::new("a_b", Arrow::Struct(inner), true),
    ]);
    // Two nested records whose names would be one are told apart.
    Schema::parse_str(&schema_of(&arrow).unwrap()).unwrap();
}

#[test]
fn an_open_record_keeps_the_top_level_fields_no_field_claims() {
    let fake = Fake::default();
    let schema = r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"home","type":{"type":"record","name":"Home","fields":[
            {"name":"city","type":"string"},{"name":"zip","type":"string"}]}},
        {"name":"extra","type":"string"},
        {"name":"gone","type":["null","long"]}
    ]}"#;
    let id = fake.hold("r-value", schema, serde_json::json!([]));
    let url = fake.serve();
    let meta = SourceMeta::KAFKA
        .declaring(vec![("id".into(), DataType::I64), ("home.city".into(), DataType::Str)])
        .unwrap()
        .opened();
    let config = AvroRead {
        registry: Some(registry(&url)),
        meta: meta.clone(),
    };
    let mut reader = AvroReader::new(&config, None, &HeldSecrets::default()).unwrap();
    let value = Avro::Record(vec![
        ("id".into(), Avro::Long(1)),
        (
            "home".into(),
            Avro::Record(vec![
                ("city".into(), Avro::String("Oslo".into())),
                ("zip".into(), Avro::String("0150".into())),
            ]),
        ),
        ("extra".into(), Avro::String("kept".into())),
        ("gone".into(), Avro::Union(0, Box::new(Avro::Null))),
    ]);
    let payload = written(id, &Schema::parse_str(schema).unwrap(), value);
    reader.ask(id).unwrap();
    // `home` is declared, so closed below: its `zip` is no one's. A null field is no field.
    assert_eq!(
        rows(&meta, &reader.read(poll(&[payload]))),
        vec![serde_json::json!({ "id": 1, "home": { "city": "Oslo" }, "extra": "kept" })]
    );
}

/// `values` written to a container file by Avro's own writer, compressed with `codec`.
fn container_file(codec: apache_avro::Codec, values: &[i64]) -> Vec<u8> {
    let schema = Schema::parse_str(r#"{"type":"record","name":"R","fields":[{"name":"n","type":"long"}]}"#).unwrap();
    let mut writer = apache_avro::Writer::builder()
        .schema(&schema)
        .writer(Vec::new())
        .codec(codec)
        .build()
        .unwrap();
    for n in values {
        writer
            .append_value(Avro::Record(vec![("n".into(), Avro::Long(*n))]))
            .unwrap();
    }
    writer.into_inner().unwrap()
}

/// Every value of `file`, each as the long it holds, block by block.
fn container_values(file: &[u8]) -> Result<Vec<Vec<i64>>, String> {
    let mut input = file;
    let container = container::Container::open(&mut input)?;
    let reader = GenericDatumReader::builder(container.schema.schema()).build().unwrap();
    let mut blocks = Vec::new();
    while let Some(values) = container.block(&mut input, 1 << 20)? {
        blocks.push(
            values
                .iter()
                .map(|v| match reader.read_value(&mut v.as_slice()).unwrap() {
                    Avro::Record(f) => match f[0].1 {
                        Avro::Long(n) => n,
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                })
                .collect(),
        );
    }
    Ok(blocks)
}

#[test]
fn a_container_file_avro_writes_reads_value_by_value() {
    for codec in [
        apache_avro::Codec::Null,
        apache_avro::Codec::Deflate(Default::default()),
    ] {
        let file = container_file(codec, &[1, -2, 300]);
        assert_eq!(container_values(&file), Ok(vec![vec![1, -2, 300]]), "{codec:?}");
    }
    let empty = container_file(apache_avro::Codec::Null, &[]);
    assert_eq!(container_values(&empty), Ok(vec![]));
}

#[test]
fn a_container_file_that_does_not_hold_is_refused() {
    let file = container_file(apache_avro::Codec::Null, &[1, 2]);
    assert!(container_values(b"Obj").unwrap_err().contains("too short"));
    assert!(container_values(b"PAR1....").unwrap_err().contains("no Avro file"));
    // The last 16 bytes are the block's sync marker.
    let mut moved = file.clone();
    let last = moved.len() - 1;
    moved[last] ^= 1;
    assert!(container_values(&moved).unwrap_err().contains("sync marker"));
    let cut = &file[..file.len() - 3];
    assert!(container_values(cut).unwrap_err().contains("ends early"));
    let mut input = &file[..];
    let header = container::Container::open(&mut input).unwrap();
    assert!(header.block(&mut input, 1).unwrap_err().contains("more than 1 bytes"));
}

#[test]
fn a_reference_that_references_reads() {
    let fake = Fake::default();
    let city = r#"{"type":"record","name":"City","namespace":"x","fields":[{"name":"name","type":"string"}]}"#;
    fake.hold("city", city, serde_json::json!([]));
    let address = r#"{"type":"record","name":"Address","namespace":"x","fields":[{"name":"city","type":"x.City"}]}"#;
    fake.hold(
        "address",
        address,
        serde_json::json!([{ "name": "x.City", "subject": "city", "version": 1 }]),
    );
    let row = r#"{"type":"record","name":"Row","namespace":"x","fields":[{"name":"home","type":"x.Address"}]}"#;
    let id = fake.hold(
        "row-value",
        row,
        serde_json::json!([{ "name": "x.Address", "subject": "address", "version": 1 }]),
    );
    let url = fake.serve();
    let (mut reader, meta) = reader(&url, &[("home.city.name", DataType::Str)]);
    let parsed = Parsed::new(row, &[city.to_string(), address.to_string()]).unwrap();
    let value = Avro::Record(vec![(
        "home".into(),
        Avro::Record(vec![(
            "city".into(),
            Avro::Record(vec![("name".into(), Avro::String("Oslo".into()))]),
        )]),
    )]);
    let bytes = GenericDatumWriter::builder(&parsed.schema)
        .resolved_schemata(parsed.resolved().unwrap())
        .build()
        .unwrap()
        .write_value_to_vec(value)
        .unwrap();
    let payload = frame(id, &bytes);
    reader.ask(id).unwrap();
    assert_eq!(
        rows(&meta, &reader.read(poll(&[payload]))),
        vec![serde_json::json!({ "home": { "city": { "name": "Oslo" } } })]
    );
}

#[test]
fn a_writer_schema_declares_its_leaves_as_they_read() {
    let address = r#"{"type":"record","name":"Address","namespace":"x","fields":[{"name":"city","type":"string"}]}"#;
    let row = r#"{"type":"record","name":"Row","namespace":"x","fields":[
        {"name":"id","type":"long"},
        {"name":"ok","type":["null","boolean"]},
        {"name":"home","type":["null","x.Address"]},
        {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},
        {"name":"took","type":{"type":"int","logicalType":"time-millis"}},
        {"name":"raw","type":"bytes"},
        {"name":"labels","type":{"type":"map","values":"long"}},
        {"name":"tags","type":{"type":"array","items":"double"}},
        {"name":"grid","type":{"type":"array","items":{"type":"array","items":"long"}}},
        {"name":"either","type":["null","long","string"]}
    ]}"#;
    let parsed = Parsed::new(row, &[address.to_string()]).unwrap();
    let fields: Vec<(String, String, String)> = fields_of(&parsed)
        .into_iter()
        .map(|f| (f.name, f.avro_type, f.suggested))
        .collect();
    let want = [
        ("id", "long", "i64"),
        ("ok", "union[null, boolean]", "bool"),
        ("home.city", "string", "str"),
        ("at", "long (timestamp-millis)", "datetime"),
        ("price", "bytes (decimal)", "str"),
        ("took", "int (time-millis)", "duration"),
        ("raw", "bytes", "bytes"),
        ("labels", "map<long>", "variant"),
        ("tags", "array<double>", "array<f64>"),
        ("grid", "array<array<long>>", "array<array<i64>>"),
        ("either", "union[null, long, string]", "variant"),
    ];
    let want: Vec<(String, String, String)> = (want.iter())
        .map(|(n, a, s)| (n.to_string(), a.to_string(), s.to_string()))
        .collect();
    assert_eq!(fields, want);
    let declared = (fields.iter())
        .map(|(name, _, ty)| (name.clone(), crate::expr::parse::parse_type(ty).unwrap()))
        .collect();
    SourceMeta::KAFKA
        .declaring(declared)
        .expect("a source declares what the schema suggests");
}

#[test]
fn a_record_nested_in_itself_is_declared_whole() {
    let node = r#"{"type":"record","name":"Node","fields":[
        {"name":"v","type":"long"},
        {"name":"next","type":["null","Node"]}
    ]}"#;
    let fields: Vec<(String, String)> = (fields_of(&Parsed::new(node, &[]).unwrap()).into_iter())
        .map(|f| (f.name, f.suggested))
        .collect();
    assert_eq!(fields, [("v".into(), "i64".into()), ("next".into(), "variant".into())]);
}

#[test]
fn a_nest_is_followed_down_to_a_depth() {
    // Each record holds the next, all named apart, deeper than `fields_of` follows. Each is its own reference, as a
    // registry holds it: nested in one text, they would be deeper than JSON reads.
    let deepest = NESTED_RECORDS + 3;
    let record = |depth: usize, ty: &str| {
        format!(r#"{{"type":"record","name":"R{depth}","fields":[{{"name":"a","type":{ty}}}]}}"#)
    };
    let mut referenced = vec![record(deepest, r#""long""#)];
    for depth in (1..deepest).rev() {
        referenced.push(record(depth, &format!(r#""R{}""#, depth + 1)));
    }
    let fields = fields_of(&Parsed::new(&record(0, r#""R1""#), &referenced).unwrap());
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name.matches('a').count(), NESTED_RECORDS + 1);
    assert_eq!(fields[0].suggested, "variant");
}

#[test]
fn a_read_carries_the_schemas_it_read_with_newest_last() {
    let fake = Fake::default();
    let a = r#"{"type":"record","name":"A","fields":[{"name":"id","type":"long"}]}"#;
    let b = r#"{"type":"record","name":"B","fields":[{"name":"id","type":"long"},{"name":"n","type":"string"}]}"#;
    let ida = fake.hold("a", a, serde_json::json!([]));
    let idb = fake.hold("b", b, serde_json::json!([]));
    let url = fake.serve();
    let (mut reader, _) = reader(&url, &[("id", DataType::I64)]);
    reader.ask(ida).unwrap();
    reader.ask(idb).unwrap();
    let (sa, sb) = (Schema::parse_str(a).unwrap(), Schema::parse_str(b).unwrap());
    let pa = written(ida, &sa, Avro::Record(vec![("id".into(), Avro::Long(1))]));
    let pb = written(
        idb,
        &sb,
        Avro::Record(vec![
            ("id".into(), Avro::Long(2)),
            ("n".into(), Avro::String("x".into())),
        ]),
    );
    let read = reader.read(poll(&[pa.clone(), pb, pa, b"not avro".to_vec()]));
    let names: Vec<String> = (read[0].chunk.avro_writers().iter())
        .map(|w| fields_of(w).into_iter().map(|f| f.name).collect::<Vec<_>>().join(","))
        .collect();
    assert_eq!(names, ["id,n", "id"], "a row that does not read names no schema");
}
