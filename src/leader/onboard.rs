//! Setting up a source from a capture: what its records are, read off the records themselves.
//!
//! [`propose`] takes a captured sample and returns the first steps of a pipeline for it and the condition a route
//! would take its rows under. The steps are a proposal for the author to edit, never a configuration that runs: the
//! GUI shows them in the step editor, a CSV Parser with its columns already declared and typed, and the GUI declares
//! any other Parser's fields from a preview of the sample. Nothing reaches a worker until a change is deployed.

#[cfg(test)]
mod fuzz_sniff;

use serde::Serialize;

use crate::config_file::{DataTypeDto, FieldDto, ParserKindDto, StepDto};
use crate::core::column::DataType;
use crate::format::grok_scan::Matcher;
use crate::format::spread::sniff_scalar;
use crate::format::tag::{Suggestion, Tag};
use crate::format::{cef_scan, csv_scan, kv_scan};
use crate::heartbeat::STREAM_FIELDS_PER_PARSER;
use crate::io::RecordObject;
use crate::plan::config::{GrokConfig, KvConfig};

/// The delimiters a CSV record is tried on, in the order a tie between them is settled.
const CSV_DELIMITERS: [u8; 4] = *b",\t;|";

/// The `__source` fields a source files a record under when one source carries several streams, in the order they are
/// tried: a HEC or S2S sender's sourcetype, then a HEC sender's or an Elasticsearch action's index.
const STREAM_FIELDS: [&str; 2] = ["sourcetype", "index"];

/// The whole-line Grok patterns a record's text is tried on, each with the name the GUI knows its format by, in the
/// order a tie between them is settled: an RFC 5424 line before a BSD one, which reads it too, and a combined access
/// log before a common one.
const LINES: [(&str, &str); 4] = [
    ("syslog5424", "SYSLOG5424LINE"),
    ("syslog", "SYSLOGLINE"),
    ("combinedLog", "COMBINEDAPACHELOG"),
    ("commonLog", "COMMONAPACHELOG"),
];

/// What a record's text reads as.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Format {
    Json,
    Cef,
    KeyValue,
    /// One of [`LINES`], by its format's name.
    Line(&'static str),
    Csv {
        delimiter: u8,
        /// The first record's fields, where it reads as a header row rather than as data.
        header: Option<Vec<String>>,
        columns: usize,
    },
}

/// The format most of a sample's records read as.
#[derive(Clone, Debug, PartialEq)]
struct Sniffed {
    /// `None` where no format reads any record.
    pub format: Option<Format>,
    /// Records `format` reads.
    pub read: usize,
    /// Records with text to read: blank ones are left out.
    pub of: usize,
}

/// The format of `texts`, one record's text each. Every format is tried on every record, and the one that reads the
/// most wins; a tie goes to the stricter format, in the order JSON, CEF, [`LINES`], CSV, Key/value, since each later
/// one reads some records an earlier one would also read: CEF reads past a syslog header.
fn sniff<'a>(texts: impl IntoIterator<Item = &'a str>) -> Sniffed {
    let texts: Vec<&str> = texts.into_iter().filter(|t| !t.trim().is_empty()).collect();
    let of = texts.len();
    let json = texts.iter().filter(|t| is_json(t)).count();
    let cef = texts.iter().filter(|t| is_cef(t)).count();
    let kv = texts.iter().filter(|t| is_key_value(t)).count();
    let csv = sniff_csv(&texts);
    let csv_read = csv.as_ref().map_or(0, |(_, read)| *read);
    let lines = LINES.map(|(format, pattern)| {
        let matcher = Matcher::compile(&line_config(pattern)).expect("a built-in line pattern compiles");
        let read = texts.iter().filter(|t| matcher.match_row(t).is_some()).count();
        (Some(Format::Line(format)), read)
    });

    let mut best: Option<(Format, usize)> = None;
    let candidates = [(Some(Format::Json), json), (Some(Format::Cef), cef)]
        .into_iter()
        .chain(lines)
        .chain([(csv.map(|(format, _)| format), csv_read), (Some(Format::KeyValue), kv)]);
    for (format, read) in candidates {
        let Some(format) = format else { continue };
        if read > 0 && best.as_ref().is_none_or(|(_, b)| read > *b) {
            best = Some((format, read));
        }
    }
    match best {
        Some((format, read)) => Sniffed {
            format: Some(format),
            read,
            of,
        },
        None => Sniffed {
            format: None,
            read: 0,
            of,
        },
    }
}

/// A Grok Parser's configuration reading a whole line as the built-in `pattern`.
fn line_config(pattern: &str) -> GrokConfig {
    GrokConfig {
        patterns: vec![format!("^%{{{pattern}}}$")],
        custom_patterns: Vec::new(),
        split_dots: false,
    }
}

fn is_json(text: &str) -> bool {
    let t = text.trim();
    t.starts_with('{') && serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(t).is_ok()
}

fn is_cef(text: &str) -> bool {
    let mut pairs = Vec::new();
    cef_scan::split_record(text.as_bytes(), &mut pairs)
}

/// At least two `key=value` pairs, every key a name rather than a stretch of prose that happened to hold an `=`.
fn is_key_value(text: &str) -> bool {
    let mut pairs = Vec::new();
    kv_scan::split_pairs(text.trim().as_bytes(), &KvConfig::default(), &mut pairs)
        && pairs.len() >= 2
        && pairs.iter().all(|p| is_name(p.key))
}

fn is_name(key: &str) -> bool {
    let mut chars = key.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

/// `text`'s fields split at `delimiter`, or `None` where it does not split or splits into more than a Parser's
/// fields can be reported (`heartbeat::STREAM_FIELDS_PER_PARSER`). The width is counted before anything is split, so
/// a line of nothing but delimiters costs a scan rather than a string per field.
fn csv_fields(text: &str, delimiter: u8) -> Option<Vec<String>> {
    if text.bytes().filter(|b| *b == delimiter).count() >= STREAM_FIELDS_PER_PARSER {
        return None;
    }
    let fields = csv_scan::split_line(text.as_bytes(), delimiter)?;
    Some(fields.into_iter().map(|f| f.text.into_owned()).collect())
}

/// The delimiter that splits the most records into the same number of fields, two or more, and how many it read. A
/// record past the first that splits into another number is not read: a CSV source's rows share one column table.
fn sniff_csv(texts: &[&str]) -> Option<(Format, usize)> {
    let mut best: Option<(Format, usize)> = None;
    for delimiter in CSV_DELIMITERS {
        let rows: Vec<Option<Vec<String>>> = texts.iter().map(|t| csv_fields(t, delimiter)).collect();
        // The most common width among the records that split, two or more.
        let mut widths: Vec<usize> = rows.iter().flatten().map(Vec::len).filter(|n| *n >= 2).collect();
        widths.sort_unstable();
        let Some(columns) = most_common(&widths) else { continue };
        let read = rows.iter().flatten().filter(|r| r.len() == columns).count();
        if best.as_ref().is_some_and(|(_, b)| read <= *b) {
            continue;
        }
        let first = rows.first().and_then(|r| r.as_ref()).filter(|r| r.len() == columns);
        let header = first.filter(|r| is_header(r, &rows[1..])).cloned();
        best = Some((
            Format::Csv {
                delimiter,
                header,
                columns,
            },
            read,
        ));
    }
    best
}

fn most_common(sorted: &[usize]) -> Option<usize> {
    let mut best: Option<(usize, usize)> = None;
    let mut i = 0;
    while i < sorted.len() {
        let j = sorted[i..]
            .iter()
            .position(|w| *w != sorted[i])
            .map_or(sorted.len(), |k| i + k);
        if best.is_none_or(|(_, n)| j - i > n) {
            best = Some((sorted[i], j - i));
        }
        i = j;
    }
    best.map(|(w, _)| w)
}

/// A first row reads as a header where every field is a distinct name and some column below it holds a number or a
/// boolean, which a header's own field cannot.
fn is_header(first: &[String], rest: &[Option<Vec<String>>]) -> bool {
    let mut seen = std::collections::HashSet::new();
    let names = first.iter().all(|f| is_name(f.trim()) && seen.insert(f.trim()));
    names
        && rest.iter().flatten().filter(|r| r.len() == first.len()).any(|r| {
            r.iter()
                .any(|f| !matches!(sniff_scalar(f.trim().as_bytes()), Tag::Str(_)))
        })
}

/// The type a CSV column's values suggest, as a preview suggests one for a field it read (`tag::Suggestion`). An
/// empty field says nothing, and a column of them is `str`.
fn column_type<'a>(values: impl Iterator<Item = &'a str>) -> String {
    let mut seen = Suggestion::default();
    for v in values.map(str::trim).filter(|v| !v.is_empty()) {
        seen.saw(&sniff_scalar(v.as_bytes()));
    }
    match seen.suggest() {
        DataType::Null => DataType::Str,
        ty => ty,
    }
    .to_string()
}

/// The most records [`propose`] reads a format from.
const PROPOSED_FROM: usize = 200;

/// What [`propose`] answers.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Proposal {
    /// The steps that parse the records' text, in order. None where they have none, a source whose records arrive as
    /// fields already, or where no format read any of it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<StepDto>,
    /// The format `steps` read, as the GUI names it: `json`, `cef`, `keyValue`, `csv`, or one of [`LINES`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<&'static str>,
    pub read: usize,
    pub of: usize,
    /// Whether the records came with fields of their own and no text to parse.
    pub structured: bool,
    /// What a route takes this stream's rows under, where every record was filed under one `STREAM_FIELDS` value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
}

/// The first steps of a pipeline for `records`, a captured sample, and the condition a route would take them under.
/// A JSON, Key/value or CSV record gets a Parser step; CSV's columns are the header row's names where the first record
/// is one, and "column_1" onward where it is not, each typed from its values. A source sends a file's header row as a
/// record of its own, which the CSV parser would read as data, so a Drop step ahead of the parser takes it out. CEF
/// has no Parser kind, so it gets a Code step that spreads `cef()` into the record. A syslog or access-log line gets a
/// Grok Parser on the built-in pattern for the whole line, whose fields are that pattern's.
///
/// The format is read from the first [`PROPOSED_FROM`] records, which is plenty to tell one; the condition holds of
/// every record `filed` says where it was filed (each one's `__source`), and of `records` where it is `None`.
pub fn propose(records: &[RecordObject], filed: Option<&[serde_json::Value]>) -> Proposal {
    let records = &records[..records.len().min(PROPOSED_FROM)];
    let raw: Vec<&str> = records.iter().filter_map(|r| r.get("_raw")?.as_str()).collect();
    // Blank lines are what `sniff` skips, so a header is the first line that is not one.
    let texts: Vec<&str> = raw.iter().copied().filter(|t| !t.trim().is_empty()).collect();
    let condition = match filed {
        Some(filed) => stream_condition(filed.iter().map(Some)),
        None => stream_condition(records.iter().map(|r| r.get("__source"))),
    };
    if texts.is_empty() {
        return Proposal {
            steps: Vec::new(),
            format: None,
            read: 0,
            of: 0,
            structured: raw.is_empty() && !records.is_empty(),
            condition,
        };
    }
    let sniffed = sniff(texts.iter().copied());
    let (steps, format) = match &sniffed.format {
        None => (Vec::new(), None),
        Some(Format::Json) => (
            vec![parser(ParserKindDto::Json { split_dots: None }, Vec::new())],
            Some("json"),
        ),
        Some(Format::KeyValue) => {
            let c = KvConfig::default();
            let kind = ParserKindDto::KeyValue {
                pair_sep: (c.pair_sep as char).to_string(),
                kv_sep: (c.kv_sep as char).to_string(),
                quote: c.quote.map(|q| (q as char).to_string()),
                next_key: None,
                split_dots: None,
            };
            (vec![parser(kind, Vec::new())], Some("keyValue"))
        }
        Some(Format::Line(format)) => {
            let (_, pattern) = LINES
                .iter()
                .find(|(f, _)| f == format)
                .expect("a line format is one of LINES");
            let kind = ParserKindDto::Grok {
                patterns: line_config(pattern).patterns,
                custom_patterns: Vec::new(),
                split_dots: None,
            };
            (vec![parser(kind, Vec::new())], Some(*format))
        }
        Some(Format::Cef) => (
            vec![StepDto::Code {
                id: "parse".to_string(),
                filter: None,
                disabled: false,
                body: "Object.assign($, cef(_raw));".to_string(),
            }],
            Some("cef"),
        ),
        Some(Format::Csv {
            delimiter,
            header,
            columns,
        }) => {
            let rows: Vec<Vec<String>> = texts
                .iter()
                .skip(usize::from(header.is_some()))
                .filter_map(|t| csv_fields(t, *delimiter))
                .filter(|r| r.len() == *columns)
                .collect();
            let declared = (0..*columns)
                .map(|i| FieldDto {
                    name: header
                        .as_ref()
                        .map_or_else(|| format!("column_{}", i + 1), |h| h[i].trim().to_string()),
                    ty: DataTypeDto::Scalar(column_type(rows.iter().map(|r| r[i].as_str()))),
                })
                .collect();
            let kind = ParserKindDto::Csv {
                delimiter: (*delimiter as char).to_string(),
                split_dots: None,
            };
            let mut steps = Vec::new();
            if header.is_some() {
                let line = serde_json::to_string(texts[0]).expect("a string serializes");
                steps.push(StepDto::Drop {
                    id: "drop-header".to_string(),
                    filter: None,
                    disabled: false,
                    condition: format!("_raw == {line}"),
                });
            }
            steps.push(parser(kind, declared));
            (steps, Some("csv"))
        }
    };
    Proposal {
        steps,
        format,
        read: sniffed.read,
        of: sniffed.of,
        structured: false,
        condition,
    }
}

fn parser(kind: ParserKindDto, declared: Vec<FieldDto>) -> StepDto {
    StepDto::Parser {
        id: "parse".to_string(),
        filter: None,
        disabled: false,
        input: "_raw".to_string(),
        kind,
        declared,
    }
}

/// `__source.<field> == "<value>"` for the first of `STREAM_FIELDS` every record carries with one value, so a route
/// takes this stream and leaves the other streams a receiver carries to their own routes.
/// `filed` is each record's `__source`.
fn stream_condition<'a>(filed: impl IntoIterator<Item = Option<&'a serde_json::Value>>) -> Option<String> {
    let filed: Vec<Option<&serde_json::Value>> = filed.into_iter().collect();
    if filed.is_empty() {
        return None;
    }
    STREAM_FIELDS.iter().find_map(|field| {
        let mut values = filed.iter().map(|f| (*f)?.get(*field)?.as_str());
        let first = values.next()??;
        if first.is_empty() || !values.all(|v| v == Some(first)) {
            return None;
        }
        let literal = serde_json::to_string(first).expect("a string serializes");
        Some(format!("__source.{field} == {literal}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn raw(texts: &[&str]) -> Vec<RecordObject> {
        texts
            .iter()
            .map(|t| json!({ "_raw": t }).as_object().unwrap().clone())
            .collect()
    }

    #[test]
    fn json_records_read_as_json() {
        let s = sniff([r#"{"a": 1, "b": "x=y z=w"}"#, r#"{"a": 2}"#, ""]);
        assert_eq!((s.format, s.read, s.of), (Some(Format::Json), 2, 2));
    }

    #[test]
    fn cef_reads_as_cef_and_not_as_key_value() {
        let line = "CEF:0|Acme|fw|1.0|100|blocked|5|src=10.0.0.1 dst=10.0.0.2 act=deny";
        let s = sniff([line, line]);
        assert_eq!(s.format, Some(Format::Cef));
    }

    #[test]
    fn key_value_needs_two_named_pairs() {
        let s = sniff(["user=jlee action=login ok=true", "user=app action=logout ok=false"]);
        assert_eq!(s.format, Some(Format::KeyValue));
        assert!(!is_key_value("the total = 4"), "prose with one =");
        assert!(!is_key_value("1=2 3=4"), "keys must be names");
    }

    #[test]
    fn csv_finds_its_delimiter_and_its_header() {
        let s = sniff(["ts;host;bytes", "2026-10-15;web-1;512", "2026-10-15;web-2;9"]);
        assert_eq!(
            s.format,
            Some(Format::Csv {
                delimiter: b';',
                header: Some(vec!["ts".into(), "host".into(), "bytes".into()]),
                columns: 3
            })
        );
        let headerless = sniff(["web-1,512,true", "web-2,9,false"]);
        assert!(matches!(
            headerless.format,
            Some(Format::Csv {
                delimiter: b',',
                header: None,
                columns: 3
            })
        ));
    }

    #[test]
    fn prose_reads_as_nothing() {
        let s = sniff(["connection reset by peer (os error 104)", "can't open /var/log/x"]);
        assert_eq!((s.format, s.read, s.of), (None, 0, 2));
    }

    #[test]
    fn a_csv_proposal_types_its_columns_from_their_values() {
        let p = propose(&raw(&["host,bytes,ratio,ok", "a,1,0.5,true", "b,2,1,false"]), None);
        let Some(StepDto::Parser {
            kind: ParserKindDto::Csv { delimiter, .. },
            declared,
            ..
        }) = p.steps.last().cloned()
        else {
            panic!("a CSV parser: {p:?}")
        };
        assert_eq!(delimiter, ",");
        let got: Vec<(String, String)> = declared
            .into_iter()
            .map(|f| match f.ty {
                DataTypeDto::Scalar(t) => (f.name, t),
                DataTypeDto::Compound(_) => unreachable!(),
            })
            .collect();
        assert_eq!(
            got,
            [("host", "str"), ("bytes", "i64"), ("ratio", "f64"), ("ok", "bool")]
                .map(|(a, b)| (a.to_string(), b.to_string()))
        );
        assert_eq!((p.format, p.read, p.of), (Some("csv"), 3, 3));
        assert!(
            matches!(&p.steps[0], StepDto::Drop { condition, .. } if condition == r#"_raw == "host,bytes,ratio,ok""#),
            "the header row is dropped before the parser: {:?}",
            p.steps[0]
        );
    }

    #[test]
    fn headerless_csv_drops_nothing() {
        let p = propose(&raw(&["web-1,512,true", "web-2,9,false"]), None);
        assert!(matches!(p.steps.as_slice(), [StepDto::Parser { .. }]), "{:?}", p.steps);
    }

    #[test]
    fn a_blank_first_record_leaves_the_header_a_header() {
        let p = propose(&raw(&["", "host,bytes", "a,1", "b,2"]), None);
        let Some(StepDto::Parser { declared, .. }) = p.steps.last().cloned() else {
            panic!("a CSV parser: {p:?}")
        };
        let types: Vec<String> = declared
            .into_iter()
            .map(|f| match f.ty {
                DataTypeDto::Scalar(t) => t,
                DataTypeDto::Compound(_) => unreachable!(),
            })
            .collect();
        assert_eq!(types, ["str", "i64"], "the header was typed as a row");
    }

    #[test]
    fn cef_is_proposed_as_a_code_step() {
        let p = propose(&raw(&["CEF:0|Acme|fw|1.0|100|blocked|5|src=10.0.0.1"]), None);
        assert!(matches!(p.steps.as_slice(), [StepDto::Code { body, .. }] if body.contains("cef(_raw)")));
    }

    /// A syslog or access-log line is proposed as a Grok Parser on its whole-line pattern, the stricter of two that
    /// both read it winning.
    #[test]
    fn a_known_line_is_proposed_as_its_grok_pattern() {
        let cases = [
            (
                "<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed",
                "syslog",
                "SYSLOGLINE",
            ),
            (
                "<165>1 2003-10-11T22:14:15.003Z host app - ID47 - An event",
                "syslog5424",
                "SYSLOG5424LINE",
            ),
            (
                r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 200 2326"#,
                "commonLog",
                "COMMONAPACHELOG",
            ),
            (
                r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 200 2326 "-" "curl/8.0""#,
                "combinedLog",
                "COMBINEDAPACHELOG",
            ),
        ];
        for (line, format, pattern) in cases {
            let p = propose(&raw(&[line, line]), None);
            assert_eq!((p.format, p.read, p.of), (Some(format), 2, 2), "{line}");
            let want = vec![format!("^%{{{pattern}}}$")];
            assert!(
                matches!(p.steps.as_slice(), [StepDto::Parser { kind: ParserKindDto::Grok { patterns, .. }, .. }]
                    if *patterns == want),
                "{line}: {:?}",
                p.steps
            );
        }
        let cef = "Oct 15 00:31:02 fw1 CEF:0|Acme|fw|1.0|100|blocked|5|src=10.0.0.1";
        assert_eq!(
            propose(&raw(&[cef]), None).format,
            Some("cef"),
            "CEF reads past a syslog header"
        );
    }

    #[test]
    fn records_with_fields_and_no_text_need_no_parser() {
        let records = vec![json!({ "id": 1, "name": "a" }).as_object().unwrap().clone()];
        let p = propose(&records, None);
        assert!(p.steps.is_empty() && p.structured);
    }

    #[test]
    fn one_sourcetype_becomes_the_routes_condition() {
        let mut records = raw(&["{}", "{}"]);
        for r in &mut records {
            r.insert(
                "__source".into(),
                json!({ "sourcetype": "fw:\"json\"", "index": "main" }),
            );
        }
        assert_eq!(
            stream_condition(records.iter().map(|r| r.get("__source"))).as_deref(),
            Some(r#"__source.sourcetype == "fw:\"json\"""#)
        );
        records[1].insert("__source".into(), json!({ "sourcetype": "other", "index": "main" }));
        assert_eq!(
            stream_condition(records.iter().map(|r| r.get("__source"))).as_deref(),
            Some(r#"__source.index == "main""#),
            "the next field"
        );
        records[1].insert("__source".into(), json!({}));
        assert_eq!(stream_condition(records.iter().map(|r| r.get("__source"))), None);
    }

    #[test]
    fn the_condition_holds_for_every_record_filed_not_only_those_read() {
        let records = raw(&["{}"; PROPOSED_FROM]);
        let mut filed = vec![json!({ "sourcetype": "fw" }); PROPOSED_FROM + 1];
        assert_eq!(
            propose(&records, Some(&filed)).condition.as_deref(),
            Some(r#"__source.sourcetype == "fw""#)
        );
        filed[PROPOSED_FROM] = json!({ "sourcetype": "other" });
        assert_eq!(propose(&records, Some(&filed)).condition, None);
    }

    #[test]
    fn a_line_wider_than_a_parser_may_name_is_not_csv() {
        let wide = vec!["x"; STREAM_FIELDS_PER_PARSER + 1].join(",");
        let s = sniff([wide.as_str(), wide.as_str()]);
        assert!(!matches!(s.format, Some(Format::Csv { .. })));
    }
}
