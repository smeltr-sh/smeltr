//! Random samples through the format detector and the proposal built on it.
//!
//! Each seed writes a sample in one format, with each of that format's options drawn, and checks that the detector
//! names that format, and for CSV its delimiter, header and width. Then it damages some records -- cut short, a byte
//! flipped, a stray line of prose -- and checks only conduct: no panic, `read` never past `of`, and a proposal whose
//! step is the detector's format.
//!
//! Seeded through `crate::fuzz`: a failure names its seed and the sample it wrote, and `Coverage` fails the run when
//! a format, or an option of one, was never drawn.

use serde_json::json;

use super::{CSV_DELIMITERS, Format, propose, sniff};
use crate::config_file::{ParserKindDto, StepDto};
use crate::fuzz::{Coverage, Rng};
use crate::io::RecordObject;

const WORDS: &[&str] = &[
    "web-1",
    "allow",
    "deny",
    "jlee",
    "svc backup",
    "a,b",
    "x;y",
    "tab\there",
    "q\"uote",
    "ünï",
];

fn value(rng: &mut Rng, cov: &mut Coverage) -> serde_json::Value {
    match rng.below(4) {
        0 => {
            cov.reach("value int");
            json!(rng.below(100_000) as i64 - 50)
        }
        1 => {
            cov.reach("value float");
            json!(rng.below(1000) as f64 / 8.0 + 0.5)
        }
        2 => {
            cov.reach("value bool");
            json!(rng.chance(2))
        }
        _ => {
            cov.reach("value text");
            json!(rng.pick(WORDS))
        }
    }
}

/// A sample in one format, and the format the detector must name.
fn sample(rng: &mut Rng, cov: &mut Coverage) -> (Vec<String>, Format) {
    let rows = 2 + rng.below(12);
    match rng.below(5) {
        0 => {
            cov.reach("json");
            let records = (0..rows)
                .map(|_| {
                    let mut o = serde_json::Map::new();
                    for k in 0..1 + rng.below(6) {
                        o.insert(format!("f{k}"), value(rng, cov));
                    }
                    if rng.chance(3) {
                        cov.reach("json nested");
                        o.insert("inner".into(), json!({ "a": [1, 2], "b": "k=v x=y" }));
                    }
                    serde_json::Value::Object(o).to_string()
                })
                .collect();
            (records, Format::Json)
        }
        1 => {
            cov.reach("cef");
            let records = (0..rows)
                .map(|i| {
                    let ext: Vec<String> = (0..rng.below(5)).map(|k| format!("k{k}={}", rng.below(999))).collect();
                    if rng.chance(3) {
                        cov.reach("cef after a syslog header");
                        format!(
                            "Oct 15 00:31:0{} fw1 CEF:0|Acme|fw|1.0|{i}|blocked|5|{}",
                            i % 10,
                            ext.join(" ")
                        )
                    } else {
                        format!("CEF:0|Acme|fw|1.0|{i}|blocked|5|{}", ext.join(" "))
                    }
                })
                .collect();
            (records, Format::Cef)
        }
        3 => {
            let (format, records) = match rng.below(4) {
                0 => {
                    cov.reach("syslog");
                    let records = (0..rows)
                        .map(|i| {
                            let pri = if rng.chance(2) {
                                format!("<{}>", rng.below(192))
                            } else {
                                String::new()
                            };
                            let tag = match rng.below(3) {
                                0 => " sshd[1234]:".to_string(),
                                1 => " kernel:".to_string(),
                                _ => String::new(),
                            };
                            format!(
                                "{pri}Oct {:>2} 00:31:0{} {}{tag} {}",
                                1 + i,
                                i % 10,
                                rng.pick(&["fw1", "10.0.0.1"]),
                                rng.pick(WORDS)
                            )
                        })
                        .collect();
                    ("syslog", records)
                }
                1 => {
                    cov.reach("syslog 5424");
                    let records = (0..rows)
                        .map(|i| {
                            let sd = if rng.chance(2) {
                                format!("[a@1 n=\"{i}\"]")
                            } else {
                                "-".into()
                            };
                            format!(
                                "<{}>1 2026-10-11T22:14:{:02}Z host app {i} - {sd} {}",
                                rng.below(192),
                                i % 60,
                                rng.pick(WORDS)
                            )
                        })
                        .collect();
                    ("syslog5424", records)
                }
                n => {
                    let combined = n == 2;
                    cov.reach(if combined { "combined log" } else { "common log" });
                    let records = (0..rows)
                        .map(|i| {
                            let bytes = if rng.chance(3) {
                                "-".to_string()
                            } else {
                                rng.below(99_999).to_string()
                            };
                            let line = format!(
                                "10.0.0.{i} - {} [10/Oct/2000:13:55:3{} -0700] \"{} /p/{i} HTTP/1.1\" {} {bytes}",
                                rng.pick(&["-", "frank"]),
                                i % 10,
                                rng.pick(&["GET", "POST"]),
                                rng.pick(&[200, 304, 404, 500]),
                            );
                            if combined {
                                format!("{line} \"-\" \"curl/8.0 (x; y)\"")
                            } else {
                                line
                            }
                        })
                        .collect();
                    (if combined { "combinedLog" } else { "commonLog" }, records)
                }
            };
            (records, Format::Line(format))
        }
        2 => {
            cov.reach("key value");
            let keys = 2 + rng.below(5);
            let records = (0..rows)
                .map(|_| {
                    (0..keys)
                        .map(|k| match rng.below(3) {
                            0 => format!("key{k}={}", rng.below(999)),
                            1 => {
                                cov.reach("key value quoted");
                                format!("key{k}=\"two words\"")
                            }
                            _ => format!("key_{k}={}", rng.pick(&["allow", "deny", "true"])),
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect();
            (records, Format::KeyValue)
        }
        _ => {
            let delimiter = *rng.pick(&CSV_DELIMITERS);
            cov.reach(format!("csv {}", delimiter as char).replace('\t', "tab"));
            let columns = 2 + rng.below(6);
            let d = (delimiter as char).to_string();
            // Each column holds one kind of value, the first a number, so a header row reads as one.
            let kinds: Vec<usize> = (0..columns).map(|c| if c == 0 { 0 } else { rng.below(3) }).collect();
            let mut records: Vec<String> = (0..rows)
                .map(|_| {
                    kinds
                        .iter()
                        .map(|k| match k {
                            0 => rng.below(10_000).to_string(),
                            1 => format!("{}.5", rng.below(100)),
                            _ => rng.pick(&["alpha", "beta", "gamma"]).to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(&d)
                })
                .collect();
            let header = rng
                .chance(2)
                .then(|| (0..columns).map(|c| format!("col{c}")).collect::<Vec<_>>());
            if let Some(h) = &header {
                cov.reach("csv header");
                records.insert(0, h.join(&d));
            } else {
                cov.reach("csv no header");
            }
            (
                records,
                Format::Csv {
                    delimiter,
                    header,
                    columns,
                },
            )
        }
    }
}

fn damage(rng: &mut Rng, records: &mut [String], cov: &mut Coverage) {
    for r in records.iter_mut() {
        match rng.below(6) {
            0 => {
                cov.reach("cut short");
                let at = rng.below(r.len() + 1);
                let at = (0..=at).rev().find(|i| r.is_char_boundary(*i)).unwrap_or(0);
                r.truncate(at);
            }
            1 => {
                cov.reach("byte flipped");
                let mut bytes = r.clone().into_bytes();
                if !bytes.is_empty() {
                    let i = rng.below(bytes.len());
                    let pool = b"{}=,;|\t\"\\ x";
                    bytes[i] = pool[rng.below(pool.len())];
                }
                *r = String::from_utf8_lossy(&bytes).into_owned();
            }
            2 => {
                cov.reach("prose");
                *r = "connection reset by peer (os error 104)".into();
            }
            _ => {}
        }
    }
}

fn objects(texts: &[String]) -> Vec<RecordObject> {
    texts
        .iter()
        .map(|t| {
            json!({ "_raw": t, "__source": { "sourcetype": "fw" } })
                .as_object()
                .unwrap()
                .clone()
        })
        .collect()
}

#[test]
fn fuzz_sniff_names_the_format_it_was_given() {
    let mut cov = Coverage::new([
        "json",
        "json nested",
        "cef",
        "cef after a syslog header",
        "key value",
        "key value quoted",
        "csv ,",
        "csv tab",
        "csv ;",
        "csv |",
        "csv header",
        "csv no header",
        "syslog",
        "syslog 5424",
        "common log",
        "combined log",
        "value int",
        "value float",
        "value bool",
        "value text",
        "cut short",
        "byte flipped",
        "prose",
    ]);
    crate::fuzz::run(0..400, |seed| {
        let mut rng = Rng::seeded(seed);
        let (mut records, want) = sample(&mut rng, &mut cov);

        let got = sniff(records.iter().map(String::as_str));
        if got.format.as_ref() != Some(&want) || got.read != records.len() {
            return Err(format!("wrote {want:?}, read {got:?}, sample {records:#?}"));
        }
        let proposal = propose(&objects(&records), None);
        let step_matches = match (&want, proposal.steps.last()) {
            (
                Format::Json,
                Some(StepDto::Parser {
                    kind: ParserKindDto::Json { .. },
                    ..
                }),
            ) => true,
            (
                Format::KeyValue,
                Some(StepDto::Parser {
                    kind: ParserKindDto::KeyValue { .. },
                    ..
                }),
            ) => true,
            (Format::Cef, Some(StepDto::Code { .. })) => true,
            (
                Format::Line(_),
                Some(StepDto::Parser {
                    kind: ParserKindDto::Grok { .. },
                    ..
                }),
            ) => true,
            (
                Format::Csv { columns, header, .. },
                Some(StepDto::Parser {
                    kind: ParserKindDto::Csv { .. },
                    declared,
                    ..
                }),
            ) => {
                // A header row is dropped ahead of the parser, and nothing is where there is none.
                let dropped = matches!(proposal.steps.as_slice(), [StepDto::Drop { .. }, _]);
                declared.len() == *columns && dropped == header.is_some()
            }
            _ => false,
        };
        if !step_matches || proposal.condition.as_deref() != Some(r#"__source.sourcetype == "fw""#) {
            return Err(format!("wrote {want:?}, proposed {proposal:?}"));
        }

        damage(&mut rng, &mut records, &mut cov);
        let got = sniff(records.iter().map(String::as_str));
        if got.read > got.of {
            return Err(format!("read {} of {}: {records:#?}", got.read, got.of));
        }
        let proposal = propose(&objects(&records), None);
        if proposal.steps.is_empty() == got.format.is_some() {
            return Err(format!("detected {got:?}, proposed {proposal:?}"));
        }
        Ok(())
    });
    cov.assert_complete();
}
