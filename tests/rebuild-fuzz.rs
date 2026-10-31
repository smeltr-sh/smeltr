//! A seeded fuzzer for a Parser that rebuilds its input: a CSV or key-value line drawn under every option the parser
//! takes, rebuilt without the columns or pairs it skips. What it writes must read back, under the same options, as
//! what the parse kept: each kept column or pair as it was, each skipped CSV column empty in its place and each
//! skipped pair gone. Parsing the rebuilt line is the oracle, so a rebuild that splits the line other than the parse
//! does fails here, whatever the compiler lowers it to.

use serde_json::Value;
use smeltr::core::batch::Batch;
use smeltr::plan::config::{CsvConfig, FunctionKind, KvConfig, ParserKind};

use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::{Env, compile_ok, func, parsed, seed_raw};

const SEEDS: u64 = 400;

/// The record `kind` parses `line` into, `_raw` included.
fn parse(kind: ParserKind, line: &str) -> serde_json::Map<String, Value> {
    let step = FunctionKind::Parser {
        input: "_raw".into(),
        into: "$".into(),
        kind,
        declared: Vec::new(),
    };
    let plan = compile_ok(vec![func(0, step)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[line]);
    let rows = env.run(&plan, &mut b);
    b.reclaim(&mut env.pool);
    match parsed(&rows[0]) {
        Value::Object(record) => record,
        other => panic!("a record is an object, not {other}"),
    }
}

/// A column's or a pair's value in `record`, where an empty one and a missing one are both `""`.
fn text(record: &serde_json::Map<String, Value>, name: &str) -> String {
    match record.get(name) {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// A skip mark on each of `n` names, `n` at least two: at least one skipped and one kept.
fn marks(rng: &mut Rng, n: usize) -> Vec<bool> {
    let mut skip: Vec<bool> = (0..n).map(|_| rng.chance(3)).collect();
    let skipped = rng.below(n);
    skip[skipped] = true;
    skip[(skipped + 1 + rng.below(n - 1)) % n] = false;
    skip
}

// -- CSV ------------------------------------------------------------------------------------------------------------

const CSV_OPTIONS: &[&str] = &[
    "delimiter ,",
    "delimiter ;",
    "delimiter |",
    "delimiter tab",
    "delimiter space",
    "quote \"",
    "quote '",
    "escape",
    "doubled quote",
    "squeeze",
    "trim",
    "padded column",
    "quoted column skipped",
    "quoted column kept",
    "first column skipped",
    "last column skipped",
];

/// A CSV line of `n` columns under `c`, and which columns hold a delimiter inside their quotes.
fn csv_line(rng: &mut Rng, c: &CsvConfig, n: usize, reached: &mut Coverage) -> (String, Vec<bool>) {
    let d = c.delimiter as char;
    let q = c.quote as char;
    let mut columns = Vec::new();
    let mut quoted = Vec::new();
    for i in 0..n {
        let kind = rng.below(4);
        quoted.push(kind == 2);
        let mut column = match kind {
            0 | 1 => format!("w{i}"),
            2 => format!("{q}a{d} b{i}{q}"),
            _ => {
                let inner = match c.escape {
                    Some(e) => format!("{}{q}", e as char),
                    None => {
                        reached.reach("doubled quote");
                        format!("{q}{q}")
                    }
                };
                format!("{q}x{inner}y{i}{q}")
            }
        };
        if c.trim && rng.chance(2) {
            reached.reach("padded column");
            column = format!("  {column}");
        }
        columns.push(column);
    }
    (columns.join(&d.to_string()), quoted)
}

fn check_csv(seed: u64, reached: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let (delimiter, name) = *rng.pick(&[
        (b',', "delimiter ,"),
        (b';', "delimiter ;"),
        (b'|', "delimiter |"),
        (b'\t', "delimiter tab"),
        (b' ', "delimiter space"),
    ]);
    reached.reach(name);
    let quote = *rng.pick(b"\"'");
    reached.reach(format!("quote {}", quote as char));
    let escape = rng.chance(2).then_some(b'\\');
    // A run of spaces is one delimiter under squeeze and padding under trim; the two cannot both hold.
    let squeeze = rng.chance(3);
    let trim = delimiter != b' ' && rng.chance(2);
    for (on, name) in [(escape.is_some(), "escape"), (squeeze, "squeeze"), (trim, "trim")] {
        if on {
            reached.reach(name);
        }
    }
    let n = 2 + rng.below(6);
    let c = CsvConfig {
        delimiter,
        quote,
        escape,
        squeeze,
        trim,
        ..CsvConfig::default()
    };
    let (line, quoted) = csv_line(&mut rng, &c, n, reached);
    let skip = marks(&mut rng, n);
    let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
    let columns = |headers: Vec<Option<String>>, rebuild| {
        ParserKind::Csv(CsvConfig {
            headers,
            rebuild,
            ..c.clone()
        })
    };
    let read = parse(columns(names.iter().cloned().map(Some).collect(), false), &line);
    let rebuilt = parse(
        columns(
            names.iter().zip(&skip).map(|(h, &s)| (!s).then(|| h.clone())).collect(),
            true,
        ),
        &line,
    );
    let out = text(&rebuilt, "_raw");
    let kept: Vec<&String> = names.iter().zip(&skip).filter(|(_, s)| !**s).map(|(h, _)| h).collect();
    for h in &kept {
        if text(&rebuilt, h) != text(&read, h) {
            return Err(format!("{h} reads differently with rebuild on\n  line {line:?}"));
        }
    }
    // Read back: under squeeze an empty column is no column, so the kept ones close up; otherwise each keeps its place.
    let again = parse(columns(names.iter().cloned().map(Some).collect(), false), &out);
    let want: Vec<String> = match squeeze {
        true => kept.iter().map(|h| text(&read, h)).collect(),
        false => names
            .iter()
            .zip(&skip)
            .map(|(h, &s)| if s { String::new() } else { text(&read, h) })
            .collect(),
    };
    let got: Vec<String> = names.iter().take(want.len()).map(|h| text(&again, h)).collect();
    if got != want {
        return Err(format!(
            "the rebuilt line reads back as {got:?}, not {want:?}\n  line    {line:?}\n  rebuilt {out:?}\n  skip    {skip:?}"
        ));
    }
    for (i, &s) in skip.iter().enumerate() {
        if quoted[i] {
            reached.reach(if s {
                "quoted column skipped"
            } else {
                "quoted column kept"
            });
        }
        if s && i == 0 {
            reached.reach("first column skipped");
        }
        if s && i == n - 1 {
            reached.reach("last column skipped");
        }
    }
    Ok(())
}

#[test]
fn a_rebuilt_csv_line_reads_back_as_the_parse_kept_it_fuzz() {
    let mut reached = Coverage::new(CSV_OPTIONS.iter().copied());
    fuzz::run(0..SEEDS, |seed| check_csv(seed, &mut reached));
    if !fuzz::replaying() {
        reached.assert_complete();
    }
}

// -- Key-value ------------------------------------------------------------------------------------------------------

const KV_OPTIONS: &[&str] = &[
    "pair separator space",
    "pair separator ,",
    "pair separator ;",
    "separator =",
    "separator :",
    "quote",
    "no quote",
    "escapes",
    "next key",
    "quoted pair skipped",
    "quoted pair kept",
    "first pair skipped",
    "last pair skipped",
];

fn check_kv(seed: u64, reached: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let (pair_sep, name) = *rng.pick(&[
        (b' ', "pair separator space"),
        (b',', "pair separator ,"),
        (b';', "pair separator ;"),
    ]);
    reached.reach(name);
    let kv_sep = *rng.pick(b"=:");
    reached.reach(format!("separator {}", kv_sep as char));
    let quote = match rng.chance(4) {
        true => None,
        false => Some(b'"'),
    };
    reached.reach(if quote.is_some() { "quote" } else { "no quote" });
    let escapes = quote.is_some() && rng.chance(2);
    let next_key = rng.chance(2);
    for (on, name) in [(escapes, "escapes"), (next_key, "next key")] {
        if on {
            reached.reach(name);
        }
    }
    let n = 2 + rng.below(6);
    let (ps, ks) = (pair_sep as char, kv_sep as char);
    let mut quoted = vec![false; n];
    let pairs: Vec<String> = (0..n)
        .map(|i| {
            let value = match quote {
                Some(q) if rng.chance(2) => {
                    quoted[i] = true;
                    let q = q as char;
                    match escapes {
                        true => format!("{q}a{ps}b \\{q}{i}{q}"),
                        false => format!("{q}a{ps}b {i}{q}"),
                    }
                }
                _ => format!("v{i}"),
            };
            format!("k{i}{ks}{value}")
        })
        .collect();
    let line = pairs.join(&ps.to_string());
    let skip = marks(&mut rng, n);
    let keys: Vec<String> = (0..n).map(|i| format!("k{i}")).collect();
    let config = |skip: Vec<String>, rebuild| {
        ParserKind::KeyValue(KvConfig {
            pair_sep,
            kv_sep,
            quote,
            escapes,
            next_key,
            skip,
            rebuild,
            ..KvConfig::default()
        })
    };
    let read = parse(config(Vec::new(), false), &line);
    let skipped: Vec<String> = keys
        .iter()
        .zip(&skip)
        .filter(|(_, s)| **s)
        .map(|(k, _)| k.clone())
        .collect();
    let rebuilt = parse(config(skipped, true), &line);
    let out = text(&rebuilt, "_raw");
    let again = parse(config(Vec::new(), false), &out);
    for (i, (k, &s)) in keys.iter().zip(&skip).enumerate() {
        let (want, got) = match s {
            true => (String::new(), text(&again, k)),
            false => (text(&read, k), text(&again, k)),
        };
        if got != want || (!s && text(&rebuilt, k) != want) {
            return Err(format!(
                "{k} reads back as {got:?}, not {want:?}\n  line    {line:?}\n  rebuilt {out:?}\n  skip    {skip:?}"
            ));
        }
        if quoted[i] {
            reached.reach(if s { "quoted pair skipped" } else { "quoted pair kept" });
        }
        if s && i == 0 {
            reached.reach("first pair skipped");
        }
        if s && i == n - 1 {
            reached.reach("last pair skipped");
        }
    }
    Ok(())
}

#[test]
fn a_rebuilt_key_value_line_reads_back_as_the_parse_kept_it_fuzz() {
    let mut reached = Coverage::new(KV_OPTIONS.iter().copied());
    fuzz::run(0..SEEDS, |seed| check_kv(seed, &mut reached));
    if !fuzz::replaying() {
        reached.assert_complete();
    }
}
