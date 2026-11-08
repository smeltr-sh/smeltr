//! `unpack` against layouts drawn from every option it takes, over messages written for the layout, the same cut
//! short, and arbitrary bytes. Each row must give what a plain decoder here reads from the same bytes, raise the
//! exceptions it raises, and never panic. A message `unpack` read whole must `pack` back to the same bytes,
//! except where a text field does not decode and encode back to its own bytes.

use base64::Engine;
use serde_json::{Map, Value, json};
use smeltr::core::buffer::BufferPool;
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ErrorSink;
use smeltr::plan::compile::compile;
use smeltr::plan::output::run_to_json_marked;
use smeltr::plan::topology::SourceMeta;

use crate::harness::{
    self,
    fuzz::{self, Coverage, Rng},
};

const SEEDS: u64 = 300;

/// Messages per layout.
const ROWS: usize = 12;

const INTS: [&str; 8] = ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"];
const FLOATS: [&str; 2] = ["f32", "f64"];
const CHARSETS: [&str; 3] = ["utf-8", "latin1", "utf-16le"];

/// Every option a layout takes, and every way a message can end.
const OPTIONS: &[&str] = &[
    "u8",
    "u16",
    "u32",
    "u64",
    "i8",
    "i16",
    "i32",
    "i64",
    "f32",
    "f64",
    "bytes",
    "str",
    "order big",
    "order little",
    "default order",
    "call order little",
    "length count",
    "length field",
    "prefix",
    "rest",
    "charset",
    "whole",
    "cut short",
    "arbitrary",
    "IndexOutOfRange",
    "Overflow",
    "NaN",
    "packed back",
];

#[derive(Clone, Copy, PartialEq)]
enum Order {
    Big,
    Little,
}

#[derive(Clone)]
enum Len {
    Count(usize),
    Field(usize),
    Prefix(&'static str, Order),
    Rest,
}

#[derive(Clone)]
enum Field {
    Num(&'static str, Order),
    Data { charset: Option<&'static str>, len: Len },
}

/// A layout, as its fields and the literal `unpack` and `pack` take.
struct Layout {
    fields: Vec<Field>,
    literal: String,
}

fn layout(rng: &mut Rng, reached: &mut Coverage) -> Layout {
    let call_order = if rng.chance(3) { Order::Little } else { Order::Big };
    if call_order == Order::Little {
        reached.reach("call order little");
    }
    let n = 1 + rng.below(6);
    let mut fields = Vec::new();
    let mut parts = Vec::new();
    for i in 0..n {
        // A field's own order, or the call's.
        let order = |rng: &mut Rng, reached: &mut Coverage| match rng.below(3) {
            0 => {
                reached.reach("default order");
                (call_order, "")
            }
            1 => {
                reached.reach("order big");
                (Order::Big, " big")
            }
            _ => {
                reached.reach("order little");
                (Order::Little, " little")
            }
        };
        let last = i == n - 1;
        let (field, spelled) = if rng.chance(2) {
            let ty = if rng.chance(4) {
                *rng.pick(&FLOATS)
            } else {
                *rng.pick(&INTS)
            };
            reached.reach(ty);
            let (o, said) = order(rng, reached);
            (Field::Num(ty, o), format!("\"{ty}{said}\""))
        } else {
            let text = rng.chance(2);
            let ty = if text { "str" } else { "bytes" };
            reached.reach(ty);
            let mut charset = (text && rng.chance(2)).then(|| *rng.pick(&CHARSETS));
            let earlier: Vec<usize> = (0..i)
                .filter(|&j| matches!(&fields[j], Field::Num(t, _) if INTS.contains(t)))
                .collect();
            let (len, said) = match rng.below(4) {
                0 => {
                    let c = rng.below(6);
                    (Len::Count(c), c.to_string())
                }
                1 if !earlier.is_empty() => {
                    let j = *rng.pick(&earlier);
                    (Len::Field(j), format!("f{j}"))
                }
                2 => {
                    let t = *rng.pick(&INTS);
                    let (o, said) = order(rng, reached);
                    (Len::Prefix(t, o), format!("{t}{said}"))
                }
                // A charset follows a length, so text that runs to the end is UTF-8.
                _ if last => {
                    charset = None;
                    (Len::Rest, String::new())
                }
                _ => {
                    let c = rng.below(6);
                    (Len::Count(c), c.to_string())
                }
            };
            reached.reach(match len {
                Len::Count(_) => "length count",
                Len::Field(_) => "length field",
                Len::Prefix(..) => "prefix",
                Len::Rest => "rest",
            });
            let spelled = match charset {
                _ if said.is_empty() => format!("\"{ty}\""),
                None => format!("\"{ty}({said})\""),
                Some(c) => {
                    reached.reach("charset");
                    format!("\"{ty}({said}, {c})\"")
                }
            };
            (
                Field::Data {
                    charset: charset.or(text.then_some("utf-8")),
                    len,
                },
                spelled,
            )
        };
        fields.push(field);
        parts.push(format!("f{i}: {spelled}"));
    }
    let options = match call_order {
        Order::Little => ", {order: \"little\"}",
        Order::Big if rng.chance(2) => ", {order: \"big\"}",
        Order::Big => "",
    };
    Layout {
        literal: format!("{{{}}}{options}", parts.join(", ")),
        fields,
    }
}

fn width(ty: &str) -> usize {
    match ty {
        "u8" | "i8" => 1,
        "u16" | "i16" => 2,
        "u32" | "i32" | "f32" => 4,
        _ => 8,
    }
}

fn put(out: &mut Vec<u8>, value: u64, w: usize, o: Order) {
    let be = value.to_be_bytes();
    let bytes = &be[8 - w..];
    match o {
        Order::Big => out.extend_from_slice(bytes),
        Order::Little => out.extend(bytes.iter().rev()),
    }
}

/// A message written for `layout`: each number drawn from its type, and each length the count it says.
fn message(rng: &mut Rng, layout: &Layout) -> Vec<u8> {
    let mut out = Vec::new();
    let mut ints: Vec<Option<u64>> = Vec::new();
    for (i, field) in layout.fields.iter().enumerate() {
        // An integer another field takes its length from holds a count.
        let a_length = layout.fields[i + 1..]
            .iter()
            .any(|f| matches!(f, Field::Data { len: Len::Field(j), .. } if *j == i));
        match field {
            Field::Num(ty, o) => {
                let w = width(ty);
                let bits = if a_length || rng.chance(2) {
                    rng.below(6) as u64
                } else if ty.starts_with('f') {
                    if rng.chance(8) {
                        if w == 4 {
                            f32::NAN.to_bits() as u64
                        } else {
                            f64::INFINITY.to_bits()
                        }
                    } else if w == 4 {
                        (((rng.below(64) as f32) - 32.0) / 4.0).to_bits() as u64
                    } else {
                        (((rng.below(64) as f64) - 32.0) / 8.0).to_bits()
                    }
                } else {
                    rng.next_u64() >> (64 - 8 * w)
                };
                put(&mut out, bits, w, *o);
                ints.push(Some(bits));
            }
            Field::Data { len, .. } => {
                let n = match len {
                    Len::Count(c) => *c,
                    Len::Field(j) => ints[*j].unwrap_or(0) as usize,
                    Len::Prefix(t, o) => {
                        let n = rng.below(6);
                        put(&mut out, n as u64, width(t), *o);
                        n
                    }
                    Len::Rest => rng.below(6),
                };
                out.extend((0..n).map(|_| b"azAZ09 \xc3\xa9\xff"[rng.below(10)]));
                ints.push(None);
            }
        }
    }
    out
}

/// What `unpack` gives for `b`, read here as plainly as it can be, the exceptions it raises, and whether each text
/// field writes back to its own bytes.
fn decode(layout: &Layout, b: &[u8]) -> (Value, Vec<&'static str>, bool) {
    let mut lossless = true;
    let mut out = Map::new();
    let mut raised = Vec::new();
    let mut ints: Vec<Option<i64>> = Vec::new();
    let mut at = Some(0usize);
    // The integer of `ty` at `at`, with the `Overflow` of a `u64` above `i64::MAX`; `Err` where the bytes end first.
    let int = |at: usize, ty: &str, o: Order| -> Result<(i64, Option<&'static str>), ()> {
        let w = width(ty);
        let bytes = b.get(at..at + w).ok_or(())?;
        let mut wide = [0u8; 8];
        for (k, byte) in bytes.iter().enumerate() {
            let pos = match o {
                Order::Big => 8 - w + k,
                Order::Little => 7 - k,
            };
            wide[pos] = *byte;
        }
        let raw = u64::from_be_bytes(wide);
        Ok(match ty {
            "u64" if raw > i64::MAX as u64 => (0, Some("Overflow")),
            t if t.starts_with('u') => (raw as i64, None),
            _ => {
                let shift = 64 - 8 * w as u32;
                (((raw << shift) as i64) >> shift, None)
            }
        })
    };
    for (i, field) in layout.fields.iter().enumerate() {
        let name = format!("f{i}");
        ints.push(None);
        let Some(pos) = at else { continue };
        match field {
            Field::Num(ty, o) => {
                let w = width(ty);
                if pos + w > b.len() {
                    raised.push("IndexOutOfRange");
                    at = None;
                    continue;
                }
                at = Some(pos + w);
                if ty.starts_with('f') {
                    let bytes = &b[pos..pos + w];
                    let f = match (w, o) {
                        (4, Order::Big) => f32::from_be_bytes(bytes.try_into().unwrap()) as f64,
                        (4, Order::Little) => f32::from_le_bytes(bytes.try_into().unwrap()) as f64,
                        (_, Order::Big) => f64::from_be_bytes(bytes.try_into().unwrap()),
                        (_, Order::Little) => f64::from_le_bytes(bytes.try_into().unwrap()),
                    };
                    if f.is_finite() {
                        // Read back through serde_json, as the rows are: it can parse a float's shortest text to the
                        // neighbouring f64, where Rust's own parse gives the float that text was written from.
                        out.insert(name, serde_json::from_str(&format!("{f:?}")).expect("a float's text"));
                    } else {
                        raised.push("NaN");
                    }
                } else {
                    match int(pos, ty, *o).expect("in range") {
                        (_, Some(kind)) => raised.push(kind),
                        (v, None) => {
                            ints[i] = Some(v);
                            out.insert(name, json!(v));
                        }
                    }
                }
            }
            Field::Data { charset, len } => {
                let (start, n) = match len {
                    Len::Count(c) => (pos, Some(*c as i64)),
                    Len::Field(j) => (pos, ints[*j]),
                    Len::Prefix(t, o) => match int(pos, t, *o) {
                        Err(()) => {
                            raised.push("IndexOutOfRange");
                            at = None;
                            continue;
                        }
                        Ok((_, Some(kind))) => {
                            raised.push(kind);
                            (pos + width(t), None)
                        }
                        Ok((v, None)) => (pos + width(t), Some(v)),
                    },
                    Len::Rest => (pos, Some(b.len() as i64 - pos as i64)),
                };
                // A length with no value reads nothing and raises nothing more.
                let Some(n) = n else {
                    at = None;
                    continue;
                };
                let end = usize::try_from(n).ok().map(|n| start + n).filter(|&end| end <= b.len());
                let Some(end) = end else {
                    raised.push("IndexOutOfRange");
                    at = None;
                    continue;
                };
                let bytes = &b[start..end];
                let value = match charset {
                    None => json!(base64::engine::general_purpose::STANDARD.encode(bytes)),
                    Some(c) => {
                        let encoding = encoding_rs::Encoding::for_label(c.as_bytes()).expect("a charset");
                        let text = encoding.decode_without_bom_handling(bytes).0;
                        let back: Vec<u8> = if encoding == encoding_rs::UTF_16LE {
                            text.encode_utf16().flat_map(u16::to_le_bytes).collect()
                        } else {
                            encoding.encode(&text).0.into_owned()
                        };
                        lossless &= back == bytes;
                        json!(text)
                    }
                };
                out.insert(name, value);
                at = Some(end);
            }
        }
    }
    (Value::Object(out), raised, lossless)
}

fn check(seed: u64, reached: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let layout = layout(&mut rng, reached);
    // Each message, and whether it is whole: written for the layout, with nothing cut or left over.
    let messages: Vec<(Vec<u8>, bool)> = (0..ROWS)
        .map(|_| match rng.below(3) {
            0 => {
                reached.reach("whole");
                (message(&mut rng, &layout), true)
            }
            1 => {
                reached.reach("cut short");
                let mut m = message(&mut rng, &layout);
                m.truncate(rng.below(m.len() + 1));
                (m, false)
            }
            _ => {
                reached.reach("arbitrary");
                ((0..rng.below(40)).map(|_| rng.next_u64() as u8).collect(), false)
            }
        })
        .collect();
    let hex: Vec<String> = messages
        .iter()
        .map(|(m, _)| m.iter().map(|b| format!("{b:02x}")).collect())
        .collect();
    let texts: Vec<&str> = hex.iter().map(String::as_str).collect();

    let body = format!(
        "out = unpack(parseBytes(_raw, \"hex\"), {}); delete(_raw);",
        layout.literal
    );
    let (rows, errors) = run(&body, &texts)?;
    let back_body = format!(
        "out = toString(unpack(parseBytes(_raw, \"hex\"), {0}).pack({0}), \"hex\"); delete(_raw);",
        layout.literal
    );
    let (backs, _) = run(&back_body, &texts)?;
    for (row, (got, (m, whole))) in rows.iter().zip(&messages).enumerate() {
        let (want, want_raised, lossless) = decode(&layout, m);
        let mut raised: Vec<&str> = (errors.entries().iter())
            .filter(|e| e.row as usize == row)
            .map(|e| e.kind.label())
            .collect();
        let mut want_raised = want_raised;
        for kind in &want_raised {
            reached.reach(*kind);
        }
        raised.sort_unstable();
        want_raised.sort_unstable();
        let got = got.clone().unwrap_or(json!({}));
        if got != want || raised != want_raised {
            return Err(format!(
                "{body}\nmessage {}: gave {got} raising {raised:?}, where it reads {want} raising {want_raised:?}",
                hex[row]
            ));
        }
        if *whole && want_raised.is_empty() && lossless {
            reached.reach("packed back");
            let back = backs[row].clone().unwrap_or(Value::Null);
            if back != json!(hex[row]) {
                return Err(format!("{back_body}\nmessage {}: packed back as {back}", hex[row]));
            }
        }
    }
    Ok(())
}

/// Each row's `out` from running `body` on the messages `texts` in `_raw`, and the exceptions they raised.
fn run(body: &str, texts: &[&str]) -> Result<(Vec<Option<Value>>, ErrorSink), String> {
    let plan = compile(&harness::pipeline(vec![harness::code(0, body)]), &SourceMeta::KAFKA).map_err(|errs| {
        format!(
            "{body} does not compile: {}",
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ")
        )
    })?;
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = harness::raw_batch(&plan, texts, &mut pool);
    let rows: Vec<Option<Value>> = {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        run_to_json_marked(&plan, &Default::default(), &mut batch, &mut ctx, false, None)
            .iter()
            .filter_map(|r| r.json.as_ref())
            .map(|row| serde_json::from_slice::<Value>(row).unwrap())
            .map(|row| row.get("out").cloned())
            .collect()
    };
    batch.reclaim(&mut pool);
    if rows.len() != texts.len() {
        return Err(format!("{body}: {} rows for {} messages", rows.len(), texts.len()));
    }
    Ok((rows, errors))
}

#[test]
fn unpack_reads_every_layout_as_a_plain_decoder_does_fuzz() {
    let mut reached = Coverage::new(OPTIONS.iter().copied());
    fuzz::run(0..SEEDS, |seed| check(seed, &mut reached));
    reached.assert_complete();
}
