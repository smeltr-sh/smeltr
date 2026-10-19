//! Adversarial input against every parser that sees a record's `_raw`
//! bytes. `_raw` is attacker-controlled, and a panic reached from it
//! diverts a whole batch of good records to the DLQ alongside the bad one,
//! so "never panics on arbitrary bytes" is a correctness property.
//!
//! A seeded fuzzer on `harness::fuzz`, not a `cargo-fuzz` target: no
//! nightly, no corpus, runs in `cargo test`, and a failure prints its seed.

use std::borrow::Cow;

use smeltr::format::json_scan::{self, Scratch, Sink};
use smeltr::format::spread::{self, Step};
use smeltr::format::tag::{Bytes, Tag};
use smeltr::format::{cef_scan, csv_scan, kv_scan};
use smeltr::ops::convert;

use crate::harness::fuzz::{self, Rng};

/// Counts values, so the scan is driven to completion. Keys alternate between being entered or taken whole and being
/// copied, so both the walk and the encoder see the input.
#[derive(Default)]
struct CountingSink {
    leaves: usize,
    keys: usize,
    copied: Vec<u8>,
}

impl<'d> Sink<'d> for CountingSink {
    fn enter(&mut self, _key: Cow<'d, str>, object: bool) -> Step {
        self.keys += 1;
        match (self.keys % 2, object) {
            (0, _) => Step::Copy,
            (_, true) => Step::Into,
            (_, false) => Step::Whole,
        }
    }
    type Leaf<'a> = Bytes<&'a mut Vec<u8>>;
    fn leaf(&mut self) -> Bytes<&mut Vec<u8>> {
        self.leaves += 1;
        self.copied.clear();
        Bytes::new(&mut self.copied)
    }
    fn copy(&mut self) -> (&mut Vec<u8>, usize, bool) {
        self.copied.clear();
        (&mut self.copied, self.keys % 70, self.keys.is_multiple_of(3))
    }
    fn copied(&mut self) {
        self.leaves += 1;
    }
    fn uncopy(&mut self) {}
    fn repeated(&mut self, _path: &[Cow<'d, str>]) {}
    fn leave(&mut self) {}
}

/// The bytes most likely to break a hand-rolled scanner: structural JSON
/// characters, escape machinery, number edge cases, and non-UTF-8. Used as
/// splice material when corrupting an otherwise-valid document.
const INTERESTING: &[&[u8]] = &[
    b"{",
    b"}",
    b"[",
    b"]",
    b":",
    b",",
    b"\"",
    b"\\",
    b"/",
    b"\\u",
    b"\\u0",
    b"\\ud800",
    b"\\udfff",
    b"\\uDBFF\\uDFFF",
    b"\\x",
    b"\xff",
    b"\xc3",
    b"\xc3\x28",
    b"\xe0\x80",
    b"\xf0\x9f\x92\xa9",
    b"\x00",
    b"\x7f",
    b"1e999999999",
    b"-0",
    b"00",
    b"1.",
    b".1",
    b"1e",
    b"1e+",
    b"18446744073709551616",
    b"-9223372036854775809",
    b"\"a\":",
    b"{}",
    b"[]",
    b"null",
    b"true",
    b"false",
];

/// String bodies worth generating: plain text, every escape the scanner
/// implements, valid and lone surrogates, and raw multi-byte UTF-8.
const STRING_PIECES: &[&[u8]] = &[
    b"a",
    b"key",
    b"source.ip",
    b"",
    b" ",
    b".",
    b"..",
    b"\\n",
    b"\\t",
    b"\\r",
    b"\\b",
    b"\\f",
    b"\\/",
    b"\\\\",
    b"\\\"",
    b"\\u0041",
    b"\\u00e9",
    b"\\u0000",
    b"\\uffff",
    b"\\ud83d\\ude00",
    b"\\ud800",
    b"\\udc00",
    b"\\ud800\\ud800",
    b"\xc3\xa9",
    b"\xe2\x82\xac",
    b"\xf0\x9f\x92\xa9",
];

/// Numbers spanning the scanner's integer/float/exponent decision points.
const NUMBERS: &[&[u8]] = &[
    b"0",
    b"-0",
    b"1",
    b"-1",
    b"1.5",
    b"-1.5",
    b"1e3",
    b"1E3",
    b"1e+3",
    b"1e-3",
    b"1.5e3",
    b"0.0",
    b"9223372036854775807",
    b"-9223372036854775808",
    b"9223372036854775808",
    b"1e999999999",
    b"1e-999999999",
    b"0.1",
    b"123456789012345678901234567890",
];

fn gen_string(rng: &mut Rng, out: &mut Vec<u8>) {
    out.push(b'"');
    for _ in 0..rng.below(4) {
        out.extend_from_slice(STRING_PIECES[rng.below(STRING_PIECES.len())]);
    }
    out.push(b'"');
}

fn gen_scalar(rng: &mut Rng, out: &mut Vec<u8>) {
    match rng.below(6) {
        0 | 1 => gen_string(rng, out),
        2 | 3 => out.extend_from_slice(NUMBERS[rng.below(NUMBERS.len())]),
        4 => out.extend_from_slice(b"true"),
        _ => out.extend_from_slice(if rng.below(2) == 0 { b"false" } else { b"null" }),
    }
}

/// A syntactically valid JSON value, nested well under the scanner's depth
/// limit; pathological nesting has its own test.
fn gen_value(rng: &mut Rng, depth: usize, out: &mut Vec<u8>) {
    if depth >= 5 || rng.below(3) == 0 {
        gen_scalar(rng, out);
        return;
    }
    if rng.below(2) == 0 {
        gen_object(rng, depth, out);
    } else {
        out.push(b'[');
        let n = rng.below(4);
        for i in 0..n {
            if i > 0 {
                out.push(b',');
            }
            gen_value(rng, depth + 1, out);
        }
        out.push(b']');
    }
}

fn gen_object(rng: &mut Rng, depth: usize, out: &mut Vec<u8>) {
    out.push(b'{');
    let n = rng.below(4);
    for i in 0..n {
        if i > 0 {
            out.push(b',');
        }
        gen_string(rng, out);
        out.push(b':');
        gen_value(rng, depth + 1, out);
    }
    out.push(b'}');
}

/// One random corruption of a valid document. Generation alone never
/// reaches an error path; random bytes alone never get past byte one.
fn corrupt(rng: &mut Rng, doc: &mut Vec<u8>) {
    if doc.is_empty() {
        return;
    }
    match rng.below(5) {
        0 => {
            let pos = rng.below(doc.len());
            doc[pos] = (rng.next_u64() & 0xff) as u8;
        }
        1 => {
            let pos = rng.below(doc.len());
            let tok = INTERESTING[rng.below(INTERESTING.len())];
            doc.splice(pos..pos, tok.iter().copied());
        }
        2 => {
            let pos = rng.below(doc.len());
            doc.remove(pos);
        }
        3 => {
            let cut = rng.below(doc.len());
            doc.truncate(cut);
        }
        _ => {
            let pos = rng.below(doc.len());
            let tok = INTERESTING[rng.below(INTERESTING.len())];
            for (i, b) in tok.iter().enumerate() {
                if pos + i < doc.len() {
                    doc[pos + i] = *b;
                }
            }
        }
    }
}

/// A generated document, valid roughly a quarter of the time and corrupted
/// in one to three places otherwise.
fn random_doc(rng: &mut Rng) -> Vec<u8> {
    let mut doc = Vec::new();
    gen_object(rng, 0, &mut doc);
    let corruptions = rng.below(4);
    for _ in 0..corruptions {
        corrupt(rng, &mut doc);
    }
    doc
}

/// `parse_object` may reject anything; it may not unwind.
#[test]
fn json_scan_never_panics_on_arbitrary_bytes() {
    fuzz::run(1..20_000, |seed| {
        let doc = random_doc(&mut Rng(seed));
        let mut sink = CountingSink::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            json_scan::parse_object(&doc, &mut Scratch::default(), &mut sink)
        }));
        result
            .map(drop)
            .map_err(|_| format!("panicked on input {:?} ({:02x?})", String::from_utf8_lossy(&doc), doc))
    });
}

/// Deep nesting must never overflow the stack: that is an abort, which
/// `catch_unwind` cannot contain.
#[test]
fn json_scan_survives_pathological_nesting() {
    for depth in [8usize, 64, 512, 4096, 65_536] {
        for (open, close) in [(&b"["[..], &b"]"[..]), (&b"{\"a\":"[..], &b"}"[..])] {
            let mut doc = Vec::new();
            doc.extend_from_slice(b"{\"k\":");
            for _ in 0..depth {
                doc.extend_from_slice(open);
            }
            for _ in 0..depth {
                doc.extend_from_slice(close);
            }
            doc.extend_from_slice(b"}");
            let mut sink = CountingSink::default();
            let _ = json_scan::parse_object(&doc, &mut Scratch::default(), &mut sink);
        }
    }
}

/// Every prefix of a valid document.
#[test]
fn json_scan_survives_every_truncation_of_a_valid_document() {
    let full = b"{\"a\":1,\"b\":[1,2,{\"c\":\"x\xc3\xa9y\"}],\"d\":{\"e\":true,\"f\":null},\"g\":-1.5e3}";
    for cut in 0..=full.len() {
        let mut sink = CountingSink::default();
        let _ = json_scan::parse_object(&full[..cut], &mut Scratch::default(), &mut sink);
    }
}

/// Every single-byte corruption of a valid document, at every position.
#[test]
fn json_scan_survives_single_byte_corruption_everywhere() {
    let full = b"{\"a\":1,\"b\":[1,2],\"c\":\"x\xc3\xa9y\",\"d\":null}";
    for pos in 0..full.len() {
        for byte in [0u8, b'"', b'\\', b'{', b'}', b'[', b']', b':', b',', 0x80, 0xff] {
            let mut doc = full.to_vec();
            doc[pos] = byte;
            let mut sink = CountingSink::default();
            let _ = json_scan::parse_object(&doc, &mut Scratch::default(), &mut sink);
        }
    }
}

/// The corpus's reach is asserted, not assumed: a generator that stops
/// producing parseable documents would leave the tests above passing while
/// testing only the first-byte check. The bounds are loose.
#[test]
fn fuzz_corpus_actually_reaches_the_scanner() {
    let (mut accepted, mut leaves) = (0usize, 0usize);
    for seed in 1..20_000u64 {
        let mut rng = Rng(seed);
        let doc = random_doc(&mut rng);
        let mut sink = CountingSink::default();
        if json_scan::parse_object(&doc, &mut Scratch::default(), &mut sink) {
            accepted += 1;
        }
        leaves += sink.leaves;
    }
    assert!(
        accepted > 1_000,
        "only {accepted}/19999 generated documents parsed -- the corpus has \
         stopped exercising the scanner, so the tests above prove nothing"
    );
    assert!(
        leaves > 3_000,
        "only {leaves} leaves reached the sink -- documents are parsing but \
         are too trivial to exercise string/number/escape decoding"
    );
}

// -- the text scanners: no structural gate at byte one, so random bytes are
// real coverage, but the quoting states only arise from plausible input.

/// The delimiters, quotes and newlines the text scanners switch on.
const DELIMITED_PIECES: &[&[u8]] = &[
    b"a",
    b"field",
    b"",
    b" ",
    b",",
    b";",
    b"\t",
    b"|",
    b"=",
    b":",
    b"\"",
    b"\"\"",
    b"\"a\"",
    b"\"a,b\"",
    b"\"unterminated",
    b"\r\n",
    b"\n",
    b"\r",
    b"\\",
    b"\\\"",
    b"\xff",
    b"\xc3\x28",
    b"\xf0\x9f\x92\xa9",
    b"\x00",
];

fn random_delimited_line(rng: &mut Rng) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..rng.below(12) + 1 {
        if rng.below(5) == 0 {
            out.push((rng.next_u64() & 0xff) as u8);
        } else {
            out.extend_from_slice(DELIMITED_PIECES[rng.below(DELIMITED_PIECES.len())]);
        }
    }
    out
}

#[test]
fn csv_scan_never_panics_on_arbitrary_bytes() {
    fuzz::run(1..20_000, |seed| {
        let mut rng = Rng(seed);
        let line = random_delimited_line(&mut rng);
        let config = smeltr::plan::config::CsvConfig {
            delimiter: [b',', b';', b'\t', b'|', b'"', 0xff][rng.below(6)],
            quote: b"\"',"[rng.below(3)],
            escape: [None, Some(b'\\'), Some(b'"')][rng.below(3)],
            squeeze: rng.below(2) == 0,
            trim: rng.below(2) == 0,
            ..Default::default()
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            csv_scan::split_line_into(&line, &config, false, &mut Vec::new())
        }));
        result
            .map(drop)
            .map_err(|_| format!("{config:?} panicked on {:?}", String::from_utf8_lossy(&line)))
    });
}

#[test]
fn kv_scan_never_panics_on_arbitrary_bytes() {
    fuzz::run(1..20_000, |seed| {
        let mut rng = Rng(seed);
        let line = random_delimited_line(&mut rng);
        let pair_sep = b" ,;&"[rng.below(4)];
        let kv_sep = b"=: "[rng.below(3)];
        let quote = [None, Some(b'"'), Some(b'\''), Some(kv_sep)][rng.below(4)];
        let config = smeltr::plan::config::KvConfig {
            pair_sep,
            kv_sep,
            quote,
            escapes: rng.below(2) == 1,
            ..Default::default()
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            kv_scan::split_pairs(&line, &config, &mut Vec::new())
        }));
        result.map(drop).map_err(|_| {
            format!(
                "pair_sep {pair_sep:?} kv_sep {kv_sep:?} quote {quote:?} panicked on {:?}",
                String::from_utf8_lossy(&line)
            )
        })
    });
}

#[test]
fn cef_scan_never_panics_on_arbitrary_bytes() {
    fuzz::run(1..20_000, |seed| {
        let mut rng = Rng(seed);
        let mut line = b"CEF:0|".to_vec();
        line.extend(
            random_delimited_line(&mut rng)
                .into_iter()
                .map(|b| b"|= \\a"[usize::from(b) % 5]),
        );
        let result = std::panic::catch_unwind(|| cef_scan::split_record(&line, &mut Vec::new()));
        result
            .map(drop)
            .map_err(|_| format!("panicked on {:?}", String::from_utf8_lossy(&line)))
    });
}

/// Sniffing and coercion see attacker bytes too, and are the one place
/// that calls `str::parse` on them.
#[test]
fn scalar_sniffing_never_panics_on_arbitrary_bytes() {
    fuzz::run(1..20_000, |seed| {
        let text = random_delimited_line(&mut Rng(seed));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = spread::sniff_scalar(&text);
            let text = String::from_utf8_lossy(&text);
            let _ = (
                convert::parse_bool(&text),
                convert::parse_i64(&text),
                convert::parse_f64(&text),
            );
        }));
        result
            .map(drop)
            .map_err(|_| format!("panicked sniffing {:?}", String::from_utf8_lossy(&text)))
    });
}

/// A value that looks numeric but overflows every numeric type lands
/// somewhere finite.
#[test]
fn scalar_sniffing_handles_numeric_extremes() {
    let cases: &[&[u8]] = &[
        b"1e999999999",
        b"-1e999999999",
        b"1e-999999999",
        b"9223372036854775808",
        b"-9223372036854775809",
        b"18446744073709551616",
        b"340282366920938463463374607431768211456",
        &[b'9'; 4096],
        b"-",
        b"+",
        b".",
        b"-.",
        b"0.",
        b".0",
        b"1e",
        b"1e+",
        b"--1",
        b"0x10",
        b"NaN",
        b"inf",
        b"Infinity",
        b"-Infinity",
    ];
    for case in cases {
        let tag = spread::sniff_scalar(case);
        if let Tag::F64(f) = tag {
            assert!(
                f.is_finite(),
                "sniffing {:?} produced non-finite {f}",
                String::from_utf8_lossy(case)
            );
        }
        assert!(convert::parse_f64(&String::from_utf8_lossy(case)).is_none_or(f64::is_finite));
    }
}

/// Every S2S frame and field kind the decoder reads: a timezone, channels with field names, an event carrying every
/// flag that has bytes and every kind of value, a close, and a channel freed and defined again.
fn s2s_stream() -> Vec<u8> {
    let channel = |s: &mut Vec<u8>, id: u8| {
        s.extend([0xfe, id]);
        for text in [&b"source::/a"[..], b"host::h", b"sourcetype::t", b"1"] {
            s.push(text.len() as u8 + 1);
            s.extend(text);
        }
        s.extend([1, 4]);
        s.extend(b"word");
    };
    let mut s = vec![0xff, 7];
    s.extend(b"N###UTC");
    channel(&mut s, 1);
    channel(&mut s, 2);
    // Stream, id, time, five fields: an integer, text, a dictionary pair, a raw slice and an escaped one.
    s.extend([0xfc, 1, 0x0f, 7, 0, 0, 2, 0x80, 0x01, 5]);
    s.extend([
        0x00, 1, b'n', 5, 0x04, 1, b'k', 1, b'v', 0x48, 4, 62, 0x0d, 0, 0, 3, 0x19, 0, 1, 2,
    ]);
    s.extend([3, b'a', b'b', b'c']);
    s.extend([0xfc, 2, 0x84, 0x04, 3, 0]);
    s.extend([0xfd, 1]);
    channel(&mut s, 1);
    s.extend([0xfc, 1, 0, 1, b'x']);
    s
}

/// A forwarder's S2S stream is whoever connects to the receiver's port. Corrupted streams, pushed in random pieces,
/// must end in an event, a wait for more, or a refusal, and never in a panic or a hold past the frame limit.
#[test]
fn s2s_decoding_never_panics_on_corrupted_streams() {
    use smeltr::io::s2s::{COMPRESSED, Decoder, Inflate};
    const MAX_FRAME: usize = 4096;
    let plain = s2s_stream();
    let mut decoder = Decoder::new(MAX_FRAME);
    decoder.push(&plain);
    let events = std::iter::from_fn(|| decoder.next_event().unwrap()).count();
    assert_eq!(
        (events, decoder.held()),
        (3, 0),
        "the stream decodes whole before it is corrupted"
    );
    let mut zlib = flate2::write::ZlibEncoder::new(vec![COMPRESSED], flate2::Compression::default());
    std::io::Write::write_all(&mut zlib, &plain).unwrap();
    std::io::Write::flush(&mut zlib).unwrap();
    let streams = [plain.clone(), zlib.get_ref().clone()];
    fuzz::run(1..5_000, |seed| {
        let mut rng = Rng(seed);
        let which = rng.below(2);
        let mut stream = streams[which].clone();
        for _ in 0..1 + rng.below(4) {
            let at = rng.below(stream.len());
            stream[at] = rng.next_u64() as u8;
        }
        stream.truncate(rng.below(stream.len()) + 1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut decoder = Decoder::new(MAX_FRAME);
            // The compressed stream is inflated as it arrives, and what it inflates to is decoded.
            let mut inflate = (which == 1).then(Inflate::default);
            let mut rest = &stream[(which == 1) as usize..];
            'pieces: while !rest.is_empty() {
                let (piece, after) = rest.split_at((1 + rng.below(300)).min(rest.len()));
                rest = after;
                match &mut inflate {
                    Some(inflate) => {
                        inflate.push(piece);
                        let mut out = Vec::new();
                        if inflate.inflate(&mut out, MAX_FRAME).is_err() {
                            break 'pieces;
                        }
                        decoder.push(&out);
                    }
                    None => decoder.push(piece),
                }
                loop {
                    match decoder.next_event() {
                        Ok(Some(_)) => {}
                        Ok(None) => break,
                        Err(_) => break 'pieces,
                    }
                }
                if decoder.held() > MAX_FRAME {
                    return Err(format!("holds {} waiting for a frame", decoder.held()));
                }
            }
            Ok(())
        }));
        result.unwrap_or_else(|_| Err("panicked".into()))
    });
}
