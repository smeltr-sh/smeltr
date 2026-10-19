//! An ArcSight Common Event Format scanner, one record per `_raw` row.
//!
//! A record is `CEF:` and seven `|`-separated header fields, then an extension of `key=value` pairs. Text before
//! `CEF:`, such as a syslog header, is not read. A header field escapes `|` and `\` with a backslash. An extension value
//! runs to the space before the next key, escapes `=` and `\`, and writes `\n` and `\r` for a newline and a carriage
//! return; any other escape keeps both characters. Malformed input never panics.

use std::borrow::Cow;

use crate::format::kv_scan::{Pair, Piece, Seps, next_key_pieces};

/// The header fields, in order, under the names the CEF specification gives them.
pub const HEADER: [&str; 7] = [
    "version",
    "deviceVendor",
    "deviceProduct",
    "deviceVersion",
    "deviceEventClassId",
    "name",
    "severity",
];

/// The fields of one record into `pairs`, cleared first. `false` where the record has no `CEF:`, fewer than seven header
/// fields, text before its first extension key, or invalid UTF-8; the fields read before that are kept.
pub fn split_record<'d>(line: &'d [u8], pairs: &mut Vec<Pair<'d>>) -> bool {
    pairs.clear();
    let Some(start) = line.windows(4).position(|w| w == b"CEF:") else {
        return false;
    };
    if std::str::from_utf8(&line[..start]).is_err() {
        return false;
    }
    let mut i = start + 4;
    for key in HEADER {
        let Some(end) = (i..line.len()).find(|&j| line[j] == b'|' && !escaped(line, i, j)) else {
            return false;
        };
        if !push(pairs, key, &line[i..end], b"|") {
            return false;
        }
        i = end + 1;
    }
    split_extension(&line[i..], pairs)
}

/// Whether the byte at `j` follows an odd run of backslashes that starts at or after `from`.
fn escaped(line: &[u8], from: usize, j: usize) -> bool {
    line[from..j].iter().rev().take_while(|&&b| b == b'\\').count() % 2 == 1
}

/// The extension's pairs. A key is the text after a space (or the extension's start) up to an unescaped `=`; its value
/// runs to the space before the next key; the last value drops the line's trailing spaces.
fn split_extension<'d>(ext: &'d [u8], pairs: &mut Vec<Pair<'d>>) -> bool {
    let seps = Seps {
        pair: b' ',
        kv: b'=',
        quote: None,
        backslash: true,
    };
    next_key_pieces(ext, &seps, |piece| match piece {
        // Only spaces may come before the first key.
        Piece::Bare(_) => false,
        Piece::Pair { key, value, .. } => match std::str::from_utf8(key) {
            Ok(key) => push(pairs, key, value, b"=nr"),
            Err(_) => false,
        },
    })
}

/// Pushes `key` holding `raw` with `escapes` decoded; `false` where `raw` is not UTF-8.
fn push<'d>(pairs: &mut Vec<Pair<'d>>, key: &'d str, raw: &'d [u8], escapes: &[u8]) -> bool {
    let Some(value) = unescape(raw, escapes) else {
        return false;
    };
    pairs.push(Pair {
        key,
        value,
        quoted: false,
    });
    crate::core::buffer::check();
    true
}

/// `raw` with a backslash before itself or one of `escapes` decoded; `n` and `r` stand for a newline and a carriage
/// return. Any other escape keeps both characters, and a backslash with nothing after it is itself.
fn unescape<'d>(raw: &'d [u8], escapes: &[u8]) -> Option<Cow<'d, str>> {
    let s = std::str::from_utf8(raw).ok()?;
    if !raw.contains(&b'\\') {
        return Some(Cow::Borrowed(s));
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.clone().next() {
            Some(next) if next == '\\' || next.is_ascii() && escapes.contains(&(next as u8)) => {
                chars.next();
                out.push(match next {
                    'n' => '\n',
                    'r' => '\r',
                    _ => next,
                });
            }
            _ => out.push('\\'),
        }
    }
    Some(Cow::Owned(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fields of `line`, and whether the record was whole.
    fn read(line: &str) -> (Vec<(String, String)>, bool) {
        let mut pairs = Vec::new();
        let whole = split_record(line.as_bytes(), &mut pairs);
        let fields = pairs
            .into_iter()
            .map(|p| (p.key.to_string(), p.value.into_owned()))
            .collect();
        (fields, whole)
    }

    fn values(line: &str) -> Vec<String> {
        let (fields, whole) = read(line);
        assert!(whole, "{line} is whole");
        fields.into_iter().map(|(_, v)| v).collect()
    }

    #[test]
    fn the_header_fields_take_their_spec_names_and_the_extension_its_keys() {
        let (fields, whole) = read("<134>Oct 4 host CEF:0|Acme|FW|1.0|100|Blocked|5|src=10.0.0.1 act=drop");
        assert!(whole);
        let keys: Vec<&str> = fields.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, [&HEADER[..], &["src", "act"]].concat());
        assert_eq!(fields[0].1, "0");
        assert_eq!(fields[7].1, "10.0.0.1");
    }

    #[test]
    fn a_value_runs_to_the_space_before_the_next_key() {
        assert_eq!(
            values("CEF:0|a|b|c|d|e|f|msg=disk full now  src=1.2.3.4")[7..],
            ["disk full now ", "1.2.3.4"]
        );
        assert_eq!(values("CEF:0|a|b|c|d|e|f|")[7..], [] as [&str; 0]);
    }

    #[test]
    fn escapes_decode_where_the_spec_defines_them() {
        let v = values(r"CEF:0|a\|b|c\\d|x|y|z|w|msg=a\=b\\c\nd q=x\ty u=k=v");
        assert_eq!(v[1..3], ["a|b", r"c\d"]);
        assert_eq!(v[7..], ["a=b\\c\nd", r"x\ty", "k=v"]);
    }

    #[test]
    fn a_record_that_breaks_off_keeps_the_fields_before_it() {
        assert_eq!(read("no cef here"), (vec![], false));
        assert!(!split_record(b"\xffCEF:0|a|b|c|d|e|f|", &mut Vec::new()));
        let (fields, whole) = read("CEF:0|a|b|c");
        assert_eq!((fields.len(), whole), (3, false));
        let (fields, whole) = read("CEF:0|a|b|c|d|e|f|junk src=1");
        assert_eq!((fields.len(), whole), (7, false));
    }
}
