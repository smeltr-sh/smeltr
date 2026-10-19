//! A single-line CSV field scanner: RFC 4180 quoting, and the variants log systems write.
//!
//! `_raw` is one record per row, never a file with a header row; column names come from `CsvConfig::headers`. A field
//! starting with the quote runs to the matching unescaped quote (doubled, or after `escape`) and may contain the
//! delimiter or a newline. One trailing `\r\n` or `\n` is trimmed before splitting. Malformed input never panics.

use std::borrow::Cow;

use crate::plan::config::CsvConfig;

/// One field: its text (owned only when a quote had to be unescaped) and whether it was explicitly quoted, which stops
/// the caller from sniffing a type out of it.
pub struct Field<'d> {
    pub text: Cow<'d, str>,
    pub quoted: bool,
}

/// Split one record into fields on `delimiter`. `None` on an unterminated quote or invalid UTF-8.
pub fn split_line(line: &[u8], delimiter: u8) -> Option<Vec<Field<'_>>> {
    let mut fields = Vec::new();
    let config = CsvConfig {
        delimiter,
        ..CsvConfig::default()
    };
    split_line_into(line, &config, false, &mut fields).then_some(fields)
}

/// The fields of `line` into a caller's vector, cleared first, so one allocation serves every row of a batch. `false`
/// where the record is not whole, an unterminated quote or invalid UTF-8; the fields read before that are kept.
/// `trusted_utf8` says the line is already known to be UTF-8 (it came out of a `Str` column), so an unquoted field is
/// not validated again; a quoted field always is, since unescaping reads it as text anyway.
pub fn split_line_into<'d>(line: &'d [u8], c: &CsvConfig, trusted_utf8: bool, fields: &mut Vec<Field<'d>>) -> bool {
    let line = trim_trailing_newline(line);
    fields.clear();
    let mut i = 0;
    let mut whole = true;

    loop {
        if c.squeeze {
            while line.get(i) == Some(&c.delimiter) {
                i += 1;
            }
            // A run at the end ends the record, not one more empty field.
            if i == line.len() && !fields.is_empty() {
                return true;
            }
        }
        let Some((field, next)) = scan_field(line, i, c, trusted_utf8, &mut whole) else {
            return false;
        };
        fields.push(field);
        crate::core::buffer::check();
        match next {
            Some(after_delim) => i = after_delim,
            None => return whole,
        }
    }
}

fn trim_trailing_newline(line: &[u8]) -> &[u8] {
    if let Some(stripped) = line.strip_suffix(b"\r\n") {
        stripped
    } else if let Some(stripped) = line.strip_suffix(b"\n") {
        stripped
    } else {
        line
    }
}

/// Scans one field starting at `start`: the field and, if a delimiter followed it, the index just past that delimiter.
/// `None` on invalid UTF-8.
#[inline(always)]
fn scan_field<'d>(
    line: &'d [u8],
    mut start: usize,
    c: &CsvConfig,
    trusted_utf8: bool,
    whole: &mut bool,
) -> Option<(Field<'d>, Option<usize>)> {
    if c.trim {
        while line.get(start) == Some(&b' ') {
            start += 1;
        }
    }
    if line.get(start) == Some(&c.quote) {
        scan_quoted_field(line, start, c, whole)
    } else {
        scan_unquoted_field(line, start, c, trusted_utf8)
    }
}

fn scan_unquoted_field<'d>(
    line: &'d [u8],
    start: usize,
    c: &CsvConfig,
    trusted_utf8: bool,
) -> Option<(Field<'d>, Option<usize>)> {
    let i = find_byte(line, start, c.delimiter);
    let raw = match c.trim {
        true => trim_end(&line[start..i]),
        false => &line[start..i],
    };
    let field = Field {
        text: Cow::Borrowed(as_text(raw, trusted_utf8)?),
        quoted: false,
    };
    Some((field, after(line, i)))
}

/// `line[start]` is the opening quote, not yet consumed. Spaces may follow the closing quote; anything else before the
/// delimiter has no reading, and is `None`. A quote the record ends inside runs to the end and clears `whole`.
fn scan_quoted_field<'d>(
    line: &'d [u8],
    start: usize,
    c: &CsvConfig,
    whole: &mut bool,
) -> Option<(Field<'d>, Option<usize>)> {
    let q = c.quote;
    let mut i = start + 1;
    let content_start = i;
    let mut escaped = false;

    let close = loop {
        i = match c.escape {
            None => find_byte(line, i, q),
            Some(e) => (i..line.len())
                .find(|&j| line[j] == q || line[j] == e)
                .unwrap_or(line.len()),
        };
        if i == line.len() {
            break None;
        }
        let doubled = c.escape.is_none() && line.get(i + 1) == Some(&q);
        if doubled || c.escape == Some(line[i]) {
            escaped = true;
            i += 2;
            continue;
        }
        break Some(i);
    };
    let raw = &line[content_start..close.unwrap_or(line.len())];
    let text = match escaped {
        true => Cow::Owned(unescape(raw, c)?),
        false => Cow::Borrowed(std::str::from_utf8(raw).ok()?),
    };
    let Some(close) = close else {
        *whole = false;
        return Some((Field { text, quoted: true }, None));
    };
    let spaces = line[close + 1..]
        .iter()
        .take_while(|&&b| b == b' ' && b != c.delimiter)
        .count();
    let end = match line.get(close + 1 + spaces) {
        None => None,
        Some(&b) if b == c.delimiter => Some(close + spaces + 2),
        Some(_) => return None,
    };
    Some((Field { text, quoted: true }, end))
}

/// Past the delimiter that ends a field at `i`; `None` at the record's end.
fn after(line: &[u8], i: usize) -> Option<usize> {
    (i < line.len()).then_some(i + 1)
}

fn trim_end(raw: &[u8]) -> &[u8] {
    let n = raw.iter().rposition(|&b| b != b' ').map_or(0, |p| p + 1);
    &raw[..n]
}

fn as_text(raw: &[u8], trusted_utf8: bool) -> Option<&str> {
    match trusted_utf8 {
        // SAFETY: the caller vouches that `line` is valid UTF-8, and a slice ending at an ASCII delimiter, quote or
        // space (or the line's end) starts and ends on a character boundary.
        true => Some(unsafe { std::str::from_utf8_unchecked(raw) }),
        false => std::str::from_utf8(raw).ok(),
    }
}

/// The first `byte` at or after `start`, or `line.len()`. Eight bytes at a time: a byte equal to `byte` is a zero
/// byte in the word xor'd with it, and a zero byte sets the top bit of `(v - 0x01..) & !v & 0x80..` in that byte and
/// perhaps in higher bytes after it, so the lowest set bit is the first hit.
#[inline]
fn find_byte(line: &[u8], start: usize, byte: u8) -> usize {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGHS: u64 = 0x8080_8080_8080_8080;
    let pattern = ONES * u64::from(byte);
    let mut i = start;
    while let Some(chunk) = line.get(i..i + 8) {
        let word = u64::from_le_bytes(chunk.try_into().expect("eight bytes"));
        let x = word ^ pattern;
        let hit = x.wrapping_sub(ONES) & !x & HIGHS;
        if hit != 0 {
            return i + hit.trailing_zeros() as usize / 8;
        }
        i += 8;
    }
    while i < line.len() && line[i] != byte {
        i += 1;
    }
    i
}

/// A quoted field's content: an escaped quote or escape is itself, and any other escape keeps both bytes. Without
/// `escape`, the quote escapes itself, so a doubled quote is one.
fn unescape(raw: &[u8], c: &CsvConfig) -> Option<String> {
    let e = c.escape.unwrap_or(c.quote);
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == e && raw.get(i + 1).is_some_and(|&n| n == c.quote || n == e) {
            i += 1;
        }
        out.push(raw[i]);
        i += 1;
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(line: &str, delimiter: u8) -> Vec<String> {
        split_line(line.as_bytes(), delimiter)
            .unwrap()
            .into_iter()
            .map(|f| f.text.into_owned())
            .collect()
    }

    /// The word scan finds what a byte loop finds, from every start, with high bytes and a hit in every lane.
    #[test]
    fn find_byte_agrees_with_a_byte_loop_at_every_offset() {
        let mut line: Vec<u8> = (0..41u8)
            .map(|i| if i % 7 == 3 { b',' } else { b'a' + i % 26 })
            .collect();
        line[10] = 0xFF;
        line[24] = 0x80;
        line[40] = b',';
        for start in 0..=line.len() {
            let slow = (start..line.len()).find(|&i| line[i] == b',').unwrap_or(line.len());
            assert_eq!(find_byte(&line, start, b','), slow, "from {start}");
        }
        assert_eq!(find_byte(b"no delimiter here", 0, b','), 17);
        assert_eq!(find_byte(b"", 0, b','), 0);
    }

    #[test]
    fn plain_fields() {
        assert_eq!(texts("a,b,c", b','), vec!["a", "b", "c"]);
        assert_eq!(texts("a\tb\tc", b'\t'), vec!["a", "b", "c"]);
    }

    #[test]
    fn a_trailing_newline_is_trimmed_once() {
        assert_eq!(texts("a,b\n", b','), vec!["a", "b"]);
        assert_eq!(texts("a,b\r\n", b','), vec!["a", "b"]);
    }

    #[test]
    fn embedded_newlines_are_not_special() {
        assert_eq!(texts("a\nb,c", b','), vec!["a\nb", "c"]);
    }

    #[test]
    fn empty_fields() {
        assert_eq!(texts("a,,c", b','), vec!["a", "", "c"]);
        assert_eq!(texts("", b','), vec![""]);
        assert_eq!(texts(",", b','), vec!["", ""]);
    }

    #[test]
    fn a_quoted_field_can_contain_the_delimiter_or_a_newline() {
        assert_eq!(texts(r#"a,"b,c",d"#, b','), vec!["a", "b,c", "d"]);
        assert_eq!(texts("a,\"b\nc\",d", b','), vec!["a", "b\nc", "d"]);
    }

    #[test]
    fn doubled_quotes_decode_to_one_literal_quote() {
        assert_eq!(texts(r#"a,"say ""hi""",c"#, b','), vec!["a", "say \"hi\"", "c"]);
    }

    #[test]
    fn an_all_quoted_line_works() {
        assert_eq!(texts(r#""a","b","c""#, b','), vec!["a", "b", "c"]);
    }

    #[test]
    fn quoted_flag_distinguishes_explicit_quoting() {
        let fields = split_line(br#"1,"2""#, b',').unwrap();
        assert!(!fields[0].quoted);
        assert!(fields[1].quoted);
    }

    #[test]
    fn spaces_may_follow_a_closing_quote_and_anything_else_breaks_the_record_off() {
        assert_eq!(texts(r#""x" ,z"#, b','), vec!["x", "z"]);
        assert_eq!(
            texts(r#""x" z"#, b' '),
            vec!["x", "z"],
            "a space delimiter is the delimiter"
        );
        assert_eq!(read(r#"a,"x"y,z"#, CsvConfig::default()), (vec!["a".into()], false));
    }

    /// The fields `config` reads from `line`, and whether the record was whole.
    fn read(line: &str, config: CsvConfig) -> (Vec<String>, bool) {
        let mut fields = Vec::new();
        let whole = split_line_into(line.as_bytes(), &config, false, &mut fields);
        (fields.into_iter().map(|f| f.text.into_owned()).collect(), whole)
    }

    #[test]
    fn an_unterminated_quote_runs_to_the_end_and_the_record_is_not_whole() {
        assert_eq!(
            read(r#"x,"cut off"#, CsvConfig::default()),
            (vec!["x".into(), "cut off".into()], false)
        );
    }

    #[test]
    fn the_options_read_their_dialects() {
        let c = CsvConfig::default;
        let ok = |v: &[&str]| (v.iter().map(|s| s.to_string()).collect::<Vec<_>>(), true);
        let escape = CsvConfig {
            escape: Some(b'\\'),
            ..c()
        };
        assert_eq!(read(r#""x\"y",z"#, escape.clone()), ok(&["x\"y", "z"]));
        assert_eq!(read(r#""a\\b""#, escape), ok(&["a\\b"]));
        assert_eq!(read(r#""x\",z""#, c()), ok(&["x\\", "z\""]));
        let squeeze = CsvConfig {
            delimiter: b' ',
            squeeze: true,
            ..c()
        };
        assert_eq!(read("  1   2 3  ", squeeze), ok(&["1", "2", "3"]));
        assert_eq!(read(" x , y ", CsvConfig { trim: true, ..c() }), ok(&["x", "y"]));
        assert_eq!(read("'x,y',z", CsvConfig { quote: b'\'', ..c() }), ok(&["x,y", "z"]));
    }

    #[test]
    fn invalid_utf8_keeps_the_fields_before_it() {
        let mut fields = Vec::new();
        assert!(!split_line_into(b"a,\xFF,b", &CsvConfig::default(), false, &mut fields));
        assert_eq!(fields.len(), 1);

        let mut line = br#"a,""#.to_vec();
        line.push(0xFF);
        line.extend_from_slice(br#"""#);
        assert!(split_line(&line, b',').is_none());
    }
}
