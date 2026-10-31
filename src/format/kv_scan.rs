//! A `key=value key=value` pair scanner, one record per `_raw` row.
//!
//! `KvConfig` supplies `pair_sep`, `kv_sep`, and an optional `quote`: a value starting with that byte runs to the
//! matching unescaped quote (`\"` and `\\` escapes), so it can contain `pair_sep`. Under `escapes`, a backslash
//! escapes any character in any value: `\n`, `\r` and `\t` are control characters and any other `\x` is `x`. Keys are
//! never quoted. Runs of `pair_sep` are skipped. Malformed input never panics.
//!
//! Under `next_key`, an unquoted value runs to the `pair_sep` before the next key, so `msg=user logged in id=5` holds
//! `user logged in`; spaces around a key or value are trimmed. Text before the first key is bare keys. Otherwise a value
//! ends at the first `pair_sep`, and a word with no `kv_sep` anywhere is a bare key. A bare key's value is `true`.

use std::borrow::Cow;

use crate::plan::config::KvConfig;

pub struct Pair<'d> {
    pub key: &'d str,
    pub value: Cow<'d, str>,
    pub quoted: bool,
}

/// The pairs of one record into `pairs`, cleared first. `false` on invalid UTF-8 or an unterminated quoted value; the
/// pairs read before it are kept.
pub fn split_pairs<'d>(line: &'d [u8], c: &KvConfig, pairs: &mut Vec<Pair<'d>>) -> bool {
    let KvConfig { pair_sep, kv_sep, .. } = *c;
    pairs.clear();
    if c.next_key {
        return split_to_next_key(line, c, pairs);
    }
    let mut i = 0;
    let n = line.len();

    while i < n {
        if line[i] == pair_sep {
            i += 1;
            continue;
        }

        let key_start = i;
        while i < n && line[i] != kv_sep && line[i] != pair_sep {
            i += 1;
        }
        let Ok(key) = std::str::from_utf8(&line[key_start..i]) else {
            return false;
        };
        if i >= n || line[i] != kv_sep {
            pairs.push(Pair {
                key,
                value: Cow::Borrowed("true"),
                quoted: false,
            });
            crate::core::buffer::check();
            continue;
        }
        i += 1; // consume kv_sep

        let Some((value, quoted, next)) = scan_value(line, i, c) else {
            return false;
        };
        pairs.push(Pair { key, value, quoted });
        crate::core::buffer::check();
        i = next;
    }
    true
}

/// `line` without the pairs whose key `drop` names, each kept pair as it was written, quotes and escapes and all.
/// A pair runs from its key to the next pair's key, so a dropped pair takes the separators after it. `None` where
/// `line` does not split whole, which leaves nothing safe to cut.
pub fn drop_pairs(line: &[u8], c: &KvConfig, drop: impl Fn(&str) -> bool) -> Option<Vec<u8>> {
    let mut pairs = Vec::new();
    if !split_pairs(line, c, &mut pairs) {
        return None;
    }
    // A key is always a slice of `line`, never unescaped, so where it starts is where its pair starts.
    let starts: Vec<usize> = pairs
        .iter()
        .map(|p| p.key.as_ptr() as usize - line.as_ptr() as usize)
        .collect();
    let mut out = Vec::with_capacity(line.len());
    let mut kept = 0;
    let mut dropped_last = false;
    for (n, pair) in pairs.iter().enumerate() {
        let end = starts.get(n + 1).copied().unwrap_or(line.len());
        dropped_last = drop(pair.key);
        if dropped_last {
            out.extend_from_slice(&line[kept..starts[n]]);
            kept = end;
        }
    }
    out.extend_from_slice(&line[kept..]);
    // The last pair dropped leaves the separator that ran up to it.
    if dropped_last {
        out.truncate(out.iter().rposition(|&b| b != c.pair_sep).map_or(0, |p| p + 1));
    }
    Some(out)
}

/// One piece of a record read by [`next_key_pieces`].
pub(crate) enum Piece<'d> {
    /// A word before the first key, or after a quoted value, with no `kv_sep` of its own.
    Bare(&'d [u8]),
    /// `value` is a quoted value's text inside its quotes, or else the raw text up to the separator before the next key.
    Pair {
        key: &'d [u8],
        value: &'d [u8],
        quoted: bool,
    },
}

/// The separators [`next_key_pieces`] splits at.
pub(crate) struct Seps {
    pub pair: u8,
    pub kv: u8,
    pub quote: Option<u8>,
    /// A backslash keeps the byte after it from being a separator.
    pub backslash: bool,
}

/// Calls `each` with the pieces of `line` in order, stopping at the first `false`. A key is the text after the last
/// `pair` before a `kv`, and its value runs to the `pair` before the next key; a `kv` with no `pair` since its value
/// began is the value's own, which keeps `url=http://x/?a=1&b=2` whole. A value opening with `quote`, after any spaces,
/// runs to the closing quote, after which only spaces and `pair` may come. The last value drops its trailing `pair`s.
/// `false` on an unterminated quoted value or text after a closing quote.
pub(crate) fn next_key_pieces<'d>(line: &'d [u8], s: &Seps, mut each: impl FnMut(Piece<'d>) -> bool) -> bool {
    let bare = |text: &'d [u8], each: &mut dyn FnMut(Piece<'d>) -> bool| {
        text.split(|&b| b == s.pair)
            .filter(|word| !word.trim_ascii().is_empty())
            .all(|word| each(Piece::Bare(word)))
    };
    let n = line.len();
    // The open pair's key and where its value starts; where text not yet given to a piece starts; the word's start.
    let mut open: Option<(&'d [u8], usize)> = None;
    let mut from = 0;
    let mut w = 0;
    while w < n {
        // A word is a key where a `kv` comes after its first byte.
        let after_first = match line[w] {
            b'\\' if s.backslash => w + 2,
            b if b == s.pair => {
                w += 1;
                continue;
            }
            _ => w + 1,
        };
        let k = unescaped(line, after_first, s.backslash, |b| b == s.pair || b == s.kv);
        if k == n || line[k] == s.pair {
            w = k + 1;
            continue;
        }
        let closed = match open {
            Some((key, v)) => each(Piece::Pair {
                key,
                value: &line[v..w - 1],
                quoted: false,
            }),
            None => bare(&line[from..w], &mut each),
        };
        if !closed {
            return false;
        }
        let key = &line[w..k];
        let Some((inner, end)) = quoted_at(line, k + 1, s) else {
            return false;
        };
        if let Some(inner) = inner {
            if !each(Piece::Pair {
                key,
                value: inner,
                quoted: true,
            }) {
                return false;
            }
            (open, from, w) = (None, end, end);
            continue;
        }
        open = Some((key, k + 1));
        from = k + 1;
        // A `kv` in the value's first word is the value's own.
        w = unescaped(line, k + 1, s.backslash, |b| b == s.pair) + 1;
    }
    match open {
        Some((key, v)) => each(Piece::Pair {
            key,
            value: trim_end_byte(&line[v..], s.pair),
            quoted: false,
        }),
        None => bare(&line[from.min(n)..], &mut each),
    }
}

/// The first byte from `j` on that `stop` matches and no backslash escapes, or the line's end. Each scan stops on a
/// match rather than stepping by one or two, so no byte's position waits on the byte before it.
fn unescaped(line: &[u8], mut j: usize, backslash: bool, stop: impl Fn(u8) -> bool) -> usize {
    while let Some(at) = line
        .get(j..)
        .and_then(|t| t.iter().position(|&b| (backslash && b == b'\\') || stop(b)))
    {
        j += at;
        if !(backslash && line[j] == b'\\') {
            return j;
        }
        j += 2;
    }
    line.len()
}

/// The quoted value starting at `v` after any spaces, and where its text ends: past the closing quote and any spaces,
/// at the `pair` or the line's end. `Some((None, _))` where the value is not quoted; `None` where a quote is unterminated
/// or anything else follows it.
fn quoted_at<'d>(line: &'d [u8], v: usize, s: &Seps) -> Option<(Option<&'d [u8]>, usize)> {
    let skip_spaces = |mut i: usize| {
        while line.get(i) == Some(&b' ') && s.pair != b' ' {
            i += 1;
        }
        i
    };
    let open = skip_spaces(v);
    let Some(quote) = s.quote.filter(|&q| line.get(open) == Some(&q)) else {
        return Some((None, v));
    };
    let mut k = open + 1;
    loop {
        k += line.get(k..)?.iter().position(|&b| b == quote || b == b'\\')?;
        if line[k] == quote {
            break;
        }
        k += 2;
    }
    let end = skip_spaces(k + 1);
    match line.get(end) {
        None => Some((Some(&line[open + 1..k]), line.len())),
        Some(&b) if b == s.pair => Some((Some(&line[open + 1..k]), end)),
        Some(_) => None,
    }
}

/// `text` without the `byte`s it ends with.
fn trim_end_byte(text: &[u8], byte: u8) -> &[u8] {
    &text[..text.iter().rposition(|&b| b != byte).map_or(0, |p| p + 1)]
}

/// [`split_pairs`] under `next_key`.
fn split_to_next_key<'d>(line: &'d [u8], c: &KvConfig, pairs: &mut Vec<Pair<'d>>) -> bool {
    let seps = Seps {
        pair: c.pair_sep,
        kv: c.kv_sep,
        quote: c.quote,
        backslash: c.escapes,
    };
    // Inlined: a call per pair is a fair share of the scan.
    next_key_pieces(
        line,
        &seps,
        #[inline(always)]
        |piece| {
            let (key, value) = match piece {
                Piece::Bare(word) => (word, None),
                Piece::Pair { key, value, quoted } => (key, Some((value, quoted))),
            };
            let Ok(key) = std::str::from_utf8(key.trim_ascii()) else {
                return false;
            };
            let (value, quoted) = match value {
                None => (Cow::Borrowed("true"), false),
                Some((raw, quoted)) => {
                    let raw = match quoted {
                        true => raw,
                        false => trim_end_byte(raw, c.pair_sep).trim_ascii(),
                    };
                    let text = match (quoted || c.escapes) && raw.contains(&b'\\') {
                        true => unescape(raw, c).map(Cow::Owned),
                        false => std::str::from_utf8(raw).ok().map(Cow::Borrowed),
                    };
                    let Some(text) = text else {
                        return false;
                    };
                    (text, quoted)
                }
            };
            pairs.push(Pair { key, value, quoted });
            crate::core::buffer::check();
            true
        },
    )
}

fn scan_value<'d>(line: &'d [u8], start: usize, c: &KvConfig) -> Option<(Cow<'d, str>, bool, usize)> {
    let pair_sep = c.pair_sep;
    // Match on `quote` itself: comparing two `Option`s would take the quoted branch when no quote is configured and
    // `start` is at the end.
    match c.quote {
        Some(q) if line.get(start) == Some(&q) => scan_quoted_value(line, start, c),
        _ if c.escapes => {
            let (mut i, mut escaped) = (start, false);
            while i < line.len() && line[i] != pair_sep {
                escaped |= line[i] == b'\\';
                i += 1 + usize::from(line[i] == b'\\');
            }
            let i = i.min(line.len());
            let text = match escaped {
                true => Cow::Owned(unescape(&line[start..i], c)?),
                false => Cow::Borrowed(std::str::from_utf8(&line[start..i]).ok()?),
            };
            Some((text, false, i))
        }
        _ => {
            let mut i = start;
            while i < line.len() && line[i] != pair_sep {
                i += 1;
            }
            let text = std::str::from_utf8(&line[start..i]).ok()?;
            Some((Cow::Borrowed(text), false, i))
        }
    }
}

/// `line[start]` is the opening quote, not yet consumed. Only `pair_sep` or the end may follow the closing quote.
fn scan_quoted_value<'d>(line: &'d [u8], start: usize, c: &KvConfig) -> Option<(Cow<'d, str>, bool, usize)> {
    let (quote, pair_sep) = (c.quote?, c.pair_sep);
    let mut i = start + 1;
    let content_start = i;
    let mut escaped = false;

    loop {
        match line.get(i) {
            Some(&b) if b == quote => {
                if line.get(i + 1).is_some_and(|&b| b != pair_sep) {
                    return None;
                }
                let raw = &line[content_start..i];
                let text = if escaped {
                    Cow::Owned(unescape(raw, c)?)
                } else {
                    Cow::Borrowed(std::str::from_utf8(raw).ok()?)
                };
                return Some((text, true, i + 1));
            }
            Some(b'\\') if line.get(i + 1).is_some() => {
                escaped = true;
                i += 2;
            }
            Some(_) => i += 1,
            None => return None, // unterminated quote
        }
    }
}

/// A value's escapes decoded. Without `escapes`, only an escaped quote or backslash is decoded, and any other escape
/// keeps both characters (`C:\temp` stays). A backslash with nothing after it is itself.
fn unescape(raw: &[u8], c: &KvConfig) -> Option<String> {
    let s = std::str::from_utf8(raw).ok()?;
    let quote = c.quote.map(char::from);
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match (chars.next(), c.escapes) {
            (Some('n'), true) => out.push('\n'),
            (Some('r'), true) => out.push('\r'),
            (Some('t'), true) => out.push('\t'),
            (Some(next), true) => out.push(next),
            (Some(next), false) if next == '\\' || Some(next) == quote => out.push(next),
            (Some(next), false) => out.extend(['\\', next]),
            (None, _) => out.push('\\'),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pairs of a whole record; `None` where it is not whole.
    fn split(line: &[u8], pair_sep: u8, kv_sep: u8, quote: Option<u8>) -> Option<Vec<Pair<'_>>> {
        let mut pairs = Vec::new();
        let c = config(pair_sep, kv_sep, quote);
        split_pairs(line, &c, &mut pairs).then_some(pairs)
    }

    fn config(pair_sep: u8, kv_sep: u8, quote: Option<u8>) -> KvConfig {
        KvConfig {
            pair_sep,
            kv_sep,
            quote,
            ..KvConfig::default()
        }
    }

    fn pairs(line: &str, pair_sep: u8, kv_sep: u8, quote: Option<u8>) -> Vec<(String, String)> {
        split(line.as_bytes(), pair_sep, kv_sep, quote)
            .unwrap()
            .into_iter()
            .map(|p| (p.key.to_string(), p.value.into_owned()))
            .collect()
    }

    #[test]
    fn plain_pairs() {
        assert_eq!(
            pairs("a=1 b=2", b' ', b'=', Some(b'"')),
            vec![("a".into(), "1".into()), ("b".into(), "2".into())]
        );
    }

    #[test]
    fn a_quoted_value_can_contain_the_pair_separator() {
        assert_eq!(
            pairs(r#"msg="hello world" status=200"#, b' ', b'=', Some(b'"')),
            vec![("msg".into(), "hello world".into()), ("status".into(), "200".into())]
        );
    }

    #[test]
    fn escaped_quote_inside_a_quoted_value() {
        let line = "msg=\"say \\\"hi\\\"\"";
        assert_eq!(
            pairs(line, b' ', b'=', Some(b'"')),
            vec![("msg".into(), "say \"hi\"".into())]
        );
    }

    #[test]
    fn repeated_and_trailing_separators_are_skipped() {
        assert_eq!(
            pairs("a=1   b=2  ", b' ', b'=', Some(b'"')),
            vec![("a".into(), "1".into()), ("b".into(), "2".into())]
        );
    }

    #[test]
    fn comma_pair_separator_and_colon_kv_separator() {
        assert_eq!(
            pairs("a:1,b:2", b',', b':', None),
            vec![("a".into(), "1".into()), ("b".into(), "2".into())]
        );
    }

    #[test]
    fn quoted_flag_distinguishes_explicit_quoting() {
        let p = split(br#"a=1 b="2""#, b' ', b'=', Some(b'"')).unwrap();
        assert!(!p[0].quoted);
        assert!(p[1].quoted);
    }

    #[test]
    fn no_quoting_configured_means_no_quote_handling_at_all() {
        assert_eq!(pairs(r#"a="1""#, b' ', b'=', None), vec![("a".into(), "\"1\"".into())]);
    }

    /// An empty value is accepted in every position, with or without a quote character configured.
    #[test]
    fn empty_values_are_accepted_in_every_position() {
        assert_eq!(
            pairs("a= b=2 c=", b' ', b'=', Some(b'"')),
            vec![
                ("a".into(), "".into()),
                ("b".into(), "2".into()),
                ("c".into(), "".into())
            ]
        );
        assert_eq!(
            pairs("x:,y:2,z:", b',', b':', None),
            vec![
                ("x".into(), "".into()),
                ("y".into(), "2".into()),
                ("z".into(), "".into())
            ]
        );
        let p = split(b"a=", b' ', b'=', None).unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].value, "");
        assert!(!p[0].quoted);
    }

    #[test]
    fn a_key_with_no_kv_separator_is_a_bare_key_holding_true() {
        let c = KvConfig {
            next_key: false,
            ..KvConfig::default()
        };
        let mut p = Vec::new();
        assert!(split_pairs(b"level=info debug x=1", &c, &mut p));
        assert_eq!(
            p.into_iter()
                .map(|p| (p.key.to_string(), p.value.into_owned()))
                .collect::<Vec<(String, String)>>(),
            vec![
                ("level".into(), "info".into()),
                ("debug".into(), "true".into()),
                ("x".into(), "1".into())
            ]
        );
    }

    #[test]
    fn a_record_that_breaks_off_keeps_the_pairs_before_it() {
        let mut pairs = Vec::new();
        for line in [&br#"a=1 b="cut"#[..], br#"a=1 b="x"junk"#] {
            assert!(!split_pairs(line, &KvConfig::default(), &mut pairs));
            assert_eq!((pairs.len(), pairs[0].key), (1, "a"));
        }
    }

    #[test]
    fn escapes_decode_a_backslash_before_any_character_quoted_or_not() {
        let c = KvConfig {
            escapes: true,
            ..KvConfig::default()
        };
        let read = |line: &'static str| {
            let mut pairs = Vec::new();
            assert!(split_pairs(line.as_bytes(), &c, &mut pairs));
            pairs.into_iter().map(|p| p.value.into_owned()).collect::<Vec<_>>()
        };
        assert_eq!(read(r"msg=a\=b\ c d=e\"), ["a=b c", "e\\"]);
        assert_eq!(read(r#"m="line\nnext\t\q" p=C:\\temp"#), ["line\nnext\tq", "C:\\temp"]);
        assert_eq!(
            pairs(r#"p="C:\temp""#, b' ', b'=', Some(b'"')),
            [("p".into(), "C:\\temp".into())]
        );
    }

    #[test]
    fn a_value_runs_to_the_separator_before_the_next_key() {
        assert_eq!(
            pairs("msg=user logged in  status=200 debug", b' ', b'=', Some(b'"')),
            vec![
                ("msg".into(), "user logged in".into()),
                ("status".into(), "200 debug".into())
            ]
        );
    }

    #[test]
    fn a_kv_separator_with_no_pair_separator_before_it_is_the_value_s_own() {
        assert_eq!(
            pairs("url=http://x/?a=1&b=2 n=1", b' ', b'=', None),
            vec![("url".into(), "http://x/?a=1&b=2".into()), ("n".into(), "1".into())]
        );
    }

    #[test]
    fn spaces_around_keys_and_values_are_trimmed_and_a_quoted_value_still_wins() {
        assert_eq!(
            pairs(
                r#"src: 1.2.3.4; product: VPN-1 & FireWall-1; msg: "a; b: c" ;"#,
                b';',
                b':',
                Some(b'"')
            ),
            vec![
                ("src".into(), "1.2.3.4".into()),
                ("product".into(), "VPN-1 & FireWall-1".into()),
                ("msg".into(), "a; b: c".into())
            ]
        );
    }

    #[test]
    fn a_backslash_quote_closes_its_value() {
        assert_eq!(
            pairs(r"a=\x\ b=1", b' ', b'=', Some(b'\\')),
            vec![("a".into(), "x".into()), ("b".into(), "1".into())]
        );
    }

    #[test]
    fn words_before_the_first_key_are_bare_keys() {
        assert_eq!(
            pairs("debug  a=1", b' ', b'=', None),
            vec![("debug".into(), "true".into()), ("a".into(), "1".into())]
        );
    }

    #[test]
    fn an_escaped_separator_neither_ends_a_value_nor_starts_a_key() {
        let c = KvConfig {
            escapes: true,
            ..KvConfig::default()
        };
        let mut p = Vec::new();
        assert!(split_pairs(br"a=x\ y\=z w b\=c d=1", &c, &mut p));
        assert_eq!(
            p.into_iter()
                .map(|p| (p.key.to_string(), p.value.into_owned()))
                .collect::<Vec<(String, String)>>(),
            vec![("a".into(), "x y=z w b=c".into()), ("d".into(), "1".into())]
        );
    }

    #[test]
    fn empty_input_yields_no_pairs() {
        assert_eq!(split(b"", b' ', b'=', Some(b'"')).unwrap().len(), 0);
    }

    #[test]
    fn invalid_utf8_is_rejected() {
        let mut line = vec![0xFF];
        line.extend_from_slice(b"=1");
        assert!(split(&line, b' ', b'=', Some(b'"')).is_none());

        let mut line = b"a=".to_vec();
        line.push(0xFF);
        assert!(split(&line, b' ', b'=', Some(b'"')).is_none());
    }
}
