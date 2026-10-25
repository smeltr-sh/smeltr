//! Grok macro expansion: `%{NAME}` / `%{NAME:field}` in a pattern string become a non-capturing or named group of the
//! named pattern's regex; everything else passes through as regex syntax, as in logstash.
//!
//! `BUILTINS` is a curated library written against what the `regex` crate supports (no backreferences, no lookarounds),
//! not an import of the logstash set. `Matcher::compile` refusing a pattern is the audit for anything added later.

use crate::plan::config::{GrokConfig, GrokPatterns};

/// Extend through `GrokConfig::custom_patterns` or a bundle's `GrokPatterns` rather than here.
pub const BUILTINS: &[(&str, &str)] = &[
    ("WORD", r"\b\w+\b"),
    ("NOTSPACE", r"\S+"),
    ("SPACE", r"\s*"),
    ("INT", r"[+-]?[0-9]+"),
    ("POSINT", r"[1-9][0-9]*"),
    ("BASE10NUM", r"[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)"),
    ("BASE16NUM", r"0[xX][0-9A-Fa-f]+"),
    ("NUMBER", r"%{BASE10NUM}"),
    // The word boundaries keep a search from starting or ending inside a word or number: `01.2.3.4` is not `1.2.3.4`.
    (
        "IPV4",
        r"(?-u:\b)(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(?-u:\b)",
    ),
    // Every RFC 4291 text form, and a zone (`fe80::1%eth0`) in RFC 6874's characters, as Logstash takes one. A `::`
    // form is tried with the most groups before it first, and an embedded IPv4 form before the rest, so the first
    // alternative to match takes the whole address.
    (
        "IPV6",
        concat!(
            r"(?:",
            r"(?:",
            r"(?:[0-9A-Fa-f]{1,4}:){6}|",
            r"(?:[0-9A-Fa-f]{1,4}:){5}:|",
            r"(?:[0-9A-Fa-f]{1,4}:){4}:(?:[0-9A-Fa-f]{1,4}:){0,1}|",
            r"(?:[0-9A-Fa-f]{1,4}:){3}:(?:[0-9A-Fa-f]{1,4}:){0,2}|",
            r"(?:[0-9A-Fa-f]{1,4}:){2}:(?:[0-9A-Fa-f]{1,4}:){0,3}|",
            r"[0-9A-Fa-f]{1,4}::(?:[0-9A-Fa-f]{1,4}:){0,4}|",
            r"::(?:[0-9A-Fa-f]{1,4}:){0,5}",
            r")%{IPV4}|",
            r"(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|",
            r"(?:[0-9A-Fa-f]{1,4}:){7}:|",
            r"(?:[0-9A-Fa-f]{1,4}:){6}:(?:[0-9A-Fa-f]{1,4})?|",
            r"(?:[0-9A-Fa-f]{1,4}:){5}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,1})?|",
            r"(?:[0-9A-Fa-f]{1,4}:){4}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,2})?|",
            r"(?:[0-9A-Fa-f]{1,4}:){3}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,3})?|",
            r"(?:[0-9A-Fa-f]{1,4}:){2}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,4})?|",
            r"[0-9A-Fa-f]{1,4}::(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,5})?|",
            r"::(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,6})?",
            r")(?:%[0-9A-Za-z._~-]+)?",
        ),
    ),
    // IPv4 first: it fails within a few bytes on an IPv6 address, which never starts with a dotted quad.
    ("IP", r"(?:%{IPV4}|%{IPV6})"),
    (
        "HOSTNAME",
        r"\b[0-9A-Za-z](?:[0-9A-Za-z-]{0,62}[0-9A-Za-z])?(?:\.[0-9A-Za-z](?:[0-9A-Za-z-]{0,62}[0-9A-Za-z])?)*\b",
    ),
    // IP first, as Logstash has it: HOSTNAME also takes a dotted quad, but would stop an IPv6 address at its first `:`.
    ("IPORHOST", r"(?:%{IP}|%{HOSTNAME})"),
    ("USERNAME", r"[a-zA-Z0-9._-]+"),
    (
        "UUID",
        r"[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}",
    ),
    ("MAC", r"(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}"),
    ("DATA", r".*?"),
    ("GREEDYDATA", r".*"),
    ("QUOTEDSTRING", r#"(?:"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')"#),
    (
        "LOGLEVEL",
        r"(?i)(?:trace|debug|info|notice|warn(?:ing)?|err(?:or)?|crit(?:ical)?|fatal|severe|emerg(?:ency)?|alert)",
    ),
    (
        "MONTH",
        r"(?i)(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)",
    ),
    ("MONTHNUM", r"(?:0?[1-9]|1[0-2])"),
    ("MONTHDAY", r"(?:0?[1-9]|[12][0-9]|3[01])"),
    ("YEAR", r"(?:[0-9]{2}){1,2}"),
    ("HOUR", r"(?:2[0-3]|[01]?[0-9])"),
    ("MINUTE", r"[0-5][0-9]"),
    ("SECOND", r"(?:[0-5]?[0-9](?:[.,][0-9]+)?|60)"),
    ("TIME", r"%{HOUR}:%{MINUTE}(?::%{SECOND})?"),
    (
        "TIMESTAMP_ISO8601",
        r"%{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{TIME}(?:Z|[+-]%{HOUR}:?%{MINUTE})?",
    ),
    ("HTTPDATE", r"%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}"),
    ("SYSLOGTIMESTAMP", r"%{MONTH} +%{MONTHDAY} %{TIME}"),
    ("SYSLOGHOST", r"%{IPORHOST}"),
    ("URIPROTO", r"[A-Za-z][A-Za-z0-9+.-]*"),
    ("URIHOST", r"(?:%{IPV4}|%{HOSTNAME})(?::%{POSINT})?"),
    ("URIPATH", r#"[A-Za-z0-9$.+!*'(){},~:;=@#%_/-]*"#),
    ("URIPATHPARAM", r"%{URIPATH}(?:\?\S*)?"),
    // Whole lines, each naming its fields as the Elastic Common Schema does, so a field nests where its name has dots.
    // A BSD syslog line (RFC 3164) as a relay or a file writes it, the PRI and the tag optional, its timestamp the
    // classic one or an ISO 8601 one.
    (
        "SYSLOGLINE",
        concat!(
            r"(?:<(?P<log.syslog.priority>[0-9]{1,3})>)?",
            r"(?P<timestamp>%{SYSLOGTIMESTAMP}|%{TIMESTAMP_ISO8601}) %{NOTSPACE:host.hostname}",
            r"(?: (?P<process.name>[\x21-\x5a\x5c\x5e-\x7e]+)(?:\[%{POSINT:process.pid}\])?:)? %{GREEDYDATA:message}",
        ),
    ),
    // An RFC 5424 line, each `-` a field the sender left out. STRUCTURED-DATA is one or more `[id param="value" ...]`,
    // a value escaping `"`, `\` and `]`.
    (
        "SYSLOG5424LINE",
        concat!(
            r"<(?P<log.syslog.priority>[0-9]{1,3})>%{POSINT:log.syslog.version} ",
            r"(?:-|%{TIMESTAMP_ISO8601:timestamp}) (?:-|%{NOTSPACE:host.hostname}) ",
            r"(?:-|%{NOTSPACE:log.syslog.appname}) (?:-|%{NOTSPACE:log.syslog.procid}) ",
            r"(?:-|%{NOTSPACE:log.syslog.msgid}) ",
            r#"(?:-|(?P<log.syslog.structured_data>(?:\[(?:[^\]"\\]|\\.|"(?:[^"\\]|\\.)*")*\])+))"#,
            r"(?: %{GREEDYDATA:message})?",
        ),
    ),
    // The NCSA common log format, and Apache's combined format, which adds the referrer and the user agent. A `-` is a
    // field the server left out, and a request line that is not `method target HTTP/version` names nothing.
    (
        "COMMONAPACHELOG",
        concat!(
            r"%{IPORHOST:source.address} (?:-|%{NOTSPACE:apache.access.user.identity}) (?:-|%{NOTSPACE:user.name}) ",
            r#"\[%{HTTPDATE:timestamp}\] "(?:(?P<http.request.method>[!#$%&'*+.^_`|~0-9A-Za-z-]+) %{NOTSPACE:url.original}"#,
            r#"(?: HTTP/%{NUMBER:http.version})?|%{DATA})" "#,
            r"(?P<http.response.status_code>[0-9]{3}) (?:(?P<http.response.body.bytes>[0-9]+)|-)",
        ),
    ),
    (
        "COMBINEDAPACHELOG",
        r#"%{COMMONAPACHELOG} "(?:-|%{DATA:http.request.referrer})" "(?:-|%{DATA:user_agent.original})""#,
    ),
];

/// Bound on macro nesting. A cycle is caught by name before this; the bound keeps a long chain from exhausting the
/// stack.
const MAX_DEPTH: u32 = 32;
/// Bound on what a pattern expands to. Nesting alone bounds nothing: a pattern naming another twice doubles at every
/// level, so thirty levels of that is a gigabyte before `regex` sees it. A real pattern is kilobytes.
const MAX_EXPANDED: usize = 1 << 20;

#[derive(Debug, PartialEq, Eq)]
pub struct GrokError(pub String);

impl std::fmt::Display for GrokError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for GrokError {}

/// Expand every macro in `pattern`, resolving names against `config.custom_patterns` first and `BUILTINS` second,
/// recursively.
pub fn expand(pattern: &str, config: &GrokConfig) -> Result<String, GrokError> {
    expand_with(pattern, config, &GrokPatterns::new())
}

/// `expand`, with the bundle's `shared` patterns resolved between the step's own and the built-ins.
pub fn expand_with(pattern: &str, config: &GrokConfig, shared: &GrokPatterns) -> Result<String, GrokError> {
    expand_at_depth(pattern, config, shared, true, &mut Vec::new())
}

/// Whether `name` is a built-in pattern, which a bundle's pattern may not take: shadowing it would change every
/// pattern in the group that uses it.
pub fn is_builtin(name: &str) -> bool {
    BUILTINS.iter().any(|(n, _)| *n == name)
}

/// Where a pattern is defined. A step's own may shadow a built-in, so a name alone does not say which pattern it is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Defined {
    Step,
    Bundle,
    BuiltIn,
}

/// The pattern `name` resolves to, as `(name, where it is defined)` and its definition, borrowed from there. The
/// step's own patterns are looked in only where `own`.
fn lookup<'a>(
    name: &str,
    config: &'a GrokConfig,
    shared: &'a GrokPatterns,
    own: bool,
) -> Option<((&'a str, Defined), &'a str)> {
    config
        .custom_patterns
        .iter()
        .find(|(n, _)| own && n == name)
        .map(|(n, v)| ((n.as_str(), Defined::Step), v.as_str()))
        .or_else(|| {
            shared
                .get_key_value(name)
                .map(|(n, v)| ((n.as_str(), Defined::Bundle), v.as_str()))
        })
        .or_else(|| {
            BUILTINS
                .iter()
                .find(|(n, _)| *n == name)
                .map(|&(n, v)| ((n, Defined::BuiltIn), v))
        })
}

/// `chain` is the patterns being expanded, outermost first; one already in it is a cycle. `own` is whether the step's
/// own patterns are in scope: not inside a bundle's pattern, which reads the same in every step.
fn expand_at_depth<'a>(
    pattern: &str,
    config: &'a GrokConfig,
    shared: &'a GrokPatterns,
    own: bool,
    chain: &mut Vec<(&'a str, Defined)>,
) -> Result<String, GrokError> {
    let mut out = String::with_capacity(pattern.len() * 2);
    let mut i = 0;
    while i < pattern.len() {
        if pattern.as_bytes()[i] == b'%' && pattern.as_bytes().get(i + 1) == Some(&b'{') {
            let start = i + 2;
            let Some(rel_end) = pattern[start..].find('}') else {
                return Err(GrokError(format!("unterminated `%{{` in pattern `{pattern}`")));
            };
            let end = start + rel_end;
            let body = &pattern[start..end];
            let (name, field) = match body.split_once(':') {
                Some((n, f)) => (n, Some(f)),
                None => (body, None),
            };
            if name.is_empty() {
                return Err(GrokError(format!("empty pattern name in `%{{{body}}}`")));
            }
            let Some((which, def)) = lookup(name, config, shared, own) else {
                return Err(GrokError(format!("unknown grok pattern `%{{{name}}}`")));
            };
            let name = which.0;
            if chain.contains(&which) {
                return Err(GrokError(format!(
                    "patterns name each other in a loop: {}",
                    path(chain, name)
                )));
            }
            if chain.len() as u32 >= MAX_DEPTH {
                return Err(GrokError(format!(
                    "patterns nest more than {MAX_DEPTH} deep: {}",
                    path(chain, name)
                )));
            }
            chain.push(which);
            let inner = expand_at_depth(def, config, shared, own && which.1 != Defined::Bundle, chain);
            chain.pop();
            let inner = inner?;
            match field {
                Some(f) => {
                    if !is_valid_field_name(f) {
                        return Err(GrokError(format!(
                            "invalid field name `{f}` in `%{{{body}}}` \
                             (each dot-separated segment must be ASCII letters/digits/underscore, \
                             not starting with a digit)"
                        )));
                    }
                    out.push_str("(?P<");
                    out.push_str(f);
                    out.push('>');
                    out.push_str(&inner);
                    out.push(')');
                }
                None => {
                    out.push_str("(?:");
                    out.push_str(&inner);
                    out.push(')');
                }
            }
            i = end + 1;
            if out.len() > MAX_EXPANDED {
                return Err(GrokError(format!("pattern `{pattern}` expands to more than 1MB")));
            }
        } else {
            let ch = pattern[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Ok(out)
}

/// `pattern` with every built-in that captures a field written out as its definition, recursively, so each field it
/// fills shows where an author can edit it; a built-in that captures nothing, such as `%{NOTSPACE:host}`, stays a
/// name. It reads the same rows into the same fields as `pattern`. A name that is not a built-in stays as it is.
pub fn unfold(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut rest = pattern;
    while let Some(at) = rest.find("%{") {
        out.push_str(&rest[..at]);
        let Some(len) = rest[at..].find('}') else {
            out.push_str(&rest[at..]);
            return out;
        };
        let body = &rest[at + 2..at + len];
        let (name, field) = match body.split_once(':') {
            Some((n, f)) => (n, Some(f)),
            None => (body, None),
        };
        let def = BUILTINS.iter().find(|(n, _)| *n == name).map(|(_, d)| *d);
        match def.filter(|d| captures(d)) {
            Some(def) => {
                let inner = unfold(def);
                match field {
                    Some(f) => out.push_str(&format!("(?P<{f}>{inner})")),
                    // A quantifier after the macro would bind to the definition's last atom alone.
                    None if concatenates(def) && !rest[at + len + 1..].starts_with(['?', '*', '+', '{']) => {
                        out.push_str(&inner)
                    }
                    None => out.push_str(&format!("(?:{inner})")),
                }
            }
            None => out.push_str(&rest[at..=at + len]),
        }
        rest = &rest[at + len + 1..];
    }
    out.push_str(rest);
    out
}

/// Whether a built-in's definition captures a field, itself or through a built-in it names.
fn captures(def: &str) -> bool {
    let none = GrokConfig {
        patterns: Vec::new(),
        custom_patterns: Vec::new(),
        split_dots: false,
    };
    expand(def, &none).is_ok_and(|e| e.contains("(?P<"))
}

/// Whether `def` can be concatenated with the text around its macro with no group around it: it has no top-level `|`
/// to split the pattern around it, and no top-level inline flag such as `(?i)` to reach the text after it.
fn concatenates(def: &str) -> bool {
    let bytes = def.as_bytes();
    let (mut depth, mut i, mut class) = (0usize, 0, false);
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 1,
            b'[' if !class => class = true,
            b']' if class => class = false,
            _ if class => {}
            b'(' => {
                if depth == 0 && bytes.get(i + 1) == Some(&b'?') {
                    let flags = def[i + 2..].find([':', ')', '<', 'P', '=', '!']);
                    if flags.is_some_and(|f| bytes[i + 2 + f] == b')') {
                        return false;
                    }
                }
                depth += 1;
            }
            b')' => depth = depth.saturating_sub(1),
            b'|' if depth == 0 => return false,
            _ => {}
        }
        i += 1;
    }
    true
}

/// The names from the outermost to `next`, as an author follows them: `A → B → A`.
fn path(chain: &[(&str, Defined)], next: &str) -> String {
    chain
        .iter()
        .map(|(n, _)| *n)
        .chain([next])
        .collect::<Vec<_>>()
        .join(" → ")
}

/// A field name: dot-separated segments, each ASCII letters/digits/underscore and not starting with a digit.
///
/// `regex` accepts far more than this in a named group -- `a[0]`, a trailing dot, an empty segment -- and truncates a
/// name at the first `>`, so `a>b` would silently leak `b>` into the pattern as regex. The gate is this crate's, not
/// the engine's: a segment rule refuses all of that, and admits the dotted name the data model asks for.
fn is_valid_field_name(name: &str) -> bool {
    name.split('.').all(is_valid_segment)
}

fn is_valid_segment(seg: &str) -> bool {
    let mut chars = seg.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::grok_scan::Matcher;

    fn cfg(patterns: &[(&str, &str)]) -> GrokConfig {
        GrokConfig {
            split_dots: true,
            patterns: vec![],
            custom_patterns: patterns.iter().map(|(n, d)| (n.to_string(), d.to_string())).collect(),
        }
    }

    /// `pattern` compiled the way a grok parser compiles it.
    fn matcher(pattern: &str) -> Matcher {
        matcher_with(pattern, &[])
    }

    fn matcher_with(pattern: &str, custom: &[(&str, &str)]) -> Matcher {
        Matcher::compile(&GrokConfig {
            split_dots: true,
            patterns: vec![pattern.to_string()],
            ..cfg(custom)
        })
        .unwrap()
    }

    /// A pattern naming another twice doubles at every level: thirty levels is a gigabyte, refused by size long before
    /// the nesting bound sees it.
    #[test]
    fn a_doubling_pattern_is_refused_by_size() {
        let mut custom = vec![("P0".to_string(), "a".to_string())];
        for n in 1..=30 {
            custom.push((format!("P{n}"), format!("%{{P{}}}%{{P{}}}", n - 1, n - 1)));
        }
        let custom: Vec<(&str, &str)> = custom.iter().map(|(n, d)| (n.as_str(), d.as_str())).collect();
        let err = expand("%{P30}", &cfg(&custom)).unwrap_err();
        assert!(err.0.contains("expands to more than"), "{err}");
    }

    #[test]
    fn literal_text_passes_through_unchanged() {
        let out = expand(r"^\[error\]", &cfg(&[])).unwrap();
        assert_eq!(out, r"^\[error\]");
    }

    /// A macro's body is one atom, named or not. Its alternation cannot split the author's pattern, and its inline
    /// flag cannot reach the author's text. LOGLEVEL and MONTH open with `(?i)`.
    #[test]
    fn a_macro_is_one_atom_whether_named_or_not() {
        for pattern in ["^%{AB}c$", "^%{AB:x}c$"] {
            let m = matcher_with(pattern, &[("AB", "a|b")]);
            assert!(m.match_row("bc").is_some(), "{pattern}");
            assert!(m.match_row("a").is_none(), "{pattern}: the alternation split it");
        }
        for pattern in ["^%{LOGLEVEL} ERROR$", "^%{LOGLEVEL:level} ERROR$"] {
            let m = matcher(pattern);
            assert!(m.match_row("WARN ERROR").is_some(), "{pattern}");
            assert!(m.match_row("warn error").is_none(), "{pattern}: the (?i) got out");
        }
    }

    /// A pattern, a line it reads, and the fields it reads from that line.
    type LineCase<'a> = (&'a str, &'a str, &'a [(&'a str, Option<&'a str>)]);

    /// Each whole-line pattern names its fields, `None` being a field the line leaves out.
    #[test]
    fn a_whole_line_pattern_names_its_fields() {
        let cases: &[LineCase] = &[
            (
                "SYSLOGLINE",
                "<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed for lonvick on /dev/pts/8",
                &[
                    ("log.syslog.priority", Some("34")),
                    ("timestamp", Some("Oct 11 22:14:15")),
                    ("host.hostname", Some("mymachine")),
                    ("process.name", Some("su")),
                    ("process.pid", Some("230")),
                    ("message", Some("'su root' failed for lonvick on /dev/pts/8")),
                ],
            ),
            (
                "SYSLOGLINE",
                "2026-10-11T22:14:15.003+00:00 web_01 kernel link up",
                &[
                    ("log.syslog.priority", None),
                    ("timestamp", Some("2026-10-11T22:14:15.003+00:00")),
                    ("host.hostname", Some("web_01")),
                    ("process.name", None),
                    ("message", Some("kernel link up")),
                ],
            ),
            (
                "SYSLOG5424LINE",
                r#"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Appl\]ication"] An application event"#,
                &[
                    ("log.syslog.priority", Some("165")),
                    ("log.syslog.version", Some("1")),
                    ("timestamp", Some("2003-10-11T22:14:15.003Z")),
                    ("host.hostname", Some("mymachine.example.com")),
                    ("log.syslog.appname", Some("evntslog")),
                    ("log.syslog.procid", None),
                    ("log.syslog.msgid", Some("ID47")),
                    (
                        "log.syslog.structured_data",
                        Some(r#"[exampleSDID@32473 iut="3" eventSource="Appl\]ication"]"#),
                    ),
                    ("message", Some("An application event")),
                ],
            ),
            (
                "SYSLOG5424LINE",
                "<34>1 - - - - - -",
                &[
                    ("timestamp", None),
                    ("host.hostname", None),
                    ("log.syslog.structured_data", None),
                    ("message", None),
                ],
            ),
            (
                "COMMONAPACHELOG",
                r#"127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326"#,
                &[
                    ("source.address", Some("127.0.0.1")),
                    ("apache.access.user.identity", None),
                    ("user.name", Some("frank")),
                    ("timestamp", Some("10/Oct/2000:13:55:36 -0700")),
                    ("http.request.method", Some("GET")),
                    ("url.original", Some("/apache_pb.gif")),
                    ("http.version", Some("1.0")),
                    ("http.response.status_code", Some("200")),
                    ("http.response.body.bytes", Some("2326")),
                ],
            ),
            (
                "COMMONAPACHELOG",
                r#"10.0.0.2 - - [10/Oct/2000:13:55:36 -0700] "\x16\x03" 400 -"#,
                &[
                    ("http.request.method", None),
                    ("url.original", None),
                    ("http.response.body.bytes", None),
                ],
            ),
            (
                "COMMONAPACHELOG",
                r#"10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "M-SEARCH * HTTP/1.1" 200 0"#,
                &[("http.request.method", Some("M-SEARCH")), ("url.original", Some("*"))],
            ),
            (
                "COMBINEDAPACHELOG",
                r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 304 - "http://example.com/" "Mozilla/5.0 (X11; Linux)""#,
                &[
                    ("http.response.status_code", Some("304")),
                    ("http.request.referrer", Some("http://example.com/")),
                    ("user_agent.original", Some("Mozilla/5.0 (X11; Linux)")),
                ],
            ),
            (
                "COMBINEDAPACHELOG",
                r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 200 1 "-" "-""#,
                &[("http.request.referrer", None), ("user_agent.original", None)],
            ),
        ];
        for (name, line, fields) in cases {
            let folded = format!("^%{{{name}}}$");
            for pattern in [unfold(&folded), folded] {
                let m = matcher(&pattern);
                let got = m
                    .match_row(line)
                    .unwrap_or_else(|| panic!("{pattern} does not match {line:?}"));
                for (field, want) in *fields {
                    assert_eq!(got.name(field), *want, "{pattern} on {line:?}: {field}");
                }
            }
        }
        assert!(
            matcher("^%{SYSLOG5424LINE}$")
                .match_row("<34>Oct 11 22:14:15 host su: x")
                .is_none()
        );
        let common = r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 200 1"#;
        assert!(matcher("^%{COMBINEDAPACHELOG}$").match_row(common).is_none());
    }

    /// Unfolding writes out every built-in that captures, nested ones too, and leaves the rest named: the fields are
    /// the folded pattern's.
    #[test]
    fn unfolding_writes_out_what_captures() {
        let folded = "^%{COMBINEDAPACHELOG}$";
        let unfolded = unfold(folded);
        assert!(!unfolded.contains("APACHELOG"), "{unfolded}");
        assert!(unfolded.starts_with("^%{IPORHOST:source.address} "), "{unfolded}");
        assert_eq!(matcher(&unfolded).capture_names(), matcher(folded).capture_names());
        for kept in ["%{WORD:w} %{INT}", "%{NOPE} x", r"\%{"] {
            assert_eq!(unfold(kept), kept);
        }
        let quantified = unfold("%{SYSLOGLINE}?");
        assert!(
            quantified.starts_with("(?:") && quantified.ends_with(")?"),
            "{quantified}"
        );
        assert_eq!(
            unfold("%{SYSLOGLINE:line}"),
            format!("(?P<line>{})", unfold("%{SYSLOGLINE}"))
        );
    }

    /// A definition goes in place of its macro bare only where nothing in it can reach past it.
    #[test]
    fn concatenating_is_free_of_top_level_alternation_and_flags() {
        for atom in ["ab", "(?:a|b)c", "[|]", r"\|", "(?i:a)b", "(?P<x>a|b)", r"(?-u:\b)a"] {
            assert!(concatenates(atom), "{atom}");
        }
        for split in ["a|b", "(?i)ab", "x(?-u)y"] {
            assert!(!concatenates(split), "{split}");
        }
    }

    #[test]
    fn an_unnamed_macro_captures_nothing() {
        assert!(matcher("%{WORD}").capture_names().is_empty());
    }

    #[test]
    fn built_in_composite_patterns_expand_recursively() {
        let out = expand("%{NUMBER:n}", &cfg(&[])).unwrap();
        assert!(!out.contains('%'), "still has an unexpanded macro: {out}");
        assert!(out.starts_with("(?P<n>"));
    }

    #[test]
    fn custom_pattern_shadows_a_built_in_of_the_same_name() {
        let m = matcher_with("^%{WORD}$", &[("WORD", "xyz")]);
        assert!(m.match_row("xyz").is_some());
        assert!(m.match_row("abc").is_none(), "the built-in WORD answered");
    }

    /// GREETING reaches NAME, a custom pattern, and NAME reaches WORD, a built-in.
    #[test]
    fn custom_patterns_can_reference_each_other_and_built_ins() {
        let custom = [("GREETING", "hi %{NAME}"), ("NAME", "%{WORD}!")];
        let m = matcher_with("^%{GREETING:g}$", &custom);
        assert_eq!(m.match_row("hi bob!").unwrap().name("g"), Some("hi bob!"));
    }

    fn shared(patterns: &[(&str, &str)]) -> GrokPatterns {
        patterns.iter().map(|(n, d)| (n.to_string(), d.to_string())).collect()
    }

    #[test]
    fn a_cycle_names_the_patterns_it_runs_through() {
        let err = expand("%{A}", &cfg(&[("A", "x%{B}"), ("B", "%{A}y")])).unwrap_err();
        assert_eq!(err.0, "patterns name each other in a loop: A → B → A");
    }

    #[test]
    fn a_step_names_a_shared_pattern_and_the_shared_one_names_a_built_in() {
        let config = GrokConfig {
            split_dots: true,
            patterns: vec!["^%{VENDOR:v}$".to_string()],
            custom_patterns: vec![],
        };
        let m = Matcher::compile_with(&config, &shared(&[("VENDOR", "acme-%{INT}")])).unwrap();
        assert_eq!(m.match_row("acme-42").unwrap().name("v"), Some("acme-42"));
    }

    /// One name, one pattern, in a group: a step that took a shared name would read differently from its neighbor.
    /// A step's own `INT` shadows the built-in where the step names it, and not inside the bundle's `VENDOR`.
    #[test]
    fn a_steps_own_pattern_does_not_reach_inside_a_shared_one() {
        let config = GrokConfig {
            split_dots: true,
            patterns: vec!["^%{VENDOR:v} %{INT:n}$".to_string()],
            custom_patterns: vec![("INT".to_string(), "[a-z]+".to_string())],
        };
        let m = Matcher::compile_with(&config, &shared(&[("VENDOR", "acme-%{INT}")])).unwrap();
        let row = m.match_row("acme-42 abc").unwrap();
        assert_eq!((row.name("v"), row.name("n")), (Some("acme-42"), Some("abc")));
    }

    /// The step's `INT` names the bundle's `VENDOR`, which names the built-in `INT`: two patterns, so no cycle.
    #[test]
    fn a_steps_pattern_and_the_built_in_it_shadows_are_two_patterns() {
        let config = GrokConfig {
            split_dots: true,
            patterns: vec!["^%{INT:i}$".to_string()],
            custom_patterns: vec![("INT".to_string(), "<%{VENDOR}>".to_string())],
        };
        let m = Matcher::compile_with(&config, &shared(&[("VENDOR", "acme-%{INT}")])).unwrap();
        assert!(m.match_row("<acme-42>").is_some());
    }

    #[test]
    fn a_step_cannot_take_a_shared_patterns_name() {
        let config = GrokConfig {
            split_dots: true,
            patterns: vec!["%{VENDOR}".to_string()],
            custom_patterns: vec![("VENDOR".to_string(), "x".to_string())],
        };
        let err = Matcher::compile_with(&config, &shared(&[("VENDOR", "y")])).unwrap_err();
        assert!(err.0.contains("already on the Grok patterns page"), "{err}");
    }

    #[test]
    fn a_shared_pattern_is_checked_against_shared_ones_and_built_ins_alone() {
        let problems = Matcher::check_shared(&shared(&[
            ("WORD", "x"),
            ("bad name", "x"),
            ("LOCAL", "%{ONLY_IN_A_STEP}"),
            ("UNCLOSED", "(%{INT}"),
            ("FINE", "%{INT}-%{LOCAL_OK}"),
            ("LOCAL_OK", "%{WORD}"),
        ]));
        let names: Vec<&str> = problems.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["LOCAL", "UNCLOSED", "WORD", "bad name"]);
    }

    #[test]
    fn malformed_macros_are_errors() {
        assert!(expand("%{NOPE}", &cfg(&[])).is_err());
        assert!(expand("%{WORD", &cfg(&[])).is_err());
    }

    #[test]
    fn a_dotted_capture_names_one_flat_field() {
        let m = matcher("%{WORD:source.ip}");
        assert_eq!(m.capture_names(), vec!["source.ip"]);
        assert_eq!(m.match_row("web01").unwrap().name("source.ip"), Some("web01"));
    }

    /// Each built-in pattern: text it takes whole, and text it does not take at all.
    const CASES: &[(&str, &[&str], &[&str])] = &[
        ("WORD", &["a", "Abc_12", "_"], &["", "a b", "a-b", "a.b"]),
        ("NOTSPACE", &["a", "a-b.c/d?e=f", "\"x\""], &["", "a b", "a\tb", " "]),
        ("SPACE", &["", " ", "  \t "], &["x", " x "]),
        (
            "INT",
            &["0", "42", "-7", "+007"],
            &["", "1.5", "1e3", "--1", "0x10", "1 2"],
        ),
        ("POSINT", &["1", "42", "1000"], &["", "0", "01", "-1", "+1", "1.0"]),
        (
            "BASE10NUM",
            &["0", "-1", "+1.5", "3.25", ".5", "-.5"],
            &["", "1.", "1e3", "1.2.3", "abc", "+"],
        ),
        (
            "BASE16NUM",
            &["0x0", "0XdeadBEEF", "0x1f"],
            &["", "0x", "x10", "0xg1", "1f"],
        ),
        ("NUMBER", &["0", "-1", "3.25", ".5"], &["", "1.", "1e3", "NaN"]),
        (
            "IPV4",
            &["0.0.0.0", "10.1.2.3", "192.168.0.1", "255.255.255.255"],
            &[
                "",
                "256.1.1.1",
                "1.2.3",
                "1.2.3.4.5",
                "1.2.3.",
                "a.b.c.d",
                "1.2.3.-4",
                "1..2.3",
                "01.02.03.04",
                "1.2.3.04",
            ],
        ),
        (
            "IPV6",
            &[
                "2001:0db8:0000:0000:0000:0000:0000:0001",
                "2001:db8:7060::febe",
                "fe80::1",
                "1::2:3:4",
                "2001:db8::",
                "1:2:3:4:5:6:7::",
                "::2:3:4:5:6:7:8",
                "::1",
                "::",
                "::ffff:10.1.2.3",
                "64:ff9b::192.0.2.33",
                "1:2:3:4:5:6:1.2.3.4",
                "fe80::1%eth0",
                "fe80::a:b%en0.100",
            ],
            &[
                "",
                "12:34:56",
                "1:2:3",
                "1::2::3",
                ":::",
                "1:2:3:4:5:6:7:8:9",
                "1:2:3:4:5:6:7:8::",
                "1:2:3:4:5:6:7::8:9",
                "12345::1",
                "gg::1",
                "1:2:3:4:5:6:7:1.2.3.4",
                "::1.2.3.256",
                "fe80::1%",
                "fe80::1%eth 0",
                "::ffff:01.2.3.4",
            ],
        ),
        (
            "IP",
            &["10.1.2.3", "2001:db8::1", "::ffff:10.1.2.3", "fe80::1%eth0"],
            &["", "256.1.1.1", "12:34:56", "host", "01.02.03.04", "10.1.2.3%eth0"],
        ),
        (
            "HOSTNAME",
            &[
                "localhost",
                "web-01",
                "a.b.example.com",
                "1.2.3.4",
                "xn--bcher-kva.example",
            ],
            &["", "-web", "web-", "a..b", "a_b", "a b", ".a", "a.b."],
        ),
        (
            "IPORHOST",
            &[
                "10.1.2.3",
                "2001:db8::1",
                "fe80::1%eth0",
                "localhost",
                "a.b.example.com",
            ],
            &["", "-web", "a_b", "a b", "10.1.2.3:443", "a.b."],
        ),
        ("USERNAME", &["alice", "a.b_c-d", "42"], &["", "a b", "a@b", "a/b"]),
        (
            "UUID",
            &[
                "123e4567-e89b-12d3-a456-426614174000",
                "ABCDEF01-2345-6789-ABCD-EF0123456789",
            ],
            &[
                "",
                "123e4567e89b12d3a456426614174000",
                "123e4567-e89b-12d3-a456-42661417400",
                "g23e4567-e89b-12d3-a456-426614174000",
            ],
        ),
        (
            "MAC",
            &["00:1a:2B:3c:4D:5e", "ff:ff:ff:ff:ff:ff"],
            &[
                "",
                "00:1a:2b:3c:4d",
                "00-1a-2b-3c-4d-5e",
                "00:1a:2b:3c:4d:5g",
                "0:1a:2b:3c:4d:5e",
            ],
        ),
        // DATA and GREEDYDATA take any text, so no text is outside them.
        ("DATA", &["", "anything at all", "a\nb"], &[]),
        ("GREEDYDATA", &["", "anything at all", "a\nb"], &[]),
        (
            "QUOTEDSTRING",
            &[r#""""#, r#""a b""#, r#""say \"hi\"""#, "'single'", r"'it\'s'"],
            &["", "unquoted", r#""open"#, r#""a" b""#, r#""a'"#, r#""bad \"#],
        ),
        (
            "LOGLEVEL",
            &["info", "WARN", "warning", "Error", "err", "CRITICAL", "emerg", "trace"],
            &["", "information", "warn!", "verbose"],
        ),
        (
            "MONTH",
            &["Jan", "january", "SEP", "September", "may"],
            &["", "Ja", "Janu", "Sept", "13"],
        ),
        ("MONTHNUM", &["1", "01", "09", "10", "12"], &["", "0", "00", "13", "1a"]),
        (
            "MONTHDAY",
            &["1", "01", "9", "10", "29", "31"],
            &["", "0", "00", "32", "40"],
        ),
        ("YEAR", &["26", "2026", "1999"], &["", "2", "202", "20266", "y2k"]),
        ("HOUR", &["0", "00", "9", "19", "23"], &["", "24", "29", "123"]),
        ("MINUTE", &["00", "07", "59"], &["", "7", "60", "123"]),
        (
            "SECOND",
            &["0", "07", "59", "60", "59.123", "07,5"],
            &["", "61", "99", "1.", "123"],
        ),
        (
            "TIME",
            &["0:00", "23:59", "12:34:56", "12:34:56.789"],
            &["", "24:00", "12:60", "12:34:61", "12", "12:3"],
        ),
        (
            "TIMESTAMP_ISO8601",
            &[
                "2026-10-03T01:58:18",
                "2026-10-03 01:58:18.123",
                "2026-10-03T01:58:18Z",
                "2026-10-03T01:58:18+05:30",
                "2026-10-03T01:58:18-0800",
            ],
            &[
                "",
                "2026-10-03",
                "2026-13-03T01:58:18",
                "2026-10-03X01:58:18",
                "2026-10-03T01:58:18+5",
            ],
        ),
        (
            "HTTPDATE",
            &["03/Oct/2026:01:58:18 +0000", "3/oct/2026:01:58:18 -0700"],
            &[
                "",
                "03/Oct/2026 01:58:18 +0000",
                "03/Oct/2026:01:58:18",
                "32/Oct/2026:01:58:18 +0000",
            ],
        ),
        (
            "SYSLOGTIMESTAMP",
            &["Oct  3 01:58:18", "Oct 03 01:58:18", "oct 3 1:58"],
            &["", "Oct 3", "Oct 32 01:58:18", "3 Oct 01:58:18"],
        ),
        (
            "SYSLOGHOST",
            &["web-01", "10.1.2.3", "::1"],
            &["", "web 01", "web_01", "-web"],
        ),
        (
            "URIPROTO",
            &["http", "https", "svn+ssh", "a.b-c"],
            &["", "1http", "+http", "ht tp"],
        ),
        (
            "URIHOST",
            &["example.com", "example.com:8080", "10.1.2.3", "10.1.2.3:443"],
            &["", "example.com:", "example.com:0", "example.com:port", "a b"],
        ),
        (
            "URIPATH",
            &["", "/", "/a/b.html", "/a%20b/~c;d=e"],
            &["a b", "/a?b", "/a\"b", "/a<b>"],
        ),
        (
            "URIPATHPARAM",
            &["/", "/a/b?c=d&e=f", "/search?q=a+b", "?x"],
            &["/a b", "/a?b c", "/a\"b"],
        ),
        (
            "SYSLOGLINE",
            &[
                "<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed",
                "Oct  1 06:25:24 web-01 kernel: link up",
                "2026-10-11T22:14:15Z web_01 no tag here",
            ],
            &["", "Oct 11 22:14:15", "<34> Oct 11 22:14:15 h m", "hello world"],
        ),
        (
            "SYSLOG5424LINE",
            &[
                "<34>1 - - - - - -",
                r#"<165>1 2003-10-11T22:14:15.003Z host app 12 ID47 [a@1 b="c"] msg"#,
            ],
            &[
                "",
                "<34>Oct 11 22:14:15 host su: x",
                "<34>1 - - - - -",
                "34>1 - - - - - -",
            ],
        ),
        (
            "COMMONAPACHELOG",
            &[
                r#"127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326"#,
                r#"::1 - - [10/Oct/2000:13:55:36 -0700] "-" 408 -"#,
                r#"10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "M-SEARCH * HTTP/1.1" 200 0"#,
            ],
            &["", r#"127.0.0.1 - - [10/Oct/2000] "GET /" 200 1"#, "127.0.0.1 GET /"],
        ),
        (
            "COMBINEDAPACHELOG",
            &[r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 304 - "-" "curl/8.0""#],
            &[
                "",
                r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" 200 1"#,
            ],
        ),
    ];

    #[test]
    fn every_builtin_takes_its_cases_whole_and_no_others() {
        for (name, _) in BUILTINS {
            assert!(CASES.iter().any(|(n, ..)| n == name), "{name} has no cases");
        }
        for (name, takes, refuses) in CASES {
            let m = matcher(&format!("^%{{{name}:v}}$"));
            for text in *takes {
                assert_eq!(
                    m.match_row(text).and_then(|r| r.name("v")),
                    Some(*text),
                    "{name} takes {text:?}"
                );
            }
            for text in *refuses {
                assert!(m.match_row(text).is_none(), "{name} refuses {text:?}");
            }
        }
    }

    /// The stdlib IP functions read an address as grok's `IP` does, so `isIp` holds on every capture.
    #[test]
    fn the_ip_cases_are_what_the_stdlib_reads_as_addresses() {
        for (name, takes, refuses) in CASES.iter().filter(|(n, ..)| matches!(*n, "IPV4" | "IPV6" | "IP")) {
            for text in *takes {
                assert!(
                    crate::ops::netops::parse_ip(text.as_bytes()).is_some(),
                    "{name}: the stdlib reads {text:?}"
                );
            }
            for text in *refuses {
                assert!(
                    crate::ops::netops::parse_ip(text.as_bytes()).is_none(),
                    "{name}: the stdlib refuses {text:?}"
                );
            }
        }
    }

    #[test]
    fn an_ipv4_search_does_not_start_or_end_inside_a_number() {
        let m = matcher("%{IP:ip} -");
        for text in ["01.2.3.4 - x", "256.1.1.1 - x", "1.2.3.256 - x"] {
            assert!(m.match_row(text).is_none(), "{text:?}");
        }
        assert_eq!(m.match_row("ip=1.2.3.4 - x").unwrap().name("ip"), Some("1.2.3.4"));
    }

    #[test]
    fn an_ipv6_zone_ends_at_a_space() {
        let m = matcher("%{IPV6:ip} %{WORD:w}");
        let row = m.match_row("fe80::1%eth0 next").unwrap();
        assert_eq!(row.name("ip"), Some("fe80::1%eth0"));
        assert_eq!(row.name("w"), Some("next"));
    }

    #[test]
    fn a_field_name_whose_segments_are_not_identifiers_is_an_error() {
        for name in [".lead", "trail.", "a..b", "0a", "a-b", "a b", "a[0]", "\u{fc}.x", ""] {
            let pattern = format!("%{{WORD:{name}}}");
            assert!(
                expand(&pattern, &cfg(&[])).is_err(),
                "`{name}` should not be a field name"
            );
        }
    }

    /// `>` ends a named group, so an unchecked `a>b` would compile as the group `a` followed by `b>` -- pattern text
    /// the author never wrote.
    #[test]
    fn a_field_name_cannot_close_its_own_capture_group() {
        assert!(expand("%{WORD:a>b}", &cfg(&[])).is_err());
    }

    #[test]
    fn self_referencing_custom_pattern_is_a_bounded_error_not_a_hang() {
        let out = expand("%{LOOP}", &cfg(&[("LOOP", "%{LOOP}")]));
        assert!(out.is_err());
    }

    #[test]
    fn mixed_literal_and_macro_text_composes() {
        let m = matcher(r"\[%{WORD:level}\] %{GREEDYDATA:msg}");
        let got = m.match_row("[warn] disk almost full").unwrap();
        assert_eq!(got.name("level"), Some("warn"));
        assert_eq!(got.name("msg"), Some("disk almost full"));
    }
}

/// The GUI's grok autocomplete and Grok patterns page (`gui-frontend/src/grokPatterns.ts`) carry a hand-written copy
/// of `BUILTINS`, with a use and an example for each and the definition last; this keeps its names and definitions
/// equal to the engine's.
#[cfg(test)]
mod gui_grok_sync {
    #[test]
    fn the_gui_names_every_builtin() {
        let source = include_str!("../../gui-frontend/src/grokPatterns.ts");
        let re = regex::Regex::new(r#"name:\s*'([A-Z0-9_]+)'"#).unwrap();
        let gui: std::collections::BTreeSet<&str> =
            re.captures_iter(source).map(|c| c.get(1).unwrap().as_str()).collect();
        let engine: std::collections::BTreeSet<&str> = super::BUILTINS.iter().map(|(n, _)| *n).collect();
        assert_eq!(gui, engine, "grokPatterns.ts's names differ from BUILTINS");
    }

    /// Each definition is quoted as a TypeScript string in either quote writes it.
    #[test]
    fn the_gui_quotes_every_builtin_definition() {
        let source = include_str!("../../gui-frontend/src/grokPatterns.ts");
        for (name, def) in super::BUILTINS {
            let escaped = def.replace('\\', r"\\");
            let in_quote = |q: char| format!("Regex: `{}`{q},", escaped.replace(q, &format!("\\{q}")));
            assert!(
                source.contains(&in_quote('\'')) || source.contains(&in_quote('"')),
                "grokPatterns.ts does not quote {name}'s definition as BUILTINS has it"
            );
        }
    }
}
