//! The parse functions `json`, `kv`, `csv`, `grok` and `lookup` where a statement writes them:
//! `Object.assign($, f(…))`, `$ = f(…)`, `x = f(…)` and `x: T = f(…)`. Each takes its text, then what its kernel
//! needs, then an options literal for what has a default:
//!
//! ```text
//! json(text, {splitDots: true, formats: {ts: "%d/%m/%Y"}})
//! kv(text, {pairSep: " ", kvSep: "=", quote: "\""})
//! csv(text, ["a", "b"], {delimiter: ","})
//! grok(text, ["%{INT:status}"], {patterns: {NAME: "regex"}})
//! lookup("geo", key, {match: "cidr", column: "net", first: true, fields: {city: "city_name", country: ["country", "?"]}})
//! ```
//!
//! A lookup names its table before its key, and its options name each output with its column, or its column and its
//! default. Every parser takes `splitDots`, whether a key it reads is split at its dots into a path. Every function
//! takes `formats`, which names the text format a `datetime` field is read in; without it the
//! field is read in any format the parser knows. Everything but the text is read at compile time, so it is written as
//! literals.

use std::collections::HashMap;
use std::sync::Arc;

use crate::expr::ast::{Expr, ExprKind, Value};
use crate::format::grok_scan::Matcher;
use crate::plan::config::{CsvConfig, GrokConfig, GrokPatterns, KvConfig, MatchKind, Multiplicity, ParserKind};
use crate::plan::tree;

/// The parse functions, each a call `ParseCall::of` reads.
pub const PARSE_FNS: [&str; 6] = ["json", "kv", "cef", "csv", "grok", "lookup"];

/// The options each parse function's options literal may name.
pub const OPTIONS: [(&str, &[&str]); 6] = [
    ("json", &["splitDots", "formats"]),
    (
        "kv",
        &[
            "pairSep",
            "kvSep",
            "quote",
            "escapes",
            "nextKey",
            "splitDots",
            "formats",
        ],
    ),
    ("cef", &["splitDots", "formats"]),
    (
        "csv",
        &[
            "delimiter",
            "quote",
            "escape",
            "squeeze",
            "trim",
            "splitDots",
            "formats",
        ],
    ),
    ("grok", &["patterns", "splitDots", "formats"]),
    ("lookup", &["match", "column", "first", "all", "fields", "formats"]),
];

fn known_options(name: &str) -> &'static [&'static str] {
    OPTIONS.iter().find(|(n, _)| *n == name).map_or(&[], |(_, known)| known)
}

/// Whether the parse function `name` splits a key at its dots when its call does not say: only where dots are the
/// format's way to nest (kv), or the key is written in code (a grok capture).
pub fn split_dots_default(name: &str) -> bool {
    matches!(name, "kv" | "grok")
}

/// A call of a parse function.
pub struct ParseCall<'e> {
    pub name: &'e str,
    /// The text it parses; a lookup's key.
    pub input: &'e Expr,
    /// A lookup's table.
    table: Option<&'e Expr>,
    /// What follows the text.
    args: &'e [Expr],
}

/// What a `lookup` call reads, and how.
pub struct LookupArgs {
    pub table: String,
    pub match_kind: MatchKind,
    pub multiplicity: Multiplicity,
    pub key_column: String,
    /// Each output, its column and its default.
    pub fields: Vec<(String, String, Option<String>)>,
}

impl<'e> ParseCall<'e> {
    /// `e` as a parse function's call, where it is one.
    pub fn of(e: &'e Expr) -> Option<ParseCall<'e>> {
        let ExprKind::Call { name, args } = &e.kind else {
            return None;
        };
        let name = PARSE_FNS.into_iter().find(|n| n == name)?;
        let (table, args) = match name {
            "lookup" => args.split_first().map(|(t, rest)| (Some(t), rest))?,
            _ => (None, args.as_slice()),
        };
        let (input, args) = args.split_first()?;
        Some(ParseCall {
            name,
            input,
            table,
            args,
        })
    }

    /// A spread of `input`, a variant: its keys on each row are the object it gives.
    pub fn variant(input: &'e Expr) -> ParseCall<'e> {
        ParseCall {
            name: "spread",
            input,
            table: None,
            args: &[],
        }
    }

    /// The table and the options of a `lookup` call. `Err` says which argument is wrong.
    pub fn lookup(&self) -> Result<LookupArgs, String> {
        const SHAPE: &str = "`lookup` is called as `lookup(table, key, {match, column, first, all, fields, formats})`";
        let (Some(table), [literal]) = (self.table, self.args) else {
            return Err(SHAPE.to_string());
        };
        let ExprKind::Literal(Value::Str(table)) = &table.kind else {
            return Err("`lookup`'s table is its id, written as a string".to_string());
        };
        let options = options(self.name, Some(literal), known_options(self.name))?;
        let get = |key: &str| options.get(key).map(|e| &e.kind);
        let flag = |key: &str| match get(key) {
            None | Some(ExprKind::Literal(Value::Bool(false))) => Ok(false),
            Some(ExprKind::Literal(Value::Bool(true))) => Ok(true),
            _ => Err(format!("`lookup`'s `{key}` is `true` or `false`")),
        };
        let multiplicity = match (flag("first")?, flag("all")?) {
            (true, true) => return Err("`lookup` takes `first` or `all`, not both".to_string()),
            (true, false) => Multiplicity::First,
            (false, true) => Multiplicity::All,
            (false, false) => Multiplicity::Best,
        };
        let match_kind = match get("match") {
            None => MatchKind::Exact,
            Some(ExprKind::Literal(Value::Str(m))) if m == "exact" => MatchKind::Exact,
            Some(ExprKind::Literal(Value::Str(m))) if m == "cidr" => MatchKind::Cidr,
            Some(ExprKind::Literal(Value::Str(m))) if m == "regex" => MatchKind::Regex,
            Some(_) => return Err("`lookup`'s `match` is \"exact\", \"cidr\" or \"regex\"".to_string()),
        };
        let key_column = match get("column") {
            None => String::new(),
            Some(ExprKind::Literal(Value::Str(c))) => c.clone(),
            Some(_) => return Err("`lookup`'s `column` is the key column's name, written as a string".to_string()),
        };
        const FIELDS: &str = "`lookup`'s `fields` maps each output to its column, `\"column\"`, or to its column and its \
                              default, `[\"column\", \"default\"]`";
        let Some(ExprKind::Object(entries)) = get("fields") else {
            return Err(FIELDS.to_string());
        };
        let text = |e: &Expr| match &e.kind {
            ExprKind::Literal(Value::Str(s)) => Some(s.clone()),
            _ => None,
        };
        let fields = entries
            .iter()
            .map(|(out, e)| {
                let out = out.clone().ok_or(FIELDS)?;
                match &e.kind {
                    ExprKind::Literal(Value::Str(c)) => Ok((out, c.clone(), None)),
                    ExprKind::Array(pair) => match pair.as_slice() {
                        [c, d] => match (text(c), &d.kind) {
                            (Some(c), ExprKind::Literal(Value::Null)) => Ok((out, c, None)),
                            (Some(c), _) => Ok((out, c, Some(text(d).ok_or(FIELDS)?))),
                            _ => Err(FIELDS),
                        },
                        _ => Err(FIELDS),
                    },
                    _ => Err(FIELDS),
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(str::to_string)?;
        if fields.is_empty() {
            return Err(FIELDS.to_string());
        }
        Ok(LookupArgs {
            table: table.clone(),
            match_kind,
            multiplicity,
            key_column,
            fields,
        })
    }

    /// The parser this call runs, and its compiled grok matcher, which may name the bundle's `grok_patterns`. `Err`
    /// says which argument is wrong.
    pub fn kind(&self, grok_patterns: &GrokPatterns) -> Result<(ParserKind, Option<Arc<Matcher>>), String> {
        let name = self.name;
        if name == "spread" {
            return Ok((ParserKind::Variant, None));
        }
        let required = match name {
            "json" | "kv" | "cef" => 0,
            "csv" | "grok" => 1,
            _ => unreachable!("`lookup` reads its arguments in `ParseCall::lookup`"),
        };
        if self.args.len() < required || self.args.len() > required + 1 {
            let shape = match name {
                "json" => "json(text, {splitDots, formats})",
                "cef" => "cef(text, {splitDots, formats})",
                "kv" => "kv(text, {pairSep, kvSep, quote, escapes, nextKey, splitDots, formats})",
                "csv" => "csv(text, [headers], {delimiter, quote, escape, squeeze, trim, splitDots, formats})",
                _ => "grok(text, [patterns], {patterns, splitDots, formats})",
            };
            return Err(format!("`{name}` is called as `{shape}`, the options optional"));
        }
        let options = options(name, self.args.get(required), known_options(name))?;
        let byte = |key: &str, default: u8| -> Result<u8, String> {
            match options.get(key) {
                None => Ok(default),
                Some(e) => match &e.kind {
                    ExprKind::Literal(Value::Str(s)) if s.len() == 1 => Ok(s.as_bytes()[0]),
                    _ => Err(format!("`{name}`'s `{key}` is one character, written as a string")),
                },
            }
        };
        let flag = |key: &str, default: bool| match options.get(key).map(|e| &e.kind) {
            None => Ok(default),
            Some(ExprKind::Literal(Value::Bool(b))) => Ok(*b),
            Some(_) => Err(format!("`{name}`'s `{key}` is `true` or `false`")),
        };
        // `null` turns the byte off.
        let maybe_byte = |key: &str, default: Option<u8>| match options.get(key).map(|e| &e.kind) {
            Some(ExprKind::Literal(Value::Null)) => Ok(None),
            Some(_) => byte(key, 0).map(Some),
            None => Ok(default),
        };
        let split_dots = flag("splitDots", split_dots_default(name))?;
        Ok(match name {
            "json" => (ParserKind::Json { split_dots }, None),
            "cef" => (ParserKind::Cef { split_dots }, None),
            "kv" => {
                let d = KvConfig::default();
                let config = KvConfig {
                    split_dots,
                    pair_sep: byte("pairSep", d.pair_sep)?,
                    kv_sep: byte("kvSep", d.kv_sep)?,
                    quote: maybe_byte("quote", d.quote)?,
                    escapes: flag("escapes", d.escapes)?,
                    next_key: flag("nextKey", d.next_key)?,
                };
                (ParserKind::KeyValue(config), None)
            }
            "csv" => {
                let ExprKind::Array(items) = &self.args[0].kind else {
                    return Err(HEADERS.to_string());
                };
                let headers = items
                    .iter()
                    .map(|i| match &i.kind {
                        ExprKind::Literal(Value::Str(s)) => Ok(Some(s.clone())),
                        ExprKind::Literal(Value::Null) => Ok(None),
                        _ => Err(HEADERS.to_string()),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let named: Vec<&String> = headers.iter().flatten().collect();
                if let Some(h) = named.iter().find(|h| named.iter().filter(|o| o == h).count() > 1) {
                    return Err(format!(
                        "csv: more than one column is named `{h}`, so which one the field takes its value from is \
                         whichever comes last. Give each column its own name."
                    ));
                }
                let d = CsvConfig::default();
                let quote = byte("quote", d.quote)?;
                let config = CsvConfig {
                    split_dots,
                    delimiter: byte("delimiter", d.delimiter)?,
                    headers,
                    quote,
                    // An escape that is the quote is a doubled quote.
                    escape: maybe_byte("escape", d.escape)?.filter(|&e| e != quote),
                    squeeze: flag("squeeze", d.squeeze)?,
                    trim: flag("trim", d.trim)?,
                };
                (ParserKind::Csv(config), None)
            }
            _ => {
                let patterns = strings(name, "patterns", &self.args[0])?;
                let custom_patterns = match options.get("patterns") {
                    None => Vec::new(),
                    Some(e) => match &e.kind {
                        ExprKind::Object(entries) => entries
                            .iter()
                            .map(|(k, v)| match (k, &v.kind) {
                                (Some(k), ExprKind::Literal(Value::Str(p))) => Ok((k.clone(), p.clone())),
                                _ => Err("grok's `patterns` option maps each name to a pattern string".to_string()),
                            })
                            .collect::<Result<_, _>>()?,
                        _ => return Err("grok's `patterns` option is an object of named patterns".to_string()),
                    },
                };
                let config = GrokConfig {
                    split_dots,
                    patterns,
                    custom_patterns,
                };
                let matcher = Matcher::compile_with(&config, grok_patterns).map_err(|e| format!("grok: {e}"))?;
                (ParserKind::Grok(config), Some(Arc::new(matcher)))
            }
        })
    }

    /// The `formats` option: each key, under what the call writes, with the format its text is read in. Read after
    /// `kind` or `lookup`, which check the options around it.
    pub fn formats(&self) -> Result<Vec<(String, String)>, String> {
        const FORMATS: &str = "`formats` maps each `datetime` field to its format, written as a string";
        let literal = match self.name {
            "lookup" | "json" | "kv" | "cef" => self.args.first(),
            _ => self.args.get(1),
        };
        let entry = match literal.map(|e| &e.kind) {
            Some(ExprKind::Object(entries)) => entries.iter().find(|(k, _)| k.as_deref() == Some("formats")),
            _ => None,
        };
        let Some((_, formats)) = entry else {
            return Ok(Vec::new());
        };
        let ExprKind::Object(entries) = &formats.kind else {
            return Err(FORMATS.to_string());
        };
        entries
            .iter()
            .map(|(k, v)| match (k, &v.kind) {
                (Some(k), ExprKind::Literal(Value::Str(f))) => Ok((k.clone(), f.clone())),
                _ => Err(FORMATS.to_string()),
            })
            .collect()
    }
}

/// The keys a parse finds, where its configuration fixes them: a CSV's headers, a grok pattern's captures. `None`
/// where the record names them.
pub fn matcher_keys(kind: &ParserKind, matcher: &Option<Arc<Matcher>>) -> Option<Vec<String>> {
    match kind {
        ParserKind::Csv(c) => Some(
            c.headers
                .iter()
                .flatten()
                .map(|h| tree::name_of(h, c.split_dots).into_owned())
                .collect(),
        ),
        ParserKind::Grok(c) => matcher.as_ref().map(|m| {
            m.capture_names()
                .into_iter()
                .map(|n| tree::name_of(n, c.split_dots).into_owned())
                .collect()
        }),
        ParserKind::Json { .. } | ParserKind::KeyValue(_) | ParserKind::Cef { .. } | ParserKind::Variant => None,
    }
}

/// The options literal `e`, each key one of `known`, by key.
fn options<'e>(name: &str, e: Option<&'e Expr>, known: &[&str]) -> Result<HashMap<&'e str, &'e Expr>, String> {
    let Some(e) = e else {
        return Ok(HashMap::new());
    };
    let ExprKind::Object(entries) = &e.kind else {
        return Err(format!(
            "`{name}`'s options are an object literal, `{{{}}}`",
            known.join(", ")
        ));
    };
    entries
        .iter()
        .map(|(k, v)| match k {
            Some(k) if known.contains(&k.as_str()) => Ok((k.as_str(), v)),
            _ => Err(format!(
                "`{name}` takes the options {}",
                known.iter().map(|k| format!("`{k}`")).collect::<Vec<_>>().join(", ")
            )),
        })
        .collect()
}

/// `csv()`'s headers: each a column's name, or `null` for a column that makes no field.
const HEADERS: &str = "`csv`'s headers are an array of names, `null` for a column that makes no field";

/// The array of string literals `e`.
fn strings(name: &str, what: &str, e: &Expr) -> Result<Vec<String>, String> {
    let ExprKind::Array(items) = &e.kind else {
        return Err(format!("`{name}`'s {what} are an array of strings"));
    };
    items
        .iter()
        .map(|i| match &i.kind {
            ExprKind::Literal(Value::Str(s)) => Ok(s.clone()),
            _ => Err(format!("`{name}`'s {what} are an array of strings")),
        })
        .collect()
}
