//! `unpack(bytes, layout, options)`: a binary layout read in one call, written as a function of `readInt`,
//! `readFloat`, `readBytes` and `decode` that `expr::inline` expands where it is called, as it expands a pipeline's
//! own.
//!
//! The layout is an object literal of fields in the order their bytes come. A field is a number type, `"u8"` to
//! `"i64"`, `"f32"` or `"f64"`; `"bytes"` or `"str"`, which run to the end of the message; or an object naming its
//! `type` and what the type takes: a number's `order`, and a `bytes` or `str` field's `length` (a count, or the name
//! of an integer field before it), its `prefix` (the integer type its length is written in ahead of it) with that
//! prefix's `order`, and a `str` field's `charset`. The options name the byte order of every number that names none,
//! big when they do not.
//!
//! Each field reads where the one before it ended. A field the bytes are too short for is null with an
//! `IndexOutOfRange` on its row, and every field after it is null.

use crate::expr::ast::{Expr, ExprKind, Function, Span, Stmt, Value};
use crate::expr::parse;
use crate::format::encoding::Encoding;
use crate::ops::byteops::{NumType, Order};

/// The function's name.
pub const UNPACK: &str = "unpack";

/// What a call is, for an error that names its shape.
const SHAPE: &str = "`unpack` is called as `unpack(bytes, {field: type, ...}, {order})`, the options optional";

/// How one field's bytes are read.
enum Read {
    Num(NumType, Order),
    /// `bytes`, or text in `charset`.
    Data {
        charset: Option<String>,
        length: Length,
    },
}

/// Where a `bytes` or `str` field ends.
enum Length {
    Count(i64),
    /// The value of the field at this position.
    Field(usize),
    Prefix(NumType, Order),
    Rest,
}

/// The function a call of `unpack` with these arguments expands to, taking the bytes. `Err` says which argument is
/// wrong.
pub fn function(args: &[Expr], span: Span) -> Result<Function, String> {
    let (layout, options) = match args {
        [_, layout] => (layout, None),
        [_, layout, options] => (layout, Some(options)),
        _ => return Err(SHAPE.to_string()),
    };
    let order = match options.map(|o| &o.kind) {
        None => Order::Big,
        Some(ExprKind::Object(entries)) => {
            let mut order = Order::Big;
            for (key, value) in entries {
                match key.as_deref() {
                    Some("order") => order = byte_order(value)?,
                    Some(k) => return Err(format!("`unpack` takes no option `{k}`; its options are {{order}}")),
                    None => return Err(SHAPE.to_string()),
                }
            }
            order
        }
        Some(_) => return Err(SHAPE.to_string()),
    };
    let ExprKind::Object(entries) = &layout.kind else {
        return Err("`unpack`'s layout is an object of its fields, `{name: type, ...}`, in the order they come".into());
    };
    if entries.is_empty() {
        return Err("`unpack`'s layout names no field".into());
    }
    let mut names: Vec<&str> = Vec::new();
    let mut reads = Vec::new();
    for (key, value) in entries {
        let Some(name) = key.as_deref() else {
            return Err("`unpack`'s layout names each field; it takes no spread".into());
        };
        if names.contains(&name) {
            return Err(format!("`unpack`'s layout names `{name}` twice"));
        }
        if let Some(Read::Data {
            length: Length::Rest, ..
        }) = reads.last()
        {
            let before = names.last().expect("a field before");
            return Err(format!(
                "`{before}` runs to the end of the message, so no field can follow it"
            ));
        }
        reads.push(field(name, value, order, &names, &reads)?);
        names.push(name);
    }
    let text = source(&names, &reads);
    let mut stmts = parse::parse_block(&text).unwrap_or_else(|e| panic!("unpack's function parses: {}", e.message));
    let Some(Stmt::Function { mut function, .. }) = stmts.pop() else {
        unreachable!("the text is one function")
    };
    for stmt in &mut function.body {
        at_span(stmt, span);
    }
    Ok(function)
}

/// The read of the field `name` the layout gives as `value`, after the fields `names` with their `reads`.
fn field(name: &str, value: &Expr, order: Order, names: &[&str], reads: &[Read]) -> Result<Read, String> {
    const TYPES: &str = "a number type, `\"u8\"` to `\"u64\"`, `\"i8\"` to `\"i64\"`, `\"f32\"` or `\"f64\"`, or \
                         `\"bytes\"` or `\"str\"`";
    let (ty, options) = match &value.kind {
        ExprKind::Literal(Value::Str(ty)) => (ty.as_str(), &[][..]),
        ExprKind::Object(entries) => {
            let ty = entries.iter().find(|(k, _)| k.as_deref() == Some("type"));
            match ty.map(|(_, t)| &t.kind) {
                Some(ExprKind::Literal(Value::Str(ty))) => (ty.as_str(), entries.as_slice()),
                _ => return Err(format!("`{name}`'s `type` is {TYPES}")),
            }
        }
        _ => return Err(format!("`{name}` is {TYPES}, or an object naming its `type`")),
    };
    let option = |key: &str| options.iter().find(|(k, _)| k.as_deref() == Some(key)).map(|(_, v)| v);
    if option("length").is_some() && option("prefix").is_some() {
        return Err(format!("`{name}` takes a `length` or a `prefix`, not both"));
    }
    // A `bytes` or `str` field's `order` is its prefix's.
    let allowed: &[&str] = match ty {
        "bytes" if option("prefix").is_some() => &["type", "prefix", "order"],
        "bytes" => &["type", "length", "prefix"],
        "str" if option("prefix").is_some() => &["type", "prefix", "order", "charset"],
        "str" => &["type", "length", "prefix", "charset"],
        _ => &["type", "order"],
    };
    if let Some(k) = options
        .iter()
        .find_map(|(k, _)| k.as_deref().filter(|k| !allowed.contains(k)))
    {
        return Err(match k {
            "order" => format!("`{name}` has no `prefix` for an `order` to read"),
            _ => format!("`{name}`, a `{ty}`, takes no `{k}`"),
        });
    }
    let order = option("order").map_or(Ok(order), byte_order)?;
    if let Some(n) = NumType::of(ty) {
        return Ok(Read::Num(n, order));
    }
    if ty != "bytes" && ty != "str" {
        return Err(format!("`{name}`'s type is {TYPES}"));
    }
    let length = match (option("length"), option("prefix")) {
        (Some(_), Some(_)) => unreachable!("refused above"),
        (None, None) => Length::Rest,
        (Some(l), None) => match &l.kind {
            ExprKind::Literal(Value::I64(n)) if *n >= 0 => Length::Count(*n),
            ExprKind::Literal(Value::Str(f)) => {
                let at = names
                    .iter()
                    .position(|n| n == f)
                    .ok_or_else(|| format!("`{name}`'s `length` names `{f}`, which is not a field before it"))?;
                match reads[at] {
                    Read::Num(t, _) if !t.is_float() => Length::Field(at),
                    _ => return Err(format!("`{name}`'s `length` names `{f}`, which is not an integer")),
                }
            }
            _ => {
                return Err(format!(
                    "`{name}`'s `length` is a count of bytes, or the name of an integer field before it"
                ));
            }
        },
        (None, Some(p)) => match &p.kind {
            ExprKind::Literal(Value::Str(t)) if NumType::of(t).is_some_and(|t| !t.is_float()) => {
                Length::Prefix(NumType::of(t).expect("an integer type"), order)
            }
            _ => return Err(format!("`{name}`'s `prefix` is an integer type, `\"u8\"` to `\"i64\"`")),
        },
    };
    let charset = match (ty, option("charset").map(|c| &c.kind)) {
        ("bytes", _) => None,
        (_, None) => Some("utf-8".to_string()),
        (_, Some(ExprKind::Literal(Value::Str(c)))) if matches!(Encoding::for_label(c), Some(e) if e != Encoding::None) => {
            Some(c.clone())
        }
        (_, Some(_)) => {
            return Err(format!(
                "`{name}`'s `charset` is an encoding's name, such as \"utf-8\" or \"latin1\""
            ));
        }
    };
    Ok(Read::Data { charset, length })
}

fn byte_order(e: &Expr) -> Result<Order, String> {
    match &e.kind {
        ExprKind::Literal(Value::Str(o)) => Order::of(o),
        _ => None,
    }
    .ok_or_else(|| "`unpack`'s `order` is \"big\" or \"little\"".to_string())
}

/// The function's text: each field a local `f<i>`, read at `at`, which then moves past it. A field the message is
/// too short for leaves `at` null, so every field after it is null without an exception of its own. A number that
/// reads as no value, a NaN or a `u64` past `i64`, leaves `at` where the next field starts.
fn source(names: &[&str], reads: &[Read]) -> String {
    let order = |o: &Order| match o {
        Order::Big => "big",
        Order::Little => "little",
    };
    let num = |t: &NumType, o: &Order| {
        let f = if t.is_float() { "readFloat" } else { "readInt" };
        format!("{f}(b, at, \"{}\", \"{}\")", t.name(), order(o))
    };
    let mut lines = vec!["function unpack(b: bytes) {".to_string(), "let at = 0;".to_string()];
    for (i, read) in reads.iter().enumerate() {
        match read {
            Read::Num(t, o) => {
                lines.push(format!("let f{i} = {};", num(t, o)));
                lines.push(advance(&t.width().to_string()));
            }
            Read::Data { charset, length } => {
                let n = match length {
                    Length::Count(n) => n.to_string(),
                    Length::Field(f) => format!("f{f}"),
                    Length::Prefix(t, o) => {
                        lines.push(format!("let p{i} = {};", num(t, o)));
                        lines.push(advance(&t.width().to_string()));
                        format!("p{i}")
                    }
                    Length::Rest => "length(b) - at".to_string(),
                };
                lines.push(format!("let n{i} = {n};"));
                let bytes = format!("readBytes(b, at, n{i})");
                let value = match charset {
                    None => bytes,
                    Some(c) => format!("decode({bytes}, {})", literal(c)),
                };
                lines.push(format!("let f{i} = {value};"));
                lines.push(advance(&format!("n{i}")));
            }
        }
    }
    let fields: Vec<String> = names
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{}: f{i}", literal(n)))
        .collect();
    lines.push(format!("return {{{}}};", fields.join(", ")));
    lines.push("}".to_string());
    lines.join("\n")
}

/// The statement that moves `at` past `n` bytes, or makes it null where the message does not hold them.
fn advance(n: &str) -> String {
    format!("at = {n} >= 0 && at + {n} <= length(b) ? at + {n} : null;")
}

/// `s` as a string literal of the language, which spells every control character as a `\u{..}` escape.
fn literal(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' | '\\' => out.extend(['\\', c]),
            c if c.is_control() => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `stmt` with every span `span`: the call's, which an error in what it expands to points at.
fn at_span(stmt: &mut Stmt, span: Span) {
    fn expr(e: &mut Expr, span: Span) {
        e.span = span;
        for c in e.children_mut() {
            expr(c, span);
        }
    }
    for e in stmt.exprs_mut() {
        expr(e, span);
    }
    for b in stmt.blocks_mut() {
        for s in b {
            at_span(s, span);
        }
    }
}
