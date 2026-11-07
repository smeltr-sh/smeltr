//! `bytes.unpack(layout, options)` and `value.pack(layout, options)`: a binary layout read from bytes in one call, and
//! written back. `expr::inline` expands `unpack` to a function of `readInt`, `readFloat`, `readBytes` and `decode`,
//! and `pack` to one of `writeInt`, `writeFloat`, `encode` and `concat`, as it expands a pipeline's own functions.
//!
//! The layout is an object literal of fields in the order their bytes come, each a type string:
//!
//! - a number type, `"u8"` to `"i64"`, `"f32"` or `"f64"`, and after it its order: `"u16 little"`;
//! - `"bytes"` or `"str"`, which run to the end of the message, or take their length in parentheses: a count,
//!   `"bytes(4)"`; an integer field before them, `"str(len)"`; or the integer type of a prefix the length is written
//!   in ahead of them, with its order, `"str(u16 little)"`. A `str` takes its charset second, `"str(len, latin1)"`,
//!   UTF-8 where it names none.
//!
//! No field is named after a type, so a length that names one is always a prefix. The options name the byte order of
//! every number that names none, big when they do not.
//!
//! `unpack` reads each field where the one before it ended. A field the bytes are too short for is null with an
//! `IndexOutOfRange` on its row, and every field after it is null.
//!
//! `pack` writes each field after the one before it, so `value.pack(layout).unpack(layout)` gives back every value
//! the layout holds. A length field is written as the length of the data it measures, whatever the value holds for
//! it, and a count the data does not match is null with an `IndexOutOfRange`. A number its type cannot hold is null
//! with an `Overflow`. Any of these, or a null field, makes the message null.

use crate::expr::ast::{Expr, ExprKind, Function, Span, Stmt, Value};
use crate::expr::parse;
use crate::format::encoding::Encoding;
use crate::ops::byteops::{NumType, Order};

/// The function that reads a layout.
pub const UNPACK: &str = "unpack";

/// The function that writes one.
pub const PACK: &str = "pack";

/// How one field's bytes are read and written.
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
    /// The value of an earlier integer field, by its index in the layout.
    Field(usize),
    Prefix(NumType, Order),
    Rest,
}

/// A layout's fields, in order.
struct Layout<'a> {
    names: Vec<&'a str>,
    reads: Vec<Read>,
}

/// The function a call of `unpack` with these arguments expands to, taking the bytes. `Err` says which argument is
/// wrong.
pub fn function(args: &[Expr], span: Span) -> Result<Function, String> {
    let layout = layout(UNPACK, args)?;
    expand(&unpack_source(&layout), span)
}

/// The function a call of `pack` with these arguments expands to, taking the value. `Err` says which argument is
/// wrong.
pub fn pack_function(args: &[Expr], span: Span) -> Result<Function, String> {
    let layout = layout(PACK, args)?;
    expand(&pack_source(&layout), span)
}

/// `text`, one function, parsed, every span in it the call's, so an error in the expansion points at the call.
fn expand(text: &str, span: Span) -> Result<Function, String> {
    let mut stmts = parse::parse_block(text).unwrap_or_else(|e| panic!("a layout's function parses: {}", e.message));
    let Some(Stmt::Function { mut function, .. }) = stmts.pop() else {
        unreachable!("the text is one function")
    };
    for stmt in &mut function.body {
        at_span(stmt, span);
    }
    Ok(function)
}

/// The layout a call of `call` gives, after its value, with its options.
fn layout<'a>(call: &str, args: &'a [Expr]) -> Result<Layout<'a>, String> {
    let shape = || {
        let value = if call == UNPACK { "bytes" } else { "value" };
        format!("`{call}` is called as `{value}.{call}({{field: type, ...}}, {{order}})`, the options optional")
    };
    let (layout, options) = match args {
        [_, layout] => (layout, None),
        [_, layout, options] => (layout, Some(options)),
        _ => return Err(shape()),
    };
    let order = match options.map(|o| &o.kind) {
        None => Order::Big,
        Some(ExprKind::Object(entries)) => {
            let mut order = Order::Big;
            for (key, value) in entries {
                match key.as_deref() {
                    Some("order") => {
                        order = match &value.kind {
                            ExprKind::Literal(Value::Str(o)) => Order::of(o),
                            _ => None,
                        }
                        .ok_or_else(|| format!("`{call}`'s `order` is \"big\" or \"little\""))?;
                    }
                    Some(k) => return Err(format!("`{call}` takes no option `{k}`; its options are {{order}}")),
                    None => return Err(shape()),
                }
            }
            order
        }
        Some(_) => return Err(shape()),
    };
    let ExprKind::Object(entries) = &layout.kind else {
        return Err(format!(
            "`{call}`'s layout is an object of its fields, `{{name: type, ...}}`, in the order they come"
        ));
    };
    if entries.is_empty() {
        return Err(format!("`{call}`'s layout names no field"));
    }
    let mut out = Layout {
        names: Vec::new(),
        reads: Vec::new(),
    };
    for (key, value) in entries {
        let Some(name) = key.as_deref() else {
            return Err(format!("`{call}`'s layout names each field; it takes no spread"));
        };
        if out.names.contains(&name) {
            return Err(format!("`{call}`'s layout names `{name}` twice"));
        }
        if is_type(name) {
            return Err(format!("`{name}` is a type's name, and a field cannot take it"));
        }
        if let Some(Read::Data {
            length: Length::Rest, ..
        }) = out.reads.last()
        {
            let before = out.names.last().expect("a field before");
            return Err(format!(
                "`{before}` runs to the end of the message, so no field can follow it"
            ));
        }
        let ExprKind::Literal(Value::Str(ty)) = &value.kind else {
            return Err(format!("`{name}`'s type is a string, such as \"u16\" or \"str(len)\""));
        };
        let read = field(name, ty, order, &out)?;
        out.reads.push(read);
        out.names.push(name);
    }
    Ok(out)
}

/// Whether `name` names a type a layout writes.
fn is_type(name: &str) -> bool {
    NumType::of(name).is_some() || name == "bytes" || name == "str"
}

/// The read of the field `name`, whose type string is `ty`, after the fields of `layout`.
fn field(name: &str, ty: &str, order: Order, layout: &Layout) -> Result<Read, String> {
    const TYPES: &str = "a number type, \"u8\" to \"u64\", \"i8\" to \"i64\", \"f32\" or \"f64\", with its order \
                         after it, or \"bytes\" or \"str\", with its length in parentheses";
    let (head, args) = match ty.split_once('(') {
        None => (ty, None),
        Some((head, rest)) => match rest.trim_end().strip_suffix(')') {
            Some(args) => (head, Some(args)),
            None => return Err(format!("`{name}`'s type has no `)` to close its `(`")),
        },
    };
    let words: Vec<&str> = head.split_whitespace().collect();
    let Some(&base) = words.first() else {
        return Err(format!("`{name}` is {TYPES}"));
    };
    if let Some(n) = NumType::of(base) {
        if args.is_some() {
            return Err(format!("`{name}`, a `{base}`, takes no length"));
        }
        return Ok(Read::Num(n, number_order(name, &words, order)?));
    }
    if base != "bytes" && base != "str" {
        return Err(format!("`{name}` is {TYPES}"));
    }
    if words.len() > 1 {
        return Err(format!(
            "`{name}`, a `{base}`, takes no order; a prefix in its parentheses does"
        ));
    }
    let args: Vec<&str> = args.map_or(Vec::new(), |a| a.split(',').map(str::trim).collect());
    let (length, charset) = match args[..] {
        [] => (Length::Rest, None),
        [length] => (length_of(name, length, order, layout)?, None),
        [length, charset] if base == "str" => (length_of(name, length, order, layout)?, Some(charset)),
        _ if base == "str" => return Err(format!("`{name}` takes its length, then its charset")),
        _ => return Err(format!("`{name}`, a `bytes`, takes only its length")),
    };
    let charset = match (base, charset) {
        ("bytes", _) => None,
        (_, None) => Some("utf-8".to_string()),
        (_, Some(c)) if matches!(Encoding::for_label(c), Some(e) if e != Encoding::None) => Some(c.to_string()),
        (_, Some(_)) => {
            return Err(format!(
                "`{name}`'s charset is an encoding's name, such as utf-8 or latin1"
            ));
        }
    };
    Ok(Read::Data { charset, length })
}

/// The order of the number `name`, written `words`: its type, and after it its order where it names one.
fn number_order(name: &str, words: &[&str], order: Order) -> Result<Order, String> {
    match words {
        [_] => Ok(order),
        [_, o] => Order::of(o).ok_or_else(|| format!("`{name}`'s order is \"big\" or \"little\"")),
        _ => Err(format!("`{name}` is a number type, then its order")),
    }
}

/// Where the data field `name` ends, by what its parentheses hold, `text`.
fn length_of(name: &str, text: &str, order: Order, layout: &Layout) -> Result<Length, String> {
    const LENGTH: &str = "a count of bytes, an integer field before it, or the integer type of its prefix";
    let words: Vec<&str> = text.split_whitespace().collect();
    match words[..] {
        [count] if count.bytes().all(|c| c.is_ascii_digit()) => count
            .parse()
            .map(Length::Count)
            .map_err(|_| format!("`{name}`'s length is too large")),
        [ty, ..] if NumType::of(ty).is_some() => match NumType::of(ty) {
            Some(t) if !t.is_float() => Ok(Length::Prefix(t, number_order(name, &words, order)?)),
            _ => Err(format!("`{name}`'s prefix is an integer type, \"u8\" to \"i64\"")),
        },
        [field] => {
            let at = layout
                .names
                .iter()
                .position(|n| *n == field)
                .ok_or_else(|| format!("`{name}`'s length names `{field}`, which is not a field before it"))?;
            match layout.reads[at] {
                Read::Num(t, _) if !t.is_float() => Ok(Length::Field(at)),
                _ => Err(format!("`{name}`'s length names `{field}`, which is not an integer")),
            }
        }
        _ => Err(format!("`{name}`'s length is {LENGTH}")),
    }
}

/// How a call spells `o`.
fn order_name(o: Order) -> &'static str {
    match o {
        Order::Big => "big",
        Order::Little => "little",
    }
}

/// `unpack`'s text: each field a local `f<i>`, read at `at`, which then moves past it. A field the message is too
/// short for leaves `at` null, so every field after it is null without an exception of its own. A number that reads
/// as no value, a NaN or a `u64` above `i64::MAX`, leaves `at` where the next field starts.
fn unpack_source(layout: &Layout) -> String {
    let num = |t: &NumType, o: &Order| {
        let f = if t.is_float() { "readFloat" } else { "readInt" };
        format!("{f}(b, at, \"{}\", \"{}\")", t.name(), order_name(*o))
    };
    let mut lines = vec!["function unpack(b: bytes) {".to_string(), "let at = 0;".to_string()];
    for (i, read) in layout.reads.iter().enumerate() {
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
    let fields: Vec<String> = layout
        .names
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{}: f{i}", literal(n)))
        .collect();
    lines.push(format!("return {{{}}};", fields.join(", ")));
    lines.push("}".to_string());
    lines.join("\n")
}

/// `pack`'s text: each data field's bytes a local `d<i>`, then every field's bytes joined in order. A length field
/// is written from the first data field it measures, and a later one it measures must match it, as a count must.
fn pack_source(layout: &Layout) -> String {
    let value = |i: usize| format!("v[{}]", literal(layout.names[i]));
    let num = |t: &NumType, o: &Order, n: &str| {
        let f = if t.is_float() { "writeFloat" } else { "writeInt" };
        format!("{f}({n}, \"{}\", \"{}\")", t.name(), order_name(*o))
    };
    // `e` exactly `n` bytes long, or null with an `IndexOutOfRange`: a read from -1 is out of range.
    let exactly = |e: &str, n: &str| format!("readBytes({e}, length({e}) == {n} ? 0 : -1, {n})");
    let mut lines = vec!["function pack(v) {".to_string()];
    // The data field each length field is written from.
    let mut measured: Vec<Option<usize>> = vec![None; layout.reads.len()];
    for (i, read) in layout.reads.iter().enumerate() {
        let Read::Data { charset, length } = read else {
            continue;
        };
        let data = match charset {
            None => value(i),
            Some(c) => format!("encode({}, {})", value(i), literal(c)),
        };
        lines.push(format!("let e{i} = {data};"));
        let d = match length {
            Length::Count(n) => exactly(&format!("e{i}"), &n.to_string()),
            Length::Field(f) => match measured[*f] {
                None => {
                    measured[*f] = Some(i);
                    format!("e{i}")
                }
                Some(first) => exactly(&format!("e{i}"), &format!("length(d{first})")),
            },
            Length::Prefix(..) | Length::Rest => format!("e{i}"),
        };
        lines.push(format!("let d{i} = {d};"));
    }
    let mut parts = Vec::new();
    for (i, read) in layout.reads.iter().enumerate() {
        match read {
            Read::Num(t, o) => parts.push(match measured[i] {
                Some(d) => num(t, o, &format!("length(d{d})")),
                None => num(t, o, &value(i)),
            }),
            Read::Data { length, .. } => {
                if let Length::Prefix(t, o) = length {
                    parts.push(num(t, o, &format!("length(d{i})")));
                }
                parts.push(format!("d{i}"));
            }
        }
    }
    let joined = match &parts[..] {
        [one] => one.clone(),
        _ => format!("concat({})", parts.join(", ")),
    };
    lines.push(format!("return {joined};"));
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

/// Sets every span in `stmt` to `span`, the call's, so an error in the expansion points at the call.
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
