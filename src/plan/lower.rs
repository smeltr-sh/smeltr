//! A step lowered to the code it stands for (Compiler). The compiler lowers each step as it reaches it and
//! compiles the statements under the step's guard. A parser's or a lookup's fields are declared only where the name
//! does not already hold their type, so two filtered parsers may name one field (`Held`).

use crate::core::column::DataType;
use crate::expr::ast::{Expr, ExprKind, Span, Stmt, Value};
use crate::expr::parse::{declared, move_stmts};
use crate::plan::config::{DeclaredField, FunctionKind, LookupOutput, MatchKind, Multiplicity, ParserKind, TimeSource};
use crate::plan::tree::{self, ROOT};

/// What the tree holds at a name when a step that declares it is reached.
pub enum Held {
    /// Bound, at this type.
    Bound(DataType),
    /// Not bound, but a key an open object may hold.
    Key,
    /// Nothing.
    Free,
}

/// A step's statements: `body` runs under the step's filter, `after` after it, under the guard around the step.
#[derive(Default)]
pub struct Lowered {
    pub body: Vec<Stmt>,
    pub after: Vec<Stmt>,
}

impl From<Vec<Stmt>> for Lowered {
    /// A step with nothing after it.
    fn from(body: Vec<Stmt>) -> Self {
        Lowered {
            body,
            after: Vec::new(),
        }
    }
}

/// `kind` with each field of the step that holds a name read into its spelling (`tree::parse_name`), so a key that
/// is not plain is the one key it spells. A blank field stays blank, for the step's own error. `Err` names the field
/// that does not read as a name.
pub fn names(kind: &FunctionKind) -> Result<FunctionKind, String> {
    let name = |text: &str| match text.trim() {
        "" => Ok(text.to_string()),
        _ => tree::parse_name(text),
    };
    let mut kind = kind.clone();
    match &mut kind {
        FunctionKind::Eval { assignments } => {
            for (target, _) in assignments {
                *target = name(target)?;
            }
        }
        FunctionKind::Rename { pairs } => {
            for (from, to) in pairs {
                (*from, *to) = (name(from)?, name(to)?);
            }
        }
        FunctionKind::Parser {
            input,
            into,
            kind,
            declared,
        } => {
            *input = name(input)?;
            // `$` names the record, as empty does.
            *into = match into.trim() {
                "" | ROOT => String::new(),
                _ => name(into)?,
            };
            for d in declared {
                // A csv column is a header, the key the parser reads: its name is that key's.
                d.name = match kind {
                    ParserKind::Csv(c) => tree::name_of(&d.name, c.split_dots).into_owned(),
                    _ => name(&d.name)?,
                };
            }
        }
        FunctionKind::Lookup { input, outputs, .. } => {
            *input = name(input)?;
            for o in outputs {
                o.field = name(&o.field)?;
            }
        }
        FunctionKind::Unroll { field, .. } => *field = name(field)?,
        FunctionKind::Comment { .. } | FunctionKind::Code { .. } | FunctionKind::Drop { .. } => {}
    }
    Ok(kind)
}

/// `move(from, to)` for each pair that names two places.
pub fn rename(pairs: &[(String, String)]) -> Vec<Stmt> {
    pairs
        .iter()
        .filter(|(from, to)| from != to)
        .enumerate()
        .flat_map(|(n, (from, to))| move_stmts(from, to, n + 1, Span::default()))
        .collect()
}

/// `if (cond) drop()`. A blank condition drops nothing.
pub fn drop(cond: Option<Expr>) -> Vec<Stmt> {
    let span = Span::default();
    cond.map(|cond| Stmt::If {
        cond,
        then: vec![Stmt::Drop(span)],
        else_: Vec::new(),
    })
    .into_iter()
    .collect()
}

/// `Object.assign(into, json(input))`, and the call each other kind makes, after a declaration of each of `fields`
/// under `into`. An empty `into` is the record.
pub fn parser(
    input: &str,
    into: &str,
    kind: &ParserKind,
    fields: &[DeclaredField],
    held: impl Fn(&str) -> Held,
) -> Lowered {
    let text = field(input);
    let parse = match kind {
        ParserKind::Json { .. } => call("json", vec![text]),
        ParserKind::Cef { .. } => call("cef", vec![text]),
        ParserKind::KeyValue(c) => {
            let quote = c
                .quote
                .map_or(Expr::new(Span::default(), ExprKind::Literal(Value::Null)), byte);
            let options = object(vec![
                ("pairSep", byte(c.pair_sep)),
                ("kvSep", byte(c.kv_sep)),
                ("quote", quote),
                ("escapes", boolean(c.escapes)),
                ("nextKey", boolean(c.next_key)),
            ]);
            call("kv", vec![text, options])
        }
        ParserKind::Csv(c) => {
            let headers = c.headers.iter().map(|h| h.as_deref().map_or(null(), string)).collect();
            let options = object(vec![
                ("delimiter", byte(c.delimiter)),
                ("quote", byte(c.quote)),
                ("escape", c.escape.map_or(null(), byte)),
                ("squeeze", boolean(c.squeeze)),
                ("trim", boolean(c.trim)),
            ]);
            call("csv", vec![text, array(headers), options])
        }
        ParserKind::Grok(c) => {
            let mut args = vec![text, strings(&c.patterns)];
            if !c.custom_patterns.is_empty() {
                let named = c.custom_patterns.iter().map(|(k, p)| (k.as_str(), string(p))).collect();
                args.push(object(vec![("patterns", object(named))]));
            }
            call("grok", args)
        }
        ParserKind::Variant => unreachable!("no step spreads a variant"),
    };
    let parse = with_value(parse, "splitDots", boolean(kind.split_dots()));
    let into = match into {
        "" => ROOT,
        into => into,
    };
    let typed = fields
        .iter()
        .map(|d| (d.name.as_str(), d.ty.clone(), d.time_source.as_ref()));
    declare_around(into, typed, parse, kind.is_dynamic(), held)
}

/// `Object.assign($, lookup(table, input, {…}))`, after a declaration of each output.
pub fn lookup(
    table: &str,
    input: &str,
    match_kind: MatchKind,
    multiplicity: Multiplicity,
    key_column: &str,
    outputs: &[LookupOutput],
    held: impl Fn(&str) -> Held,
) -> Lowered {
    let mut options = vec![
        (
            "match",
            string(match match_kind {
                MatchKind::Exact => "exact",
                MatchKind::Cidr => "cidr",
                MatchKind::Regex => "regex",
            }),
        ),
        ("column", string(key_column)),
    ];
    match multiplicity {
        Multiplicity::Best => {}
        Multiplicity::First => options.push(("first", boolean(true))),
        Multiplicity::All => options.push(("all", boolean(true))),
    }
    let fields = outputs
        .iter()
        .map(|o| {
            let default = o.default.as_deref().map_or(null(), string);
            (o.field.as_str(), array(vec![string(&o.source), default]))
        })
        .collect();
    options.push(("fields", object(fields)));
    let parse = call("lookup", vec![string(table), field(input), object(options)]);
    let all = multiplicity == Multiplicity::All;
    let typed = outputs.iter().map(|o| {
        let ty = match all {
            true => DataType::Array(Box::new(o.ty.clone())),
            false => o.ty.clone(),
        };
        (o.field.as_str(), ty, o.time_source.as_ref())
    });
    declare_around(ROOT, typed, parse, false, held)
}

/// `Object.assign(into, parse)`, each field declared under `into` before it where the name does not hold its type,
/// and a formatted `datetime` read by the parse's `formats`. Fields under an object nothing holds yet declare it once,
/// open, with all of them; so does an `into` nothing holds. A key the record may hold converts after a `dynamic` parse,
/// which lays it as it finds it, and before any other, whose fields are typed by what the name holds; a formatted one is
/// read as text and converted after.
fn declare_around<'f>(
    into: &str,
    fields: impl Iterator<Item = (&'f str, DataType, Option<&'f TimeSource>)>,
    parse: Expr,
    dynamic: bool,
    held: impl Fn(&str) -> Held,
) -> Lowered {
    let mut lowered = Lowered::default();
    let mut objects: Vec<(&str, Vec<(String, DataType)>)> = Vec::new();
    let mut formats = Vec::new();
    let open = || DataType::Struct(Vec::new(), true);
    // `Object.assign` merges into an object a name holds, so `into` is made one first. Its own entry comes before the
    // fields under it, which `nest` lays inside it.
    if into != ROOT {
        match held(into) {
            Held::Bound(_) => {}
            Held::Free => match tree::ancestors(into).find(|above| matches!(held(above), Held::Free)) {
                Some(top) => objects.push((top, vec![(tree::key_under(top, into).to_string(), open())])),
                None => objects.push((into, Vec::new())),
            },
            Held::Key => lowered.body.push(Stmt::Assign {
                target: into.to_string(),
                value: declared(into, Some(open()), field(into)),
                typed: true,
            }),
        }
    }
    let names: Vec<_> = fields
        .map(|(key, ty, time)| {
            let name = match into {
                ROOT => key.to_string(),
                _ => tree::join(into, key),
            };
            (key, name, ty, time)
        })
        .collect();
    for (key, name, ty, time) in &names {
        let (name, ty) = (name.as_str(), ty.clone());
        let format = match time {
            Some(TimeSource::Format(f)) => Some(f),
            _ => None,
        };
        match held(name) {
            // Converted already, by the step that bound it.
            Held::Bound(t) if t == ty => {}
            Held::Bound(_) => lowered.body.push(Stmt::Declare {
                name: name.to_string(),
                ty: ty.clone(),
            }),
            Held::Free => {
                let top = tree::ancestors(name).find(|above| matches!(held(above), Held::Free));
                match top {
                    Some(top) => {
                        let below = tree::key_under(top, name).to_string();
                        match objects.iter_mut().find(|(o, _)| *o == top) {
                            Some((_, fields)) => fields.push((below, ty.clone())),
                            None => objects.push((top, vec![(below, ty.clone())])),
                        }
                    }
                    None => lowered.body.push(Stmt::Declare {
                        name: name.to_string(),
                        ty: ty.clone(),
                    }),
                }
            }
            // `name: T = name`: the record's key, converted.
            Held::Key => {
                let staged = format.map_or_else(|| ty.clone(), |_| DataType::Str);
                let convert = Stmt::Assign {
                    target: name.to_string(),
                    value: declared(name, Some(staged), field(name)),
                    typed: true,
                };
                match dynamic {
                    true => lowered.after.push(convert),
                    false => lowered.body.push(convert),
                }
                if let Some(format) = format {
                    let read = call("parseTime", vec![field(name), string(format)]);
                    lowered.after.push(Stmt::Assign {
                        target: name.to_string(),
                        value: declared(name, Some(ty), read),
                        typed: true,
                    });
                }
                continue;
            }
        }
        // The parse's own key, not the name it lands at.
        if let Some(format) = format {
            formats.push((*key, string(format)));
        }
    }
    let parse = with_option(parse, "formats", formats);
    let objects = objects.into_iter().map(|(name, fields)| {
        let mut ty = open();
        for (path, leaf) in fields {
            nest(&mut ty, &path, leaf);
        }
        let name = name.to_string();
        Stmt::Declare { name, ty }
    });
    lowered.body.splice(0..0, objects);
    lowered.body.push(Stmt::Assign {
        target: into.to_string(),
        value: call("Object.assign", vec![field(into), parse]),
        typed: false,
    });
    lowered
}

/// `parse` with `entries` as its option `key`, in the options literal it ends with or a new one. No entries add nothing.
fn with_option(parse: Expr, key: &str, entries: Vec<(&str, Expr)>) -> Expr {
    match entries.is_empty() {
        true => parse,
        false => with_value(parse, key, object(entries)),
    }
}

/// `parse` with `value` as its option `key`, in the options literal it ends with or a new one.
fn with_value(mut parse: Expr, key: &str, value: Expr) -> Expr {
    let ExprKind::Call { args, .. } = &mut parse.kind else {
        return parse;
    };
    let option = (Some(key.to_string()), value);
    match args.last_mut().map(|a| &mut a.kind) {
        Some(ExprKind::Object(options)) => options.push(option),
        _ => args.push(Expr::new(Span::default(), ExprKind::Object(vec![option]))),
    }
    parse
}

/// `object` with `leaf` at `path` under it, each object on the way open.
fn nest(object: &mut DataType, path: &str, leaf: DataType) {
    let DataType::Struct(fields, _) = object else {
        return;
    };
    let (key, below) = tree::split_first(path);
    let at = match fields.iter().position(|(k, _)| *k == key) {
        Some(at) => at,
        None => {
            fields.push((key.into_owned(), DataType::Struct(Vec::new(), true)));
            fields.len() - 1
        }
    };
    match below {
        None => fields[at].1 = leaf,
        Some(below) => nest(&mut fields[at].1, below, leaf),
    }
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    let kind = ExprKind::Call {
        name: name.to_string(),
        args,
    };
    Expr::new(Span::default(), kind)
}

fn field(name: &str) -> Expr {
    Expr::new(Span::default(), ExprKind::Field(name.to_string()))
}

fn string(s: &str) -> Expr {
    Expr::new(Span::default(), ExprKind::Literal(Value::Str(s.to_string())))
}

fn byte(b: u8) -> Expr {
    string(&char::from(b).to_string())
}

fn boolean(b: bool) -> Expr {
    Expr::new(Span::default(), ExprKind::Literal(Value::Bool(b)))
}

fn null() -> Expr {
    Expr::new(Span::default(), ExprKind::Literal(Value::Null))
}

fn array(items: Vec<Expr>) -> Expr {
    Expr::new(Span::default(), ExprKind::Array(items))
}

fn strings(items: &[String]) -> Expr {
    array(items.iter().map(|s| string(s)).collect())
}

fn object(entries: Vec<(&str, Expr)>) -> Expr {
    let entries = entries.into_iter().map(|(k, v)| (Some(k.to_string()), v)).collect();
    Expr::new(Span::default(), ExprKind::Object(entries))
}
