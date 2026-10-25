//! What may follow a `.` an author has just typed: the expression the dot follows, and, once the compiler has typed
//! it where it sits, the fields an object holds and whether each function a call there may name takes it.

use crate::core::column::DataType;
use crate::expr::ast::{Expr, ExprKind};
use crate::expr::inline::PRELUDE_FUNCTIONS;
use crate::expr::lex::{Lexer, Punct, Spanned, Token};
use crate::expr::parse::parse_expr;
use crate::ops::objectops::ObjectOp;
use crate::ops::stdfuncs::{Arg, ROWS};
use crate::plan::resolve::{ResourceBuilder, SymbolTable, resolve};

/// What a `.` follows.
#[derive(Debug)]
pub enum Before {
    /// No value: the dot is in a string or a comment, or it is a number's decimal point. A number literal is never
    /// read as a value before a dot, since `1.` is how a decimal starts.
    Nothing,
    /// A value this does not read, such as a bracketed sum: `(a + b).`.
    Unread,
    /// The expression the dot follows: `$`, the record, where nothing touches the dot.
    Value(Expr),
}

/// What the `.` at byte `dot` of `src` follows: the longest run of tokens ending at it that parses as one operand, so
/// `a == b.` follows `b` and `f(x).` follows `f(x)`. A call binds tighter than any operator, so the longest such run
/// is the one the call will take.
pub fn before_dot(src: &str, dot: usize) -> Before {
    read(src, dot).map_or(Before::Nothing, |(_, before, _)| before)
}

/// `before_dot`, with the tokens before the dot and where in `src` what it follows starts. `None` where it follows
/// nothing.
fn read(src: &str, dot: usize) -> Option<(Vec<Spanned>, Before, usize)> {
    if src.get(dot..=dot) != Some(".") {
        return None;
    }
    // The dot itself is a token only where it is not in a string, a comment or a number.
    let mut toks = Lexer::tokenize(&src[..=dot]).ok()?;
    toks.retain(|t| t.token != Token::Eof);
    if toks
        .pop()
        .is_none_or(|t| t.token != Token::Punct(Punct::Dot) || t.span.start as usize != dot)
    {
        return None;
    }
    let head = &src[..dot];
    match toks.last() {
        // `1.` is a number being typed.
        Some(t) if t.span.end as usize == dot && matches!(t.token, Token::Int(_) | Token::Float(_)) => None,
        Some(t) if t.span.end as usize == dot => {
            let found = toks.iter().find_map(|t| {
                let start = t.span.start as usize;
                parse_expr(&head[start..]).ok().filter(is_operand).map(|e| (start, e))
            });
            Some(match found {
                Some((start, e)) => (toks, Before::Value(e), start),
                None => (toks, Before::Unread, dot),
            })
        }
        _ => Some((
            toks,
            Before::Value(Expr::new(Default::default(), ExprKind::Field("$".into()))),
            dot,
        )),
    }
}

/// A code step's body, cut where the statement the dot is in starts, so the names its statements before the dot
/// bind are bound: the statements before it, then a statement that reads what the dot follows, starting at byte `at`,
/// then a `}` for each block still open.
#[derive(Debug, Clone, PartialEq)]
pub struct Cut {
    pub body: String,
    pub at: u32,
}

/// The `Cut` for the `.` at byte `dot` of a code step's body, `src`. `None` where the dot follows no value, or only
/// the record.
pub fn cut_at(src: &str, dot: usize) -> Option<Cut> {
    let (toks, before, start) = read(src, dot)?;
    if !matches!(before, Before::Value(_)) || start == dot {
        return None;
    }
    // The statement starts after the last `;`, `{` or `}` before what the dot follows.
    let ends = |t: &&Spanned| matches!(t.token, Token::Punct(Punct::Semi | Punct::LBrace | Punct::RBrace));
    let kept: Vec<&Spanned> = toks.iter().filter(|t| (t.span.end as usize) <= start).collect();
    let n = kept.iter().rposition(|t| ends(t)).map_or(0, |i| i + 1);
    let open = kept[..n].iter().fold(0usize, |open, t| match t.token {
        Token::Punct(Punct::LBrace) => open + 1,
        Token::Punct(Punct::RBrace) => open.saturating_sub(1),
        _ => open,
    });
    let mut body = src[..kept[..n].last().map_or(0, |t| t.span.end as usize)].to_string();
    body.push('\n');
    let at = body.len() as u32;
    // A name alone is not a statement; a call that reads it is.
    body.push_str(&format!("isNull({});\n", &src[start..dot]));
    body.push_str(&"}".repeat(open));
    Some(Cut { body, at })
}

/// Whether `e` is one operand, not an operator over several.
fn is_operand(e: &Expr) -> bool {
    !matches!(
        e.kind,
        ExprKind::Binary { .. } | ExprKind::Unary { .. } | ExprKind::Ternary { .. } | ExprKind::Arrow(_)
    )
}

/// The type `e` has read against `syms`. `None` where it does not resolve.
pub fn type_in(e: &Expr, syms: &SymbolTable) -> Option<DataType> {
    resolve(e, &mut syms.clone(), &mut ResourceBuilder::default())
        .ok()
        .map(|ir| ir.ty)
}

/// An object's fields, by key, each with its type. None for any other type.
pub fn fields_of(ty: &DataType) -> &[(String, DataType)] {
    match ty {
        DataType::Struct(fields, _) => fields,
        _ => &[],
    }
}

/// A function a call on a value may name: what its first parameter takes, and whether that takes `ty`.
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct Method {
    pub name: &'static str,
    /// The types it takes, as a type error spells them: `i64/f64`.
    pub takes: String,
    pub fits: bool,
}

/// Each function a call on a value of `ty` may name. None where only a row knows the type, a variant or null: any of
/// them may fit.
pub fn methods_for(ty: &DataType) -> Vec<Method> {
    if matches!(ty, DataType::Variant | DataType::Null) {
        return Vec::new();
    }
    let arg = Arg::Ty(ty.clone());
    let mut out: Vec<Method> = Vec::new();
    for row in ROWS {
        let Some(first) = row.params.first() else {
            continue;
        };
        match out.iter_mut().find(|m| m.name == row.name) {
            Some(m) => {
                if !m.takes.split('/').any(|t| t == first.describe()) {
                    m.takes = format!("{}/{}", m.takes, first.describe());
                }
                m.fits |= first.accepts(&arg);
            }
            None => out.push(Method {
                name: row.name,
                takes: first.describe().to_string(),
                fits: first.accepts(&arg),
            }),
        }
    }
    for op in ObjectOp::NAMED {
        out.push(Method {
            name: op.name().expect("a named op has a name"),
            takes: op.takes().into(),
            fits: op.accepts(ty),
        });
    }
    // The prelude's functions walk their first argument with `for...of`, which takes an array.
    for name in PRELUDE_FUNCTIONS {
        out.push(Method {
            name,
            takes: "array".into(),
            fits: matches!(ty, DataType::Array(_)),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::plan::ingest_fields;
    use crate::plan::topology::SourceMeta;

    /// What the dot ending `src` follows, as a name, a call's name, `[]` or `unread`.
    fn before(src: &str) -> Option<String> {
        match before_dot(src, src.len() - 1) {
            Before::Value(e) => Some(match e.kind {
                ExprKind::Field(name) => name,
                ExprKind::Call { name, .. } => format!("{name}()"),
                ExprKind::Index { .. } => "[]".into(),
                other => format!("{other:?}"),
            }),
            Before::Unread => Some("unread".into()),
            Before::Nothing => None,
        }
    }

    #[test]
    fn a_dot_follows_the_operand_it_touches() {
        assert_eq!(before("_raw."), Some("_raw".into()));
        assert_eq!(before("x == 1 && url.path."), Some("url.path".into()));
        assert_eq!(before("a + _raw.length()."), Some("_raw.length()".into()));
        assert_eq!(before(r#"labels["a.b"]."#), Some(r#"labels["a.b"]"#.into()));
        assert_eq!(before("tags[0]."), Some("[]".into()));
        assert_eq!(before("!flag."), Some("flag".into()));
        assert_eq!(before("x == 1 && ."), Some("$".into()));
        assert_eq!(before("(a + b)."), Some("unread".into()));
    }

    #[test]
    fn a_dot_in_a_string_a_comment_or_a_number_follows_nothing() {
        assert_eq!(before(r#""a."#), None);
        assert_eq!(before("x == 1."), None);
        assert_eq!(before("x // a."), None);
    }

    #[test]
    fn a_cut_keeps_the_statements_before_the_one_the_dot_is_in() {
        let cut = |src: &str| cut_at(src, src.len() - 1).map(|c| (c.body[c.at as usize..].to_string(), c.body));
        assert_eq!(
            cut("let u = parseUrl(_raw); x = f(u."),
            Some(("isNull(u);\n".into(), "let u = parseUrl(_raw);\nisNull(u);\n".into()))
        );
        assert_eq!(
            cut("if (a) { let v = 1; if (v."),
            Some(("isNull(v);\n}".into(), "if (a) { let v = 1;\nisNull(v);\n}".into()))
        );
        assert_eq!(
            cut("x = {a: 1} y.").map(|c| c.1),
            Some("x = {a: 1}\nisNull(y);\n".into())
        );
        assert_eq!(cut("x = 1; ."), None);
        assert_eq!(cut("x = \"a."), None);
    }

    #[test]
    fn the_type_is_the_compilers() {
        let meta = SourceMeta::KAFKA;
        let syms = SymbolTable::new(&ingest_fields(&meta));
        let ty = |src: &str| match before_dot(src, src.len() - 1) {
            Before::Value(e) => type_in(&e, &syms),
            _ => None,
        };
        assert_eq!(ty("_raw."), Some(DataType::Str));
        assert_eq!(ty("_raw.length()."), Some(DataType::I64));
        assert_eq!(ty("parseUrl(_raw).path."), Some(DataType::Str));
        assert!(matches!(ty("parseUrl(_raw)."), Some(DataType::Struct(..))));
        assert_eq!(ty("nope."), None);
    }

    #[test]
    fn a_method_fits_where_its_first_parameter_takes_the_type() {
        let fit = |ty: &DataType, name: &str| methods_for(ty).into_iter().find(|m| m.name == name).unwrap();
        assert_eq!(
            fit(&DataType::Str, "sign"),
            Method {
                name: "sign",
                takes: "i64/f64".into(),
                fits: false
            }
        );
        assert!(fit(&DataType::I64, "sign").fits);
        assert!(fit(&DataType::Str, "length").fits);
        assert!(!fit(&DataType::Str, "keys").fits);
        assert!(fit(&DataType::Struct(vec![], true), "keys").fits);
        assert!(fit(&DataType::Array(Box::new(DataType::Str)), "map").fits);
        assert!(methods_for(&DataType::Variant).is_empty());
    }
}
