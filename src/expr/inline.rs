//! Inlining user-defined functions, and the desugarings that ride on it.
//!
//! A `function` is a compile-time definition. Every call is expanded on the AST before compilation: arguments are bound
//! to fresh `let` locals, the body's own locals are renamed per call, its statements are hoisted ahead of the statement
//! containing the call, and the returned expression takes the call's place. Nothing survives into the `Ir`.
//!
//! An early `return` writes the call's result local and leaves the call's `Body`, and a self-call in tail position
//! becomes a `while` loop. The reserved prefix is `__inl`.
//!
//! A function is a value at compile time: a name holds one where a `function` declares it, a `let` binds an arrow to
//! it, or a parameter is given one. A function sees the functions in scope where it was written, so a call through a
//! parameter expands the arrow the caller passed, in the caller's scope. `map` and `filter` are functions of the
//! prelude, written as a pipeline's own are.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::core::column::DataType;
use crate::expr::ast::{Expr, ExprKind, Function, Region, Site, Span, Stmt, Value};
use crate::expr::parse;
use crate::ops::{BinOp, UnOp};
use crate::plan::{resolve, tree};

/// The functions every pipeline has, written as a pipeline's own are.
const PRELUDE: &str = include_str!("prelude.smeltr");

/// The prelude's function names.
pub const PRELUDE_FUNCTIONS: [&str; 2] = ["map", "filter"];

/// How deep calls may nest. Recursion is caught by name first; this bounds the expansion of a block whose helpers
/// merely nest deeply.
const MAX_INLINE_DEPTH: usize = 32;
/// How many calls one block may inline in all. Depth bounds nothing on its own: a function that calls the next
/// twice doubles at every level, and thirty-two levels of that is billions of statements. A real block inlines
/// dozens. The refusal spells the number out.
const MAX_INLINED_CALLS: usize = 10_000;

pub struct InlineError {
    pub span: Span,
    pub message: String,
}

/// A function a name holds.
#[derive(Clone)]
struct Bound {
    function: Rc<Function>,
    /// Which function it is, for the recursion check: a declared function's name, or an arrow's number.
    id: String,
    /// The scopes where it was written, which its body sees. `None` for a declared function, which sees the declared
    /// functions.
    scopes: Option<Rc<Vec<Scope>>>,
}

/// The functions a block's names hold.
type Scope = HashMap<String, Bound>;

struct Inliner {
    /// The declared functions, the prelude's among them, then a scope for each block being expanded.
    scopes: Vec<Scope>,
    /// The calls being expanded: each function with the functions its parameters hold, and its name. Reaching one
    /// again is recursion.
    active: Vec<(String, String)>,
    next_tmp: usize,
    next_arrow: usize,
    /// The locals for a name the user wrote, a parameter or a function body's own, that an arrow in that body writes.
    arrow_written: HashSet<String>,
    /// Calls inlined so far, bounded by `MAX_INLINED_CALLS`.
    inlined: usize,
}

/// The prefixes this pass mints temporaries under. A bare `__` cannot be reserved: `__destination.topic` is a user-set control
/// field.
pub(crate) const RESERVED_PREFIXES: [&str; 2] = ["__inl", LOCAL_PREFIX];

/// The prefix of a user's local that a function can name, renamed so no call site captures it (`lexical`). It carries
/// the user's name, and every message gives that name.
pub(crate) const LOCAL_PREFIX: &str = "__loc";

/// `text` with every local `lexical` renamed under the name the user wrote.
pub fn user_names(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.contains(LOCAL_PREFIX) {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(LOCAL_PREFIX) {
        out.push_str(&rest[..at]);
        let after = &rest[at + LOCAL_PREFIX.len()..];
        let digits = after.len() - after.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        // Only at a name's start: the user may write the prefix inside a name of their own.
        let starts = !out.ends_with(|c: char| c.is_alphanumeric() || c == '_');
        match (starts && digits > 0, after[digits..].strip_prefix('_')) {
            (true, Some(name)) => rest = name,
            _ => {
                out.push_str(LOCAL_PREFIX);
                rest = after;
            }
        }
    }
    out.push_str(rest);
    std::borrow::Cow::Owned(out)
}

/// `null` typed as `ty`: the seed of a function's result, and a field a returned object lacks.
fn typed_null(ty: &DataType, span: Span) -> Expr {
    let null = Expr {
        span,
        kind: ExprKind::Literal(Value::Null),
    };
    Expr {
        span,
        kind: ExprKind::Coerce {
            ty: ty.clone(),
            value: Box::new(null),
        },
    }
}

/// The seed of a function's result: `null`, typed as the function's return where it has one.
fn seed(ty: Option<&DataType>, span: Span) -> Expr {
    match ty {
        Some(ty) => typed_null(ty, span),
        None => Expr::new(span, ExprKind::Literal(Value::Null)),
    }
}

/// `value`, checked against `ty` at `site` where a type is written.
fn check_optional(ty: Option<&DataType>, value: Expr, site: Site) -> Expr {
    match ty {
        Some(ty) => checked(ty, value, site),
        None => value,
    }
}

/// The site where `param` of `function` meets its argument.
fn argument(function: &str, param: &str) -> Site {
    Site::Argument {
        function: function.to_string(),
        param: param.to_string(),
    }
}

/// `value`, which must already have type `ty` at `site`.
fn checked(ty: &DataType, value: Expr, site: Site) -> Expr {
    Expr {
        span: value.span,
        kind: ExprKind::Check {
            ty: ty.clone(),
            value: Box::new(value),
            site,
        },
    }
}

/// Check every `return` of `function` in `stmts` against `ret` where it is written, so each branch meets the declared
/// type on its own.
fn check_returns(stmts: &mut [Stmt], ret: &DataType, function: &str) -> Result<(), InlineError> {
    for stmt in stmts {
        match stmt {
            Stmt::Return(e) => *e = check_return(ret, e.clone(), function)?,
            Stmt::Function { .. } => {}
            s => {
                for b in s.blocks_mut() {
                    check_returns(b, ret, function)?;
                }
            }
        }
    }
    Ok(())
}

/// Check every assignment to `target` in `stmts` against `ret` as a `return` of `function`. A recursive function's
/// returns are checked this way, after their calls are inlined, so a returned call's object literal is checked field
/// by field.
fn check_assigned(stmts: &mut [Stmt], target: &str, ret: &DataType, function: &str) -> Result<(), InlineError> {
    for stmt in stmts {
        if let Stmt::Assign { target: t, value, .. } = stmt
            && t == target
        {
            *value = check_return(ret, value.clone(), function)?;
        }
        for b in stmt.blocks_mut() {
            check_assigned(b, target, ret, function)?;
        }
    }
    Ok(())
}

/// A `return` of `function` checked against `ret`. An object literal bound to the result is its paths, so under an
/// object type each of its fields is checked on its own.
fn check_return(ret: &DataType, value: Expr, function: &str) -> Result<Expr, InlineError> {
    let site = format!("the return of `{function}`");
    match (ret, &value.kind) {
        (DataType::Struct(..), ExprKind::Object(_)) => check_fields(ret, value, &site, ""),
        _ => Ok(checked(ret, value, Site::Return(site))),
    }
}

/// An object literal returned where the object type `ty` is declared, `prefix` the path to it. Each field is checked
/// against the type's, nested where both nest. A field the type names and the literal lacks is a typed null,
/// and one it does not name is an error. A literal holding a spread is checked whole.
fn check_fields(ty: &DataType, value: Expr, site: &str, prefix: &str) -> Result<Expr, InlineError> {
    let DataType::Struct(fields, _) = ty else {
        unreachable!("called with an object type")
    };
    let span = value.span;
    let ExprKind::Object(pairs) = value.kind else {
        unreachable!("called on an object literal")
    };
    if pairs.iter().any(|(k, _)| k.is_none()) {
        let value = Expr {
            span,
            kind: ExprKind::Object(pairs),
        };
        return Ok(checked(ty, value, Site::Return(site.to_string())));
    }
    let mut checked_pairs = Vec::with_capacity(fields.len());
    for (k, v) in pairs {
        let k = k.expect("a literal with no spread");
        let path = tree::join(prefix, &k);
        let Some((_, t)) = fields.iter().find(|(n, _)| tree::spell(n) == k) else {
            return Err(InlineError {
                span: v.span,
                message: format!("{site} has a field `{path}` its type does not name"),
            });
        };
        let v = match (t, &v.kind) {
            (DataType::Struct(..), ExprKind::Object(_)) => check_fields(t, v, site, &path)?,
            _ => checked(t, v, Site::Return(format!("field `{path}` of {site}"))),
        };
        checked_pairs.push((Some(k), v));
    }
    for (k, t) in fields {
        let k = tree::spell(k).into_owned();
        if checked_pairs.iter().any(|(n, _)| n.as_ref() == Some(&k)) {
            continue;
        }
        let v = match t {
            DataType::Struct(..) => {
                let empty = Expr {
                    span,
                    kind: ExprKind::Object(Vec::new()),
                };
                check_fields(t, empty, site, &tree::join(prefix, &k))?
            }
            t => typed_null(t, span),
        };
        checked_pairs.push((Some(k), v));
    }
    Ok(Expr {
        span,
        kind: ExprKind::Object(checked_pairs),
    })
}

/// The first `name: type;` in `stmts` whose name is one of `locals`, a function's parameters and body locals: a
/// local's type is its signature's or its value's, never a declaration's.
fn declared_local<'a>(stmts: &'a [Stmt], local: &impl Fn(&str) -> bool) -> Option<&'a str> {
    Stmt::find_map(stmts, &|s| !matches!(s, Stmt::Function { .. }), &mut |s| match s {
        Stmt::Declare { name, .. } if local(tree::head(name)) => Some(name.as_str()),
        _ => None,
    })
}

/// Hoist every `function` and inline every call: an early `return` leaves the call's `Body`. The result has no
/// `Function` or `Return` in it.
pub fn expand(stmts: &[Stmt]) -> Result<Vec<Stmt>, InlineError> {
    check_reserved_names(stmts)?;

    let prelude = parse::parse_block(PRELUDE).unwrap_or_else(|e| panic!("the prelude parses: {}", e.message));
    let stmts = &lexical(stmts);
    let mut declared = Scope::new();
    collect(&prelude, &mut declared, false)?;
    collect(stmts, &mut declared, true)?;
    let mut inliner = Inliner {
        scopes: vec![declared],
        active: Vec::new(),
        next_tmp: 0,
        next_arrow: 0,
        arrow_written: HashSet::new(),
        inlined: 0,
    };
    inliner.block(stmts)
}

/// `stmts` with each local a function or an arrow can name given a name of its own, so a function reads the names
/// where it was written: a call's body lands among the caller's locals, and a same-named one there would take its
/// name.
fn lexical(stmts: &[Stmt]) -> Vec<Stmt> {
    let mut stmts = stmts.to_vec();
    let named = named_by_functions(&stmts);
    let declared: HashSet<String> = (stmts.iter())
        .filter_map(|s| match s {
            Stmt::Function { name, .. } => Some(name.clone()),
            _ => None,
        })
        .chain(PRELUDE_FUNCTIONS.map(str::to_string))
        .collect();
    let (mut functions, mut next) = (HashSet::new(), 0);
    rename(&mut stmts, &HashMap::new(), &mut |name, value| {
        let function = match value.map(|v| &v.kind) {
            Some(ExprKind::Arrow(_)) => true,
            Some(ExprKind::Field(n)) => functions.contains(n) || declared.contains(n),
            _ => false,
        };
        if function {
            functions.insert(name.to_string());
            return None;
        }
        functions.remove(name);
        if !named.contains(name) {
            return None;
        }
        next += 1;
        Some(format!("{LOCAL_PREFIX}{next}_{name}"))
    });
    stmts
}

/// The names at the head of a name the bodies of `stmts`' functions and arrows read or write, their own among them.
fn named_by_functions(stmts: &[Stmt]) -> HashSet<String> {
    fn names_in(stmts: &[Stmt], out: &mut HashSet<String>) {
        Stmt::any(stmts, &|_| true, &mut |s| {
            match s {
                Stmt::Assign { target, .. } | Stmt::IndexAssign { target, .. } | Stmt::Unroll { field: target, .. } => {
                    out.insert(tree::head(target).to_string());
                }
                Stmt::Delete { names, .. } => out.extend(names.iter().map(|n| tree::head(n).to_string())),
                _ => {}
            }
            for e in s.exprs() {
                e.find_map::<()>(&mut |x| {
                    match &x.kind {
                        ExprKind::Field(n) | ExprKind::Call { name: n, .. } => {
                            _ = out.insert(tree::head(n).to_string())
                        }
                        ExprKind::Arrow(f) => names_in(&f.body, out),
                        _ => {}
                    }
                    None
                });
            }
            false
        });
    }
    let mut out = HashSet::new();
    Stmt::any(stmts, &|_| true, &mut |s| {
        if let Stmt::Function { function, .. } = s {
            names_in(&function.body, &mut out);
        }
        for e in s.exprs() {
            e.find_map::<()>(&mut |x| {
                if let ExprKind::Arrow(f) = &x.kind {
                    names_in(&f.body, &mut out);
                }
                None
            });
        }
        false
    });
    out
}

/// Whether `body` writes `name` or a path under it, or binds it afresh, an arrow in it included.
fn writes(body: &[Stmt], name: &str) -> bool {
    Stmt::any(body, &|_| true, &mut |s| {
        let written = match s {
            Stmt::Assign { target, .. } | Stmt::IndexAssign { target, .. } | Stmt::Unroll { field: target, .. } => {
                tree::head(target) == name
            }
            Stmt::Let { name: n, .. } | Stmt::For { var: n, .. } => n == name,
            Stmt::Delete { names, .. } => names.iter().any(|n| tree::head(n) == name),
            _ => false,
        };
        written || arrow_in(s, name)
    })
}

/// Whether an arrow in `body` writes `name`: passed on, it can write the name inside a call that reads it.
fn arrow_writes(body: &[Stmt], name: &str) -> bool {
    Stmt::any(body, &|_| true, &mut |s| arrow_in(s, name))
}

/// Whether an arrow in `stmt`'s own expressions writes `name`.
fn arrow_in(stmt: &Stmt, name: &str) -> bool {
    stmt.exprs().into_iter().any(|e| {
        e.find_map(&mut |x| match &x.kind {
            ExprKind::Arrow(f) if writes(&f.body, name) => Some(()),
            _ => None,
        })
        .is_some()
    })
}

/// Whether `name` is one the inliner makes, which the user never wrote.
pub fn is_temporary(name: &str) -> bool {
    name.starts_with("__inl")
}

/// Rejects any name under `RESERVED_PREFIXES`, before any rewrite runs.
fn check_reserved_names(stmts: &[Stmt]) -> Result<(), InlineError> {
    let Some((name, span)) = reserved_name(stmts) else {
        return Ok(());
    };
    Err(InlineError {
        span,
        message: format!(
            "`{name}` uses a name prefix the compiler reserves for its own temporaries \
             ({}) -- rename it. A leading `__` on its own is fine and means \
             \"does not serialize\" (`__destination.topic`); only these prefixes are taken.",
            RESERVED_PREFIXES.join("`, `")
        ),
    })
}

/// The first name under `RESERVED_PREFIXES` in `stmts`, arrows' bodies included.
fn reserved_name(stmts: &[Stmt]) -> Option<(String, Span)> {
    let bad = |n: &str| RESERVED_PREFIXES.iter().any(|p| n.starts_with(p));
    Stmt::find_map(stmts, &|_| true, &mut |stmt| {
        let (names, span): (Vec<&String>, Span) = match stmt {
            Stmt::Let { name, value } => (vec![name], value.span),
            Stmt::Assign { target, value, .. } | Stmt::IndexAssign { target, value, .. } => (vec![target], value.span),
            Stmt::For { var, iterable, .. } => (vec![var], iterable.span),
            Stmt::Declare { name, .. } => (vec![name], Span::default()),
            Stmt::Function { name, function } => {
                (std::iter::once(name).chain(&function.params).collect(), Span::default())
            }
            Stmt::Delete { names, span, .. } => (names.iter().collect(), *span),
            Stmt::Unroll { field, span, .. } => (vec![field], *span),
            Stmt::Try { catches, .. } => (catches.iter().filter_map(|c| c.0.as_ref()).collect(), Span::default()),
            _ => (Vec::new(), Span::default()),
        };
        if let Some(name) = names.into_iter().find(|n| bad(n)) {
            return Some((name.clone(), span));
        }
        stmt.exprs().into_iter().find_map(|e| {
            e.find_map(&mut |e| match &e.kind {
                ExprKind::Field(n) | ExprKind::Call { name: n, .. } if bad(n) => Some((n.clone(), e.span)),
                ExprKind::Arrow(f) => match f.params.iter().find(|p| bad(p)) {
                    Some(p) => Some((p.clone(), e.span)),
                    None => reserved_name(&f.body),
                },
                _ => None,
            })
        })
    })
}

/// Refuses a built-in's name for a pipeline's own function, declared or `let`-bound.
fn own_name(name: &str, span: Span) -> Result<(), InlineError> {
    if !resolve::is_builtin(name) {
        return Ok(());
    }
    Err(InlineError {
        span,
        message: format!("function `{name}` has a built-in's name; give it a name of its own"),
    })
}

/// Declarations are hoisted; a helper may be defined below its first use. A pipeline's own function takes a name no
/// built-in has.
fn collect(stmts: &[Stmt], out: &mut Scope, own: bool) -> Result<(), InlineError> {
    for stmt in stmts {
        let Stmt::Function { name, function } = stmt else {
            continue;
        };
        if own {
            own_name(name, Span::default())?;
        }
        if out.contains_key(name) {
            return Err(InlineError {
                span: Span::default(),
                message: format!("function `{name}` is declared more than once"),
            });
        }
        check_body(&function.body, &format!("function `{name}`"))?;
        out.insert(
            name.clone(),
            Bound {
                function: Rc::new(function.clone()),
                id: name.clone(),
                scopes: None,
            },
        );
    }
    Ok(())
}

/// Refuses what a function's body cannot hold: a `function`, since there is one flat namespace of them, and a `break`
/// or `continue` outside a loop of its own, which cannot reach a loop around its call.
fn check_body(body: &[Stmt], what: &str) -> Result<(), InlineError> {
    if body.iter().any(|s| matches!(s, Stmt::Function { .. })) {
        return Err(InlineError {
            span: Span::default(),
            message: format!(
                "{what} declares a nested function -- declare it at the top level of the code block instead"
            ),
        });
    }
    let outside_a_loop = |s: &Stmt| !matches!(s, Stmt::For { .. } | Stmt::While { .. });
    if let Some((to, span)) = Stmt::find_map(body, &outside_a_loop, &mut |s| match s {
        Stmt::Leave { to, span } => Some((*to, *span)),
        _ => None,
    }) {
        let word = if to == Region::Trip { "continue" } else { "break" };
        return Err(InlineError {
            span,
            message: format!("`{word}` is only valid inside a loop"),
        });
    }
    Ok(())
}

/// `e`, leaving a null in its place.
fn take(e: &mut Expr) -> Expr {
    let null = Expr::new(e.span, ExprKind::Literal(Value::Null));
    std::mem::replace(e, null)
}

impl Inliner {
    fn tmp(&mut self, name: &str) -> String {
        self.next_tmp += 1;
        format!("__inl{}_{}", self.next_tmp, name)
    }

    /// A local for the user's name `name`, a parameter or a local of `body`.
    fn user_local(&mut self, name: &str, body: &[Stmt]) -> String {
        let local = self.tmp(name);
        if arrow_writes(body, name) {
            self.arrow_written.insert(local.clone());
        }
        local
    }

    /// The function `name` holds, from the innermost scope out.
    fn lookup(&self, name: &str) -> Option<&Bound> {
        self.scopes.iter().rev().find_map(|s| s.get(name))
    }

    /// Whether `name` holds one of the pipeline's own functions. A built-in's name read bare is a field's.
    fn user_function(&self, name: &str) -> bool {
        !resolve::is_builtin(name) && self.lookup(name).is_some()
    }

    /// The function `e` is, where it is one: an arrow, which sees the scopes it is written in, or a name holding one.
    fn function_value(&mut self, e: &Expr) -> Result<Option<Bound>, InlineError> {
        match &e.kind {
            ExprKind::Arrow(f) => {
                check_body(&f.body, "an arrow")?;
                self.next_arrow += 1;
                Ok(Some(Bound {
                    function: Rc::new(f.clone()),
                    id: format!("arrow {}", self.next_arrow),
                    scopes: Some(Rc::new(self.scopes.clone())),
                }))
            }
            ExprKind::Field(n) if self.user_function(n) => Ok(self.lookup(n).cloned()),
            _ => Ok(None),
        }
    }

    /// The function a call names, with its arguments. A method call of one is a call of it with the receiver first,
    /// as a built-in's is, so `xs.map(f)` is `map(xs, f)`. `Object` is a namespace, not a receiver.
    fn callee(&self, name: &str, args: &[Expr], span: Span) -> Option<(String, Vec<Expr>)> {
        if self.lookup(name).is_some() {
            return Some((name.to_string(), args.to_vec()));
        }
        let (recv, method) = name.rsplit_once('.')?;
        if recv == "Object" {
            return None;
        }
        self.lookup(method)?;
        let recv = Expr::new(span, ExprKind::Field(recv.to_string()));
        Some((
            method.to_string(),
            std::iter::once(recv).chain(args.iter().cloned()).collect(),
        ))
    }

    /// `stmts`, expanded in a scope of their own.
    fn block(&mut self, stmts: &[Stmt]) -> Result<Vec<Stmt>, InlineError> {
        self.scopes.push(Scope::new());
        let mut out = Vec::with_capacity(stmts.len());
        let expanded = stmts.iter().try_for_each(|stmt| match stmt {
            Stmt::Function { .. } => Ok(()),
            Stmt::Return(e) => Err(InlineError {
                span: e.span,
                message: "`return` is only valid inside a `function` body".into(),
            }),
            _ => self.stmt(stmt, &mut out),
        });
        self.scopes.pop();
        expanded.map(|()| out)
    }

    fn stmt(&mut self, stmt: &Stmt, out: &mut Vec<Stmt>) -> Result<(), InlineError> {
        self.refuse_function_name(stmt)?;
        match stmt {
            // The "assigns nothing" check runs here, before `emit_while` adds an assignment of its own.
            Stmt::While { cond, body, .. } => {
                if !assigns_anything(body) {
                    return Err(InlineError {
                        span: cond.span,
                        message: "a `while` body that assigns nothing can never change its \
                                  own condition, so the loop either runs zero times or \
                                  forever -- assign something the condition reads"
                            .into(),
                    });
                }
                let body = self.block(body)?;
                self.emit_while(cond, body, out)
            }
            Stmt::Let { name, value } => match self.function_value(value)? {
                // `name` holds the function from here on: nothing runs, and nothing is written.
                Some(bound) => {
                    own_name(name, value.span)?;
                    self.scopes
                        .last_mut()
                        .expect("a block's scope")
                        .insert(name.clone(), bound);
                    Ok(())
                }
                None => self.each(stmt, out),
            },
            Stmt::Expr(Expr {
                kind: ExprKind::Call { name, args },
                span,
            }) => match self.callee(name, args, *span) {
                Some((name, args)) => self.call_stmt(&name, &args, *span, out),
                None => self.each(stmt, out),
            },
            Stmt::Function { .. } | Stmt::Return(_) => {
                unreachable!("handled by `block` before reaching here")
            }
            _ => self.each(stmt, out),
        }
    }

    /// `stmt` with each expression and block in it expanded.
    fn each(&mut self, stmt: &Stmt, out: &mut Vec<Stmt>) -> Result<(), InlineError> {
        let mut stmt = stmt.clone();
        for e in stmt.exprs_mut() {
            self.rewrite(e, out)?;
        }
        for b in stmt.blocks_mut() {
            *b = self.block(b)?;
        }
        out.push(stmt);
        Ok(())
    }

    /// Refuses a write, a declaration, a value's `let` or a loop variable under a name that holds a function: one name
    /// is one thing.
    fn refuse_function_name(&self, stmt: &Stmt) -> Result<(), InlineError> {
        let (name, span) = match stmt {
            Stmt::Let { name, value } => (name, value.span),
            Stmt::Assign { target, value, .. } | Stmt::IndexAssign { target, value, .. } => (target, value.span),
            Stmt::Declare { name, .. } => (name, Span::default()),
            Stmt::For { var, iterable, .. } => (var, iterable.span),
            _ => return Ok(()),
        };
        let head = tree::head(name);
        if !self.user_function(head) {
            return Ok(());
        }
        Err(InlineError {
            span,
            message: format!("`{head}` names a function; give the value a name of its own"),
        })
    }

    /// `expr`, rewriting a copy of `e`.
    fn expr(&mut self, e: &Expr, out: &mut Vec<Stmt>) -> Result<Expr, InlineError> {
        let mut e = e.clone();
        self.rewrite(&mut e, out)?;
        Ok(e)
    }

    /// Rewrite an expression in place, hoisting statements onto `out`.
    fn rewrite(&mut self, e: &mut Expr, out: &mut Vec<Stmt>) -> Result<(), InlineError> {
        let span = e.span;
        match &e.kind {
            ExprKind::Call { name, args } => {
                if let Some((name, args)) = self.callee(name, args, span) {
                    *e = self.call_value(&name, &args, span, out)?;
                    return Ok(());
                }
            }
            ExprKind::Arrow(_) => {
                return Err(InlineError {
                    span,
                    message: "a function is a value only where it is called, passed to a function, or bound with \
                              `let`"
                        .into(),
                });
            }
            ExprKind::Field(n) if self.user_function(n) => {
                return Err(InlineError {
                    span,
                    message: format!("`{n}` is a function: call it, pass it to a function, or bind it with `let`"),
                });
            }
            ExprKind::Ternary { .. } => return self.ternary(e, out),
            ExprKind::Binary {
                op: BinOp::And | BinOp::Or | BinOp::Coalesce,
                ..
            } => return self.logic(e, out),
            _ => {}
        }
        for c in e.children_mut() {
            self.rewrite(c, out)?;
        }
        Ok(())
    }

    /// `cond ? then : else_`. A branch whose calls expand to statements runs them only on the rows that take it.
    fn ternary(&mut self, e: &mut Expr, out: &mut Vec<Stmt>) -> Result<(), InlineError> {
        let ExprKind::Ternary { cond, then, else_ } = &mut e.kind else {
            unreachable!("called on a ternary")
        };
        self.rewrite(cond, out)?;
        let (mut taken, mut not) = (Vec::new(), Vec::new());
        self.rewrite(then, &mut taken)?;
        self.rewrite(else_, &mut not)?;
        if !taken.is_empty() || !not.is_empty() {
            let span = e.span;
            let (cond, then, else_) = (take(cond), take(then), take(else_));
            *e = self.select(span, cond, (taken, then), (not, else_), out);
        }
        Ok(())
    }

    /// `lhs && rhs`, `lhs || rhs` or `lhs ?? rhs`. Where `rhs`'s calls expand to statements, they run only on the rows
    /// `lhs` leaves undecided: JavaScript stops at the first `false`, `true` or non-null.
    fn logic(&mut self, e: &mut Expr, out: &mut Vec<Stmt>) -> Result<(), InlineError> {
        let ExprKind::Binary { op, lhs, rhs } = &mut e.kind else {
            unreachable!("called on an operator")
        };
        let op = *op;
        self.rewrite(lhs, out)?;
        let mut taken = Vec::new();
        self.rewrite(rhs, &mut taken)?;
        if taken.is_empty() {
            return Ok(());
        }
        // `null ?? rhs` is `rhs` on every row: there is no `lhs` to hold, and a local holding only `null` has no type.
        if op == BinOp::Coalesce && matches!(lhs.kind, ExprKind::Literal(Value::Null)) {
            out.append(&mut taken);
            *e = take(rhs);
            return Ok(());
        }
        let span = e.span;
        let left = self.tmp("left");
        out.push(Stmt::Let {
            name: left.clone(),
            value: take(lhs),
        });
        let read = Expr::new(span, ExprKind::Field(left));
        let not = |e: Expr| {
            Expr::new(
                span,
                ExprKind::Unary {
                    op: UnOp::Not,
                    operand: Box::new(e),
                },
            )
        };
        let unless_null = || {
            Expr::new(
                span,
                ExprKind::Binary {
                    op: BinOp::Coalesce,
                    lhs: Box::new(read.clone()),
                    rhs: Box::new(lit_bool(false, span)),
                },
            )
        };
        // The rows `lhs` leaves undecided: null reads as false.
        let undecided = match op {
            BinOp::And => unless_null(),
            BinOp::Or => not(unless_null()),
            _ => Expr::new(
                span,
                ExprKind::Call {
                    name: "isNull".into(),
                    args: vec![read.clone()],
                },
            ),
        };
        // Only the right side is guarded. The operator is the one written, over a right side that holds nothing where
        // `lhs` decides.
        let none = Expr::new(span, ExprKind::Literal(Value::Null));
        let right = self.select(span, undecided, (taken, take(rhs)), (Vec::new(), none), out);
        **lhs = read;
        **rhs = right;
        Ok(())
    }

    /// An `if` on `test` whose arms run their statements and write their value to a local, which is what it gives.
    fn select(
        &mut self,
        span: Span,
        test: Expr,
        (mut taken, then): (Vec<Stmt>, Expr),
        (mut not, else_): (Vec<Stmt>, Expr),
        out: &mut Vec<Stmt>,
    ) -> Expr {
        let value = self.tmp("value");
        let assign = |v: Expr| Stmt::Assign {
            target: value.clone(),
            value: v,
            typed: false,
        };
        taken.push(assign(then));
        not.push(assign(else_));
        out.push(Stmt::Let {
            name: value.clone(),
            value: Expr::new(span, ExprKind::Literal(Value::Null)),
        });
        out.push(Stmt::If {
            cond: test,
            then: taken,
            else_: not,
        });
        Expr::new(span, ExprKind::Field(value))
    }

    /// Whether a call of `name` is the outermost of a declared function that calls itself by name, which expands to a
    /// loop. An arrow has no name of its own to call: a name in its body is a parameter's or its scope's.
    fn recurs(&self, name: &str, bound: &Bound) -> bool {
        bound.id == name && !self.active.iter().any(|(_, n)| n == name) && calls_itself(&bound.function.body, name)
    }

    /// The value of a call of the function `name` holds.
    fn call_value(&mut self, name: &str, args: &[Expr], span: Span, out: &mut Vec<Stmt>) -> Result<Expr, InlineError> {
        let bound = self.lookup(name).cloned().expect("a callee holds a function");
        // Checked on the callee's body, so the loop is built at the outermost call site.
        if self.recurs(name, &bound) {
            return self.tail_recursive_value(name, &bound, span, args, out);
        }
        let (body, caller) = self.enter(name, &bound, args, span, out)?;
        let value = self.body_value(name, &body, bound.function.ret.as_ref(), span, out);
        self.leave(caller);
        value
    }

    /// A call standing alone: its body runs, and a value it returns is dropped.
    fn call_stmt(&mut self, name: &str, args: &[Expr], span: Span, out: &mut Vec<Stmt>) -> Result<(), InlineError> {
        let bound = self.lookup(name).cloned().expect("a callee holds a function");
        if self.recurs(name, &bound) {
            let value = self.tail_recursive_value(name, &bound, span, args, out)?;
            out.push(Stmt::Expr(value));
            return Ok(());
        }
        let (body, caller) = self.enter(name, &bound, args, span, out)?;
        let leading = body.split_last().map_or(&body[..], |(_, leading)| leading);
        let ran = if !body.iter().any(contains_return) {
            body.iter().try_for_each(|stmt| self.stmt(stmt, out))
        } else if !leading.iter().any(contains_return) && tail_shaped(&body) {
            let ret = bound.function.ret.as_ref();
            self.body_value(name, &body, ret, span, out)
                .map(|value| out.push(Stmt::Expr(value)))
        } else {
            let body = leave_at_return(&body, &mut |e| vec![Stmt::Expr(e.clone())]);
            self.block(&body).map(|body| out.push(Stmt::Body(body)))
        };
        self.leave(caller);
        ran
    }

    /// Binds `args` to the parameters of `bound`, each evaluated once and in order in the caller's scope, then enters
    /// its body's scope: the scopes it was written in, and its parameters'. A function given as an argument binds its
    /// parameter; any other value is a local of the parameter's type, where one is written. Gives the body, its locals
    /// renamed so two calls never collide, and the caller's scopes, which `leave` restores.
    fn enter(
        &mut self,
        name: &str,
        bound: &Bound,
        args: &[Expr],
        span: Span,
        out: &mut Vec<Stmt>,
    ) -> Result<(Vec<Stmt>, Vec<Scope>), InlineError> {
        let f = Rc::clone(&bound.function);
        if self.active.len() >= MAX_INLINE_DEPTH {
            return Err(InlineError {
                span,
                message: format!(
                    "calls nest more than {MAX_INLINE_DEPTH} deep at `{name}`; inlining \
                     multiplies, so this would expand without bound"
                ),
            });
        }
        self.inlined += 1;
        if self.inlined > MAX_INLINED_CALLS {
            return Err(InlineError {
                span,
                message: format!("more than 10,000 calls inlined at `{name}`"),
            });
        }
        if args.len() != f.params.len() {
            return Err(InlineError {
                span,
                message: format!("`{name}` takes {} argument(s), found {}", f.params.len(), args.len()),
            });
        }

        let mut params = Scope::new();
        let mut renames: HashMap<String, String> = HashMap::new();
        let mut key = bound.id.clone();
        for ((param, ty), arg) in f.params.iter().zip(&f.param_types).zip(args) {
            if let Some(value) = self.function_argument(name, param, ty.as_ref(), arg)? {
                key = format!("{key} {}", value.id);
                params.insert(param.clone(), value);
                continue;
            }
            let arg = self.expr(arg, out)?;
            // A local the inliner made is one nothing in this call can write, unless an arrow written beside it does: a
            // parameter the body leaves alone is it.
            if let (None, ExprKind::Field(n)) = (ty, &arg.kind)
                && is_temporary(n)
                && !self.arrow_written.contains(n)
                && !writes(&f.body, param)
            {
                renames.insert(param.clone(), n.clone());
                continue;
            }
            let local = self.user_local(param, &f.body);
            out.push(Stmt::Let {
                name: local.clone(),
                value: check_optional(ty.as_ref(), arg, argument(name, param)),
            });
            renames.insert(param.clone(), local);
        }
        // The same function, given the same functions, inside itself.
        if self.active.iter().any(|(k, _)| *k == key) {
            let chain: Vec<&str> = self.active.iter().map(|(_, n)| n.as_str()).chain([name]).collect();
            return Err(InlineError {
                span,
                message: format!(
                    "`{name}` calls itself ({}) -- recursion has no bounded expansion, so it \
                     cannot be inlined",
                    chain.join(" -> ")
                ),
            });
        }

        let scopes = match &bound.scopes {
            Some(scopes) => (**scopes).clone(),
            None => vec![self.scopes[0].clone()],
        };
        let holds = |n: &str| params.contains_key(n) || scopes.iter().any(|s| s.contains_key(n));
        let mut body = f.body.clone();
        let locals = self.rename_body(&mut body, &renames, &holds);
        if let Some(bad) = declared_local(&f.body, &|n| renames.contains_key(n) || locals.contains(n)) {
            let what = if f.params.iter().any(|p| p == tree::head(bad)) {
                "a parameter"
            } else {
                "a local"
            };
            return Err(InlineError {
                span,
                message: format!("`{bad}` is {what} of `{name}`: its type is its signature's or its value's"),
            });
        }
        if let Some(ret) = &f.ret {
            check_returns(&mut body, ret, name)?;
        }
        let caller = std::mem::replace(&mut self.scopes, scopes);
        self.scopes.push(params);
        self.active.push((key, name.to_string()));
        Ok((body, caller))
    }

    /// The function `arg` is, for `name`'s parameter `param` of type `ty`, or `None` where it is a value. A typed
    /// parameter takes a value.
    fn function_argument(
        &mut self,
        name: &str,
        param: &str,
        ty: Option<&DataType>,
        arg: &Expr,
    ) -> Result<Option<Bound>, InlineError> {
        let value = self.function_value(arg)?;
        if value.is_some() && ty.is_some() {
            return Err(InlineError {
                span: arg.span,
                message: format!("argument `{param}` of `{name}` has a type, so it takes a value, not a function"),
            });
        }
        Ok(value)
    }

    /// `body` with each local of a function renamed, its parameters by `params`: an arrow passed in reads its own
    /// names, which a local or a loop variable of the body must not shadow. A `let` of a function, an arrow or a name
    /// `holds` or such a `let` bound, names a function, and holds no value to rename. The user's names of the locals
    /// renamed.
    fn rename_body(
        &mut self,
        body: &mut [Stmt],
        params: &HashMap<String, String>,
        holds: &impl Fn(&str) -> bool,
    ) -> HashSet<String> {
        let whole = body.to_vec();
        let (mut functions, mut locals) = (HashSet::new(), HashSet::new());
        rename(body, params, &mut |name, value| {
            let function = match value.map(|v| &v.kind) {
                Some(ExprKind::Arrow(_)) => true,
                Some(ExprKind::Field(n)) => functions.contains(n) || holds(n),
                _ => false,
            };
            if function {
                functions.insert(name.to_string());
                return None;
            }
            locals.insert(name.to_string());
            Some(self.user_local(name, &whole))
        });
        locals
    }

    /// Back in the caller's scopes, after `enter`.
    fn leave(&mut self, caller: Vec<Scope>) {
        self.active.pop();
        self.scopes = caller;
    }

    /// A self-recursive function, compiled as a loop.
    ///
    /// Every self-call must be the whole of a `return`. The call site binds the arguments to locals, then loops until a
    /// non-tail `return` sets `done`/`ret`; each tail call computes its arguments into temporaries before rebinding, so
    /// `f(b, a)` swaps rather than clobbers. A parameter every tail call passes through unchanged is bound once outside
    /// the loop, which is what lets an `Array` parameter work.
    fn tail_recursive_value(
        &mut self,
        name: &str,
        bound: &Bound,
        span: Span,
        args: &[Expr],
        out: &mut Vec<Stmt>,
    ) -> Result<Expr, InlineError> {
        let Function {
            params,
            param_types,
            ret: ret_ty,
            body,
        } = (*bound.function).clone();
        if args.len() != params.len() {
            return Err(InlineError {
                span,
                message: format!("`{name}` takes {} argument(s), found {}", params.len(), args.len()),
            });
        }
        if let Some(bad) = non_tail_self_call(&body, name) {
            return Err(InlineError {
                span: bad,
                message: format!(
                    "`{name}` calls itself somewhere other than in tail position. A recursive \
                     function here is compiled as a loop, which only works when the call is \
                     the whole of a `return` -- `return {name}(...)`, not `return {name}(...) \
                     + 1` and not inside a ternary (both of whose branches evaluate). Rewrite \
                     it with an accumulator parameter: pass the pending work forward instead \
                     of doing it on the way back"
                ),
            });
        }
        must_return(name, &body, span)?;

        let invariant: Vec<bool> = (0..params.len())
            .map(|i| {
                tail_calls(&body, name)
                    .iter()
                    .all(|call_args| matches!(&call_args[i].kind, ExprKind::Field(f) if *f == params[i]))
            })
            .collect();

        let locals: Vec<String> = params.iter().map(|p| self.user_local(p, &body)).collect();
        // A function is no value a trip can rebind: it is bound once, so every tail call passes it unchanged.
        let mut functions = Scope::new();
        for (idx, (((param, local), arg), ty)) in params.iter().zip(&locals).zip(args).zip(&param_types).enumerate() {
            if let Some(value) = self.function_argument(name, param, ty.as_ref(), arg)? {
                if !invariant[idx] {
                    return Err(InlineError {
                        span: arg.span,
                        message: format!("`{param}` is a function, so `{name}` must pass it to itself unchanged"),
                    });
                }
                functions.insert(param.clone(), value);
                continue;
            }
            let arg = self.expr(arg, out)?;
            out.push(Stmt::Let {
                name: local.clone(),
                value: check_optional(ty.as_ref(), arg, argument(name, param)),
            });
        }
        let renames: HashMap<String, String> = params
            .iter()
            .zip(&locals)
            .filter(|(p, _)| !functions.contains_key(*p))
            .map(|(p, l)| (p.clone(), l.clone()))
            .collect();
        if let Some(bad) = declared_local(&body, &|n| renames.contains_key(n)) {
            return Err(InlineError {
                span,
                message: format!("`{bad}` is a parameter of `{name}`: its type is in the signature"),
            });
        }
        let scopes = match &bound.scopes {
            Some(scopes) => (**scopes).clone(),
            None => vec![self.scopes[0].clone()],
        };
        let holds = |n: &str| functions.contains_key(n) || scopes.iter().any(|s| s.contains_key(n));
        let mut body = body;
        self.rename_body(&mut body, &renames, &holds);

        let done = self.tmp("done");
        let ret = self.tmp("ret");
        out.push(Stmt::Let {
            name: done.clone(),
            value: lit_bool(false, span),
        });
        out.push(Stmt::Let {
            name: ret.clone(),
            value: seed(ret_ty.as_ref(), span),
        });

        // A tail call computes its arguments into temporaries before rebinding, and leaves the body for the next trip;
        // any other `return` sets the result and ends the loop too.
        let body = leave_at_return(&body, &mut |e| {
            let mut written = Vec::new();
            match &e.kind {
                ExprKind::Call { name: n, args } if n == name => {
                    let mut temps = Vec::with_capacity(args.len());
                    for (idx, arg) in args.iter().enumerate() {
                        if invariant[idx] {
                            temps.push(None);
                            continue;
                        }
                        // A tail call's argument meets its parameter as the first call's does.
                        let site = argument(name, &params[idx]);
                        let t = self.tmp("arg");
                        written.push(Stmt::Let {
                            name: t.clone(),
                            value: check_optional(param_types[idx].as_ref(), arg.clone(), site),
                        });
                        temps.push(Some(t));
                    }
                    for (local, t) in locals.iter().zip(temps) {
                        let Some(t) = t else { continue };
                        written.push(Stmt::Assign {
                            target: local.clone(),
                            value: Expr {
                                kind: ExprKind::Field(t),
                                span,
                            },
                            typed: false,
                        });
                    }
                }
                _ => {
                    written.push(Stmt::Assign {
                        target: ret.clone(),
                        value: e.clone(),
                        typed: false,
                    });
                    written.push(assign_true(&done, span));
                }
            }
            written
        });
        let caller = std::mem::replace(&mut self.scopes, scopes);
        self.scopes.push(functions);
        self.active.push((name.to_string(), name.to_string()));
        let r = self.block(&body);
        self.leave(caller);
        let mut body = r?;
        if let Some(ret_ty) = &ret_ty {
            check_assigned(&mut body, &ret, ret_ty, name)?;
        }

        out.push(Stmt::While {
            cond: not_expr(&done, span),
            body: vec![Stmt::Body(body)],
            next: Vec::new(),
        });
        Ok(Expr {
            kind: ExprKind::Field(ret),
            span,
        })
    }

    /// The value a body returns, hoisting its statements onto `out`. A trailing `return`, or an `if`/`else` whose
    /// branches both return (folded to a ternary), needs no exit tracking.
    fn body_value(
        &mut self,
        name: &str,
        body: &[Stmt],
        ret: Option<&DataType>,
        span: Span,
        out: &mut Vec<Stmt>,
    ) -> Result<Expr, InlineError> {
        let Some((last, leading)) = body.split_last() else {
            return Err(InlineError {
                span,
                message: format!("`{name}` has an empty body, so a call to it has no value"),
            });
        };

        if leading.iter().any(contains_return) || !tail_shaped(body) {
            return self.early_return_value(name, body, ret, span, out);
        }

        for stmt in leading {
            self.stmt(stmt, out)?;
        }

        match last {
            Stmt::Return(e) => self.expr(e, out),
            Stmt::If { cond, then, else_ } if !else_.is_empty() => {
                let cond = self.expr(cond, out)?;
                let then = self.body_value(name, then, ret, span, out)?;
                let else_ = self.body_value(name, else_, ret, span, out)?;
                Ok(Expr {
                    kind: ExprKind::Ternary {
                        cond: Box::new(cond),
                        then: Box::new(then),
                        else_: Box::new(else_),
                    },
                    span,
                })
            }
            _ => Err(InlineError {
                span,
                message: format!(
                    "`{name}` must end in `return <expr>;` (or an `if`/`else` where both \
                     branches do) -- a call has to have a value"
                ),
            }),
        }
    }

    /// A body with an early `return`: a result local each `return` writes before it leaves the `Body`.
    fn early_return_value(
        &mut self,
        name: &str,
        body: &[Stmt],
        ret_ty: Option<&DataType>,
        span: Span,
        out: &mut Vec<Stmt>,
    ) -> Result<Expr, InlineError> {
        must_return(name, body, span)?;

        let ret = self.tmp("ret");
        // `let`-declared, so a local: absent from output and from the internal-fields view. An object
        // literal bound to a name is its paths, so a function returning one seeds the union of every return's
        // leaves as nulls, and each guarded return then writes into them; a null seed would be the value read back.
        // The result is seeded null, typed as the function declares, so every guarded return blends into that type.
        let returns = return_exprs(body);
        let objects = returns.iter().filter(|e| matches!(e.kind, ExprKind::Object(_))).count();
        let seed = if objects == 0 {
            seed(ret_ty, span)
        } else if objects == returns.len() {
            null_shape(&returns, ret_ty.unwrap_or(&DataType::Null), span)
        } else {
            return Err(InlineError {
                span,
                message: format!(
                    "`{name}` returns an object literal from one `return` and something else from another; with \
                     an early return every `return` must be an object literal, or none"
                ),
            });
        };
        out.push(Stmt::Let {
            name: ret.clone(),
            value: seed,
        });
        let body = leave_at_return(body, &mut |e| {
            vec![Stmt::Assign {
                target: ret.clone(),
                value: e.clone(),
                typed: false,
            }]
        });
        out.push(Stmt::Body(self.block(&body)?));
        Ok(Expr {
            kind: ExprKind::Field(ret),
            span,
        })
    }

    /// Emits a `while`. A condition that hoists statements is computed into a flag, before the loop and again in
    /// `next`: `while (f(x)) { B }` becomes `<f(x)>; let c = ...; while (c) { B }` with `next` `<f(x)>; c = ...`.
    fn emit_while(&mut self, cond: &Expr, body: Vec<Stmt>, out: &mut Vec<Stmt>) -> Result<(), InlineError> {
        let span = cond.span;
        let mut head = Vec::new();
        let first = self.expr(cond, &mut head)?;
        if head.is_empty() {
            out.push(Stmt::While {
                cond: first,
                body,
                next: Vec::new(),
            });
            return Ok(());
        }
        let flag = self.tmp("cond");
        out.extend(head);
        out.push(Stmt::Let {
            name: flag.clone(),
            value: first,
        });
        let mut next = Vec::new();
        let again = self.expr(cond, &mut next)?;
        next.push(Stmt::Assign {
            target: flag.clone(),
            value: again,
            typed: false,
        });
        out.push(Stmt::While {
            cond: Expr::new(span, ExprKind::Field(flag)),
            body,
            next,
        });
        Ok(())
    }
}

fn assigns_anything(stmts: &[Stmt]) -> bool {
    Stmt::any(stmts, &|s| !matches!(s, Stmt::Function { .. }), &mut |s| {
        matches!(s, Stmt::Assign { .. } | Stmt::IndexAssign { .. })
    })
}

/// `stmts` with each `return e` replaced by what `write` gives for `e`, then a leave of the function's body. A nested
/// `function` is its own.
fn leave_at_return(stmts: &[Stmt], write: &mut impl FnMut(&Expr) -> Vec<Stmt>) -> Vec<Stmt> {
    let mut out = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        match stmt {
            Stmt::Return(e) => {
                out.extend(write(e));
                out.push(Stmt::Leave {
                    to: Region::Body,
                    span: e.span,
                });
            }
            Stmt::Function { .. } => out.push(stmt.clone()),
            _ => {
                let mut stmt = stmt.clone();
                for b in stmt.blocks_mut() {
                    *b = leave_at_return(b, write);
                }
                out.push(stmt);
            }
        }
    }
    out
}

fn assign_true(name: &str, span: Span) -> Stmt {
    Stmt::Assign {
        target: name.to_string(),
        value: lit_bool(true, span),
        typed: false,
    }
}

/// Every `return`'s expression in `body`, in source order.
fn return_exprs(body: &[Stmt]) -> Vec<&Expr> {
    let mut out = Vec::new();
    Stmt::find_map::<()>(body, &|s| !matches!(s, Stmt::Function { .. }), &mut |s| {
        if let Stmt::Return(e) = s {
            out.push(e);
        }
        None
    });
    out
}

/// An object literal holding every leaf path the literals in `shapes` hold, each leaf `null`. A key that is an object
/// in one shape and a scalar in another is an object: the leaves under it are what a return writes. A spread's keys
/// are only known where the call compiles, so it seeds nothing.
fn null_shape(shapes: &[&Expr], ty: &DataType, span: Span) -> Expr {
    // Every return was checked against `ty`, so each key is a field it names; a typed null gives the leaf its type.
    let field_ty = |k: &str| match ty {
        DataType::Struct(fields, _) => fields.iter().find(|(n, _)| tree::spell(n) == k).map(|(_, t)| t.clone()),
        _ => None,
    };
    let mut keys: Vec<String> = Vec::new();
    for shape in shapes {
        if let ExprKind::Object(pairs) = &shape.kind {
            for k in pairs.iter().filter_map(|(k, _)| k.as_ref()) {
                if !keys.contains(k) {
                    keys.push(k.clone());
                }
            }
        }
    }
    let fields = keys
        .into_iter()
        .map(|k| {
            let nested: Vec<&Expr> = shapes
                .iter()
                .filter_map(|shape| match &shape.kind {
                    ExprKind::Object(pairs) => pairs.iter().find(|(n, _)| n.as_ref() == Some(&k)).map(|(_, v)| v),
                    _ => None,
                })
                .filter(|v| matches!(v.kind, ExprKind::Object(_)))
                .collect();
            let value = match (nested.is_empty(), field_ty(&k)) {
                (true, Some(t)) => typed_null(&t, span),
                (true, None) => Expr {
                    kind: ExprKind::Literal(Value::Null),
                    span,
                },
                (false, t) => null_shape(&nested, &t.unwrap_or(DataType::Null), span),
            };
            (Some(k), value)
        })
        .collect();
    Expr {
        kind: ExprKind::Object(fields),
        span,
    }
}

fn lit_bool(v: bool, span: Span) -> Expr {
    Expr {
        kind: ExprKind::Literal(Value::Bool(v)),
        span,
    }
}

fn tail_shaped(body: &[Stmt]) -> bool {
    match body.split_last() {
        Some((Stmt::Return(_), _)) => true,
        Some((Stmt::If { then, else_, .. }, _)) if !else_.is_empty() => tail_shaped(then) && tail_shaped(else_),
        _ => false,
    }
}

/// Whether every path through `stmts` reaches a `return`.
/// Refuses a body with a path that reaches its end without returning: a call must have a value on every row.
fn must_return(name: &str, body: &[Stmt], span: Span) -> Result<(), InlineError> {
    if always_returns(body) {
        return Ok(());
    }
    let message = if body.iter().any(contains_return) {
        format!(
            "`{name}` has a path that reaches the end without returning, so a call \
             to it would have no value on that path -- every branch must return \
             (an `if` with no `else` leaves the fall-through open)"
        )
    } else {
        format!(
            "`{name}` must end in `return <expr>;` (or an `if`/`else` where both \
             branches do) -- a call has to have a value"
        )
    };
    Err(InlineError { span, message })
}

fn always_returns(stmts: &[Stmt]) -> bool {
    stmts.iter().any(|s| match s {
        Stmt::Return(_) => true,
        Stmt::If { then, else_, .. } => !else_.is_empty() && always_returns(then) && always_returns(else_),
        Stmt::Try { body, catches, finally } => {
            always_returns(finally) || always_returns(body) && catches.iter().all(|c| always_returns(&c.2))
        }
        _ => false,
    })
}

/// The span of the first call of `name` in `e` or inside it.
fn call_of(e: &Expr, name: &str) -> Option<Span> {
    e.find_map(&mut |e| matches!(&e.kind, ExprKind::Call { name: n, .. } if n == name).then_some(e.span))
}

/// Whether `body` calls `name` anywhere.
fn calls_itself(body: &[Stmt], name: &str) -> bool {
    Stmt::any(body, &|_| true, &mut |s| {
        s.exprs().into_iter().any(|e| call_of(e, name).is_some())
    })
}

/// The span of a self-call that is not the whole of a `return`.
fn non_tail_self_call(body: &[Stmt], name: &str) -> Option<Span> {
    Stmt::find_map(body, &|_| true, &mut |s| match s {
        Stmt::Return(Expr {
            kind: ExprKind::Call { name: n, args },
            ..
        }) if n == name => args.iter().find_map(|a| call_of(a, name)),
        s => s.exprs().into_iter().find_map(|e| call_of(e, name)),
    })
}

/// Every tail call's argument list, in source order.
fn tail_calls<'a>(body: &'a [Stmt], name: &str) -> Vec<&'a [Expr]> {
    let mut out = Vec::new();
    Stmt::find_map::<()>(body, &|_| true, &mut |s| {
        if let Stmt::Return(Expr {
            kind: ExprKind::Call { name: n, args },
            ..
        }) = s
            && n == name
        {
            out.push(args.as_slice());
        }
        None
    });
    out
}

fn not_expr(name: &str, span: Span) -> Expr {
    Expr {
        kind: ExprKind::Unary {
            op: UnOp::Not,
            operand: Box::new(Expr {
                kind: ExprKind::Field(name.to_string()),
                span,
            }),
        },
        span,
    }
}

fn contains_return(stmt: &Stmt) -> bool {
    Stmt::any(
        std::slice::from_ref(stmt),
        &|s| !matches!(s, Stmt::Function { .. }),
        &mut |s| matches!(s, Stmt::Return(_)),
    )
}

/// Rename, in `stmts`, each local `map` names, and each name whose head is one. A declaration and a function are left
/// as they are. Each `let` and loop variable takes the name `fresh` gives it, from where it is bound to the end of its
/// block, as the compiler scopes it; one `fresh` leaves as it is hides a same-named local of `map` there.
fn rename(
    stmts: &mut [Stmt],
    map: &HashMap<String, String>,
    fresh: &mut impl FnMut(&str, Option<&Expr>) -> Option<String>,
) {
    let mut map = std::borrow::Cow::Borrowed(map);
    for stmt in stmts {
        let names: Vec<&mut String> = match stmt {
            Stmt::Declare { .. } => continue,
            // A function reads the names where it was written. Its parameters and its own locals are its own, renamed
            // where it is called.
            Stmt::Function { function, .. } => {
                let mut inner = map.clone().into_owned();
                for name in &function.params {
                    inner.remove(name);
                }
                rename(&mut function.body, &inner, &mut own_locals);
                continue;
            }
            Stmt::Let { name, value } => {
                rename_expr(value, &map);
                let renamed = fresh(name, Some(value));
                bind(map.to_mut(), name, renamed);
                continue;
            }
            Stmt::For { var, iterable, body } => {
                rename_expr(iterable, &map);
                let mut inner = map.clone().into_owned();
                let renamed = fresh(var, None);
                bind(&mut inner, var, renamed);
                rename(body, &inner, fresh);
                continue;
            }
            Stmt::Assign { target, .. } | Stmt::IndexAssign { target, .. } => vec![target],
            Stmt::Delete { names, .. } => names.iter_mut().collect(),
            Stmt::Unroll { field, .. } => vec![field],
            // A `catch` variable is a local of its `catch` block, as a loop's variable is of its body.
            Stmt::Try { body, catches, finally } => {
                rename(body, &map, fresh);
                rename(finally, &map, fresh);
                for (var, _, catch) in catches {
                    let mut inner = map.clone().into_owned();
                    if let Some(var) = var {
                        let renamed = fresh(var, None);
                        bind(&mut inner, var, renamed);
                    }
                    rename(catch, &inner, fresh);
                }
                continue;
            }
            _ => Vec::new(),
        };
        for name in names {
            if let Some(renamed) = map.get(name).cloned().or_else(|| rename_head(name, &map)) {
                *name = renamed;
            }
        }
        for e in stmt.exprs_mut() {
            rename_expr(e, &map);
        }
        for b in stmt.blocks_mut() {
            rename(b, &map, fresh);
        }
    }
}

/// A function's or an arrow's own local keeps its name: it is renamed where the function is called.
fn own_locals(_: &str, _: Option<&Expr>) -> Option<String> {
    None
}

/// `name` bound afresh in `map`: as `renamed` where it is one, else as itself, hiding a same-named outer local.
fn bind(map: &mut HashMap<String, String>, name: &mut String, renamed: Option<String>) {
    match renamed {
        Some(renamed) => {
            map.insert(name.clone(), renamed.clone());
            *name = renamed;
        }
        None => _ = map.remove(name.as_str()),
    }
}

/// `name` with a renamed local at its head. The lexer joins a dotted chain into one name, so `p.key` and the receiver
/// of `xs.length()` name the local `p` or `xs` only up to their first dot. The longest renamed prefix wins.
fn rename_head(name: &str, map: &HashMap<String, String>) -> Option<String> {
    let above: Vec<&str> = tree::ancestors(name).collect();
    above
        .into_iter()
        .rev()
        .find_map(|at| map.get(at).map(|local| format!("{local}{}", &name[at.len()..])))
}

fn rename_expr(e: &mut Expr, map: &HashMap<String, String>) {
    match &mut e.kind {
        ExprKind::Field(name) => {
            if let Some(renamed) = map.get(name).cloned().or_else(|| rename_head(name, map)) {
                *name = renamed;
            }
        }
        // Only a receiver: a bare name here is a function, which no local shadows.
        ExprKind::Call { name, .. } => {
            if let Some(renamed) = rename_head(name, map) {
                *name = renamed;
            }
        }
        // An arrow's parameters and its own locals shadow same-named locals of the enclosing body. Its names are its
        // own, renamed where it is called.
        ExprKind::Arrow(f) => {
            let mut inner = map.clone();
            for name in &f.params {
                inner.remove(name);
            }
            return rename(&mut f.body, &inner, &mut |_, _| None);
        }
        _ => {}
    }
    for c in e.children_mut() {
        rename_expr(c, map);
    }
}
