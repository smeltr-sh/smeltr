//! The syntax tree. Purely syntactic: names are strings, patterns are source text, and nothing is typed.
//! `plan::resolve` lowers it to `Ir`.

use crate::core::column::DataType;

pub use crate::ops::{BinOp, UnOp};

/// Byte offsets into the source text.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Span {
        Span {
            start: start as u32,
            end: end as u32,
        }
    }

    pub fn join(self, other: Span) -> Span {
        Span {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    pub fn slice<'a>(&self, src: &'a str) -> &'a str {
        &src[self.start as usize..self.end as usize]
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
/// Every flag here reaches the regex engine. A flag that did nothing was worse than no flag: `/x/y` read as if it
/// meant something. `replaceFirst` and `replaceAll` name what `g` used to decide.
pub struct RegexFlags {
    pub case_insensitive: bool,
    pub multi_line: bool,
    pub dot_matches_newline: bool,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PatternSrc {
    pub source: String,
    pub flags: RegexFlags,
}

/// Where a value meets a declared type.
#[derive(Clone, Debug)]
pub enum Site {
    /// Parameter `param` of `function`.
    Argument { function: String, param: String },
    /// A return, or a field of one, named as an error names it: "the return of `f`".
    Return(String),
    /// The typed declaration of `name`, `name: type = e` or `let name: type = e`. A variant converts here.
    Declaration(String),
}

impl std::fmt::Display for Site {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Site::Argument { function, param } => write!(f, "argument `{param}` of `{function}`"),
            Site::Return(place) => f.write_str(place),
            Site::Declaration(name) => write!(f, "`{name}`"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Expr {
    pub span: Span,
    pub kind: ExprKind,
}

impl Expr {
    pub fn new(span: Span, kind: ExprKind) -> Expr {
        Expr { span, kind }
    }

    /// The expressions directly inside this one, in source order: the order a rewrite hoists their statements in. An
    /// arrow's body is statements of its own, so it has none.
    pub fn children(&self) -> Vec<&Expr> {
        match &self.kind {
            ExprKind::Literal(_) | ExprKind::Field(_) | ExprKind::Regex(_) | ExprKind::Arrow(_) => Vec::new(),
            ExprKind::Binary { lhs, rhs, .. } => vec![lhs, rhs],
            ExprKind::Unary { operand: e, .. }
            | ExprKind::Coerce { value: e, .. }
            | ExprKind::Check { value: e, .. } => vec![e],
            ExprKind::Ternary { cond, then, else_ } => vec![cond, then, else_],
            ExprKind::Call { args: items, .. } | ExprKind::Array(items) => items.iter().collect(),
            ExprKind::Index { base, index } => vec![base, index],
            ExprKind::Object(fields) => fields.iter().map(|(_, v)| v).collect(),
            ExprKind::SetElement { array, index, value } => vec![array, index, value],
        }
    }

    /// `children`, to rewrite in place.
    pub fn children_mut(&mut self) -> Vec<&mut Expr> {
        match &mut self.kind {
            ExprKind::Literal(_) | ExprKind::Field(_) | ExprKind::Regex(_) | ExprKind::Arrow(_) => Vec::new(),
            ExprKind::Binary { lhs, rhs, .. } => vec![lhs, rhs],
            ExprKind::Unary { operand: e, .. }
            | ExprKind::Coerce { value: e, .. }
            | ExprKind::Check { value: e, .. } => vec![e],
            ExprKind::Ternary { cond, then, else_ } => vec![cond, then, else_],
            ExprKind::Call { args: items, .. } | ExprKind::Array(items) => items.iter_mut().collect(),
            ExprKind::Index { base, index } => vec![base, index],
            ExprKind::Object(fields) => fields.iter_mut().map(|(_, v)| v).collect(),
            ExprKind::SetElement { array, index, value } => vec![array, index, value],
        }
    }

    /// The first `Some` `f` gives on this expression or one inside it, outermost first, in source order.
    pub fn find_map<T>(&self, f: &mut impl FnMut(&Expr) -> Option<T>) -> Option<T> {
        f(self).or_else(|| self.children().into_iter().find_map(|c| c.find_map(f)))
    }
}

#[derive(Clone, Debug)]
pub enum ExprKind {
    Literal(Value),
    /// A dotted chain is one identifier (`source.ip`), collapsed by the lexer.
    Field(String),
    Binary {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    Unary {
        op: UnOp,
        operand: Box<Expr>,
    },
    /// Both branches always evaluate; the result is selected per row.
    Ternary {
        cond: Box<Expr>,
        then: Box<Expr>,
        else_: Box<Expr>,
    },
    /// `name(args)`. Method-call syntax desugars to this with the receiver prepended to `args`, except when the
    /// receiver is a bare identifier chain, which the lexer has already collapsed into `name`
    /// (`plan::resolve::resolve_call` splits it).
    Call {
        name: String,
        args: Vec<Expr>,
    },
    Index {
        base: Box<Expr>,
        index: Box<Expr>,
    },
    Array(Vec<Expr>),
    /// Entries in source order. A `None` key is a spread, `...value`.
    Object(Vec<(Option<String>, Expr)>),
    /// A regex literal: always a compile-time constant.
    Regex(PatternSrc),
    /// A function written in place: `x => x * 2`, `(a, b) => { let s = a + b; s * s }`. It is bound with `let`,
    /// passed as an argument and called; `expr::inline` expands every call, so none reaches the compiler.
    Arrow(Function),
    /// `value`, coerced to `ty` by the coercion table: what `name: type = expr` and `let x: type = expr` desugar
    /// to. Built by the parser and the inliner, never spelled.
    Coerce {
        ty: DataType,
        value: Box<Expr>,
    },
    /// `value`, which must already have type `ty`: an argument meeting its parameter, a `return` meeting its
    /// function's return type. `site` is the place, for an error. Built by the inliner, never spelled.
    Check {
        ty: DataType,
        value: Box<Expr>,
        site: Site,
    },
    /// `array` with position `index` set to `value`: what `a[i] = x` desugars to where `a` holds an array. Built by
    /// the compiler, never spelled.
    SetElement {
        array: Box<Expr>,
        index: Box<Expr>,
        value: Box<Expr>,
    },
}

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    DateTime(i64),
    Duration(i64),
    Str(String),
    Bytes(Vec<u8>),
}

impl Value {
    /// `None` for `Null`, which inhabits every type.
    pub fn data_type(&self) -> Option<DataType> {
        Some(match self {
            Value::Null => return None,
            Value::Bool(_) => DataType::Bool,
            Value::I64(_) => DataType::I64,
            Value::F64(_) => DataType::F64,
            Value::DateTime(_) => DataType::DateTime,
            Value::Duration(_) => DataType::Duration,
            Value::Str(_) => DataType::Str,
            Value::Bytes(_) => DataType::Bytes,
        })
    }
}

/// A function: a declared `function`'s or an arrow's. Every call is inlined by `expr::inline`. A parameter or return
/// with a type must have it at every call; one without takes its value's type, as a `let` does.
#[derive(Clone, Debug)]
pub struct Function {
    pub params: Vec<String>,
    pub param_types: Vec<Option<DataType>>,
    pub ret: Option<DataType>,
    pub body: Vec<Stmt>,
}

/// A block a row can leave before its end. A row leaves every region from the innermost out to the one it leaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Region {
    /// An `if` branch or a `catch`: no `Leave` names one, so a row leaves it only on its way out.
    Block,
    /// One trip of a loop: `continue` leaves it.
    Trip,
    /// A whole loop: `break` leaves it.
    Loop,
    /// An inlined function's body: `return` leaves it.
    Body,
    /// A `try` body or a `finally`: an exception leaves the nearest one with a `catch`, and with none, the outermost.
    Try { catches: bool },
}

/// A `catch`: its name, the kinds it takes by position in `ExceptionKind::KINDS` (none for every kind a `try`
/// catches), and its body.
pub type Catch = (Option<String>, Vec<usize>, Vec<Stmt>);

/// A statement in a `Code` block. Structural restrictions (where `return`, `break` and `target[index] = v` may appear)
/// are checked by `expr::inline` and `plan::compile`, not by the parser.
#[derive(Clone, Debug)]
pub enum Stmt {
    /// `typed` for `name: type = value`, the one write that changes a bound name's type.
    Assign {
        target: String,
        value: Expr,
        typed: bool,
    },
    /// A block-scoped local, never a field.
    Let {
        name: String,
        value: Expr,
    },
    /// `name: type;`: the field is declared, and coerces to `ty` where it meets the record.
    Declare {
        name: String,
        ty: DataType,
    },
    Expr(Expr),
    /// `else if` parses as a single nested `If` in `else_`.
    If {
        cond: Expr,
        then: Vec<Stmt>,
        else_: Vec<Stmt>,
    },
    /// `for (let var of iterable) { body }`: unrolled for a literal array, a loop over a real `Array` field otherwise.
    For {
        var: String,
        iterable: Expr,
        body: Vec<Stmt>,
    },
    /// `while (cond) { body }`, run as `if (cond)` and then trips, each followed by `next` and a test of `cond`: a
    /// row the test fails takes no further trip. `next` is outside the trip, so a `continue` runs it too; it is where
    /// `expr::inline` computes a condition that calls a function.
    While {
        cond: Expr,
        body: Vec<Stmt>,
        next: Vec<Stmt>,
    },
    /// `break` leaves the loop, `continue` the trip, and an inlined `return` the function's body, on the rows it runs
    /// on. Carries its span so one with no region to leave is reported where written.
    Leave {
        to: Region,
        span: Span,
    },
    /// An inlined function's body, the region its `return`s leave.
    Body(Vec<Stmt>),
    /// `target[index] = value;`. With a literal string key this is sugar for `target.key = value`; otherwise it is the
    /// struct-build shape.
    IndexAssign {
        target: String,
        index: Expr,
        value: Expr,
    },
    /// `function name(...) { ... }`: a function bound to `name`, hoisted.
    Function {
        name: String,
        function: Function,
    },
    Return(Expr),
    /// `delete(a, b.c)`: each name leaves the tree, as a declaration binds it. `moved` marks `move`'s own delete,
    /// which clears a bound name on the rows an `if` selects where a written `delete` is refused.
    Delete {
        names: Vec<String>,
        moved: bool,
        span: Span,
    },
    /// `drop()`: the rows it runs on are dead.
    Drop(Span),
    /// `unroll(field, keep)`: one row per element of `field`, which becomes the element. The one row barrier.
    Unroll {
        field: String,
        keep: bool,
        span: Span,
    },
    /// `try { body } catch (var) { catch } ... finally { finally }`: the first exception on a row skips the rest of
    /// `body` there; each `catch` in turn runs on the rows holding a kind it takes, with `var.kind` the first such kind
    /// and `var.message` their text, and clears those; the row's other exceptions go on to the next `catch`, and past the
    /// last as if uncaught. `finally` runs on every row. A `try` with no `catch` leaves its exceptions on the row.
    Try {
        body: Vec<Stmt>,
        catches: Vec<Catch>,
        finally: Vec<Stmt>,
    },
    /// `throw e`, `e` a name a `catch` binds: the rows it runs on take back the exceptions that `catch` took.
    Throw(Expr),
}

impl Stmt {
    /// The expressions this statement holds itself, outside its blocks, in source order.
    pub fn exprs(&self) -> Vec<&Expr> {
        match self {
            Stmt::Assign { value: e, .. }
            | Stmt::Let { value: e, .. }
            | Stmt::Expr(e)
            | Stmt::Return(e)
            | Stmt::If { cond: e, .. }
            | Stmt::For { iterable: e, .. }
            | Stmt::While { cond: e, .. }
            | Stmt::Throw(e) => vec![e],
            Stmt::IndexAssign { index, value, .. } => vec![index, value],
            Stmt::Declare { .. }
            | Stmt::Leave { .. }
            | Stmt::Body(_)
            | Stmt::Function { .. }
            | Stmt::Delete { .. }
            | Stmt::Drop(_)
            | Stmt::Unroll { .. }
            | Stmt::Try { .. } => Vec::new(),
        }
    }

    /// `exprs`, to rewrite in place.
    pub fn exprs_mut(&mut self) -> Vec<&mut Expr> {
        match self {
            Stmt::Assign { value: e, .. }
            | Stmt::Let { value: e, .. }
            | Stmt::Expr(e)
            | Stmt::Return(e)
            | Stmt::If { cond: e, .. }
            | Stmt::For { iterable: e, .. }
            | Stmt::While { cond: e, .. }
            | Stmt::Throw(e) => vec![e],
            Stmt::IndexAssign { index, value, .. } => vec![index, value],
            Stmt::Declare { .. }
            | Stmt::Leave { .. }
            | Stmt::Body(_)
            | Stmt::Function { .. }
            | Stmt::Delete { .. }
            | Stmt::Drop(_)
            | Stmt::Unroll { .. }
            | Stmt::Try { .. } => Vec::new(),
        }
    }

    /// The blocks nested in this statement: an `if`'s two branches, a loop's body and a `while`'s `next`, a
    /// function's body.
    pub fn blocks(&self) -> Vec<&[Stmt]> {
        match self {
            Stmt::If { then, else_, .. } => vec![then, else_],
            Stmt::Try { body, catches, finally } => (std::iter::once(body).chain(catches.iter().map(|c| &c.2)))
                .chain([finally])
                .map(Vec::as_slice)
                .collect(),
            Stmt::While { body, next, .. } => vec![body, next],
            Stmt::For { body, .. }
            | Stmt::Function {
                function: Function { body, .. },
                ..
            }
            | Stmt::Body(body) => vec![body],
            _ => Vec::new(),
        }
    }

    /// `blocks`, to rewrite in place.
    pub fn blocks_mut(&mut self) -> Vec<&mut Vec<Stmt>> {
        match self {
            Stmt::If { then, else_, .. } => vec![then, else_],
            Stmt::Try { body, catches, finally } => (std::iter::once(body).chain(catches.iter_mut().map(|c| &mut c.2)))
                .chain([finally])
                .collect(),
            Stmt::While { body, next, .. } => vec![body, next],
            Stmt::For { body, .. }
            | Stmt::Function {
                function: Function { body, .. },
                ..
            }
            | Stmt::Body(body) => vec![body],
            _ => Vec::new(),
        }
    }

    /// The first `Some` `f` gives on a statement in `stmts` or in a block nested in one, in source order, entering the
    /// blocks only of a statement `enter` accepts.
    pub fn find_map<'s, T>(
        stmts: &'s [Stmt],
        enter: &impl Fn(&Stmt) -> bool,
        f: &mut impl FnMut(&'s Stmt) -> Option<T>,
    ) -> Option<T> {
        stmts.iter().find_map(|s| {
            f(s).or_else(|| {
                if !enter(s) {
                    return None;
                }
                s.blocks().into_iter().find_map(|b| Stmt::find_map(b, enter, f))
            })
        })
    }

    /// Whether `f` holds for a statement `find_map` would visit.
    pub fn any(stmts: &[Stmt], enter: &impl Fn(&Stmt) -> bool, f: &mut impl FnMut(&Stmt) -> bool) -> bool {
        Stmt::find_map(stmts, enter, &mut |s| f(s).then_some(())).is_some()
    }
}
