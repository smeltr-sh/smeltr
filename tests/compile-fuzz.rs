//! Generated pipelines against the compiler. Each seed writes a program of steps over a parsed record -- code, eval,
//! drop, rename, unroll, each parser, lookup, a step's filter and an `if` step -- typed as it is written so most
//! compile, and holds four properties that need no expected output:
//!
//! 1. Compiling it gives a plan or compile errors, and never panics.
//! 2. Each record comes out of a batch of all of them as it comes out alone.
//! 3. The program rewritten to mean the same thing with a different layout -- its locals renamed, each write taken
//!    through a `let`, a dead `let` before each statement, each step split where no local crosses, each call written
//!    as a method, each step written as the code it lowers to -- gives the same records. A name resolved to the wrong
//!    slot breaks this, where no assertion on the resolution path can see it.
//! 4. The program broken in one place -- a write of another type, an operator on an operand it does not take, a
//!    condition that is not `bool`, a local declared twice in its block -- is refused.
//!
//! The generator asks the engine what it accepts: a call is a row of `stdfuncs::ROWS` and an operator a pair of
//! operand types `binop_result` types. Over the seed range every row, every operator, every step kind and every
//! construct the generator knows is written into a program that compiles, or the test fails naming what was missed.
//!
//! A seeded fuzzer on `harness::fuzz`: a failure prints its seed and both programs.

use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use serde_json::Value;
use smeltr::core::buffer::BufferPool;
use smeltr::core::column::DataType;
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ErrorSink;
use smeltr::ops::stdfuncs::{Arg, Param, ROWS, StdFunc};
use smeltr::ops::{BinOp, UnOp};
use smeltr::plan::config::{
    CsvConfig, DeclaredField, FunctionConfig, FunctionKind, GrokConfig, KvConfig, LookupOutput, LookupSource,
    MatchKind, Multiplicity, ParserKind, PipelineConfig, Step,
};
use smeltr::plan::output::run_to_json;
use smeltr::plan::plan::{Plan, compile};
use smeltr::plan::resolve::{binop_result, unop_result};
use smeltr::plan::topology::SourceMeta;

use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::{code, pipeline, raw_batch};

const SEEDS: u64 = 300;

/// The record's declarations and its parse, step 0 of every program.
const HEAD: &str = "a: i64; b: i64; f: f64; s: str; u: str; bo: bool; by: bytes; t: datetime; d: duration; \
                    xs: array<i64>; ys: array<str>; fs: array<f64>; o: {p: i64, q: str, ...}; \
                    Object.assign($, json(_raw));";

/// Records that differ in which keys they hold, nulls, empty values and an `i64` that overflows when doubled.
const DOCS: &[&str] = &[
    r#"{"a": 1, "b": 2, "f": 1.5, "s": "Ab", "u": "x", "bo": true, "by": "aGk=", "t": "2024-01-02T03:04:05Z", "d": 5000000, "xs": [1, 2, 3], "ys": ["a", "b"], "fs": [0.5], "o": {"p": 5, "q": "q", "r": 1}, "zz": "z"}"#,
    r#"{"a": -7, "b": 0, "f": -0.0, "s": "", "u": "a,b", "bo": false, "by": "", "t": 0, "d": -1, "xs": [], "ys": [], "fs": [], "o": {"p": 0}}"#,
    r#"{"a": 4611686018427387904, "b": 3, "f": 1e300, "s": " pad ", "t": "1999-12-31T23:59:59.999Z", "xs": [9], "ys": ["10.0.0.1"], "zz": [1]}"#,
    r#"{}"#,
    r#"{"a": null, "s": "ab,c", "u": "", "xs": [0, -1], "fs": [1, 2.5], "o": {"q": "z", "r": {"k": 1}}}"#,
    r#"{"b": 10, "u": "Ab", "f": 3, "o": {}, "zz": null}"#,
];

// -- what the engine accepts ----------------------------------------------------

/// The scalar types.
fn scalars() -> Vec<DataType> {
    use DataType::*;
    vec![Bool, I64, F64, Str, Bytes, DateTime, Duration]
}

/// The types a call or an operator is tried at: every scalar, an array of each, and an array of arrays.
fn universe() -> Vec<DataType> {
    let mut out = scalars();
    out.extend(scalars().into_iter().map(|t| DataType::Array(Box::new(t))));
    out.push(DataType::Array(Box::new(DataType::Array(Box::new(DataType::I64)))));
    out
}

fn arr(t: DataType) -> DataType {
    DataType::Array(Box::new(t))
}

/// A call the engine types: a row of `ROWS` at argument types it accepts, `None` for a regex.
struct Inst {
    row: usize,
    args: Vec<Option<DataType>>,
    ret: DataType,
}

struct Typed<Op> {
    op: Op,
    args: Vec<DataType>,
    ret: DataType,
}

/// Every call and operator the generator writes, at every type of `universe` the engine gives a type for.
struct Index {
    calls: Vec<Inst>,
    bins: Vec<Typed<BinOp>>,
    uns: Vec<Typed<UnOp>>,
}

fn index() -> &'static Index {
    static INDEX: OnceLock<Index> = OnceLock::new();
    INDEX.get_or_init(|| {
        let some = |v: Vec<DataType>| v.into_iter().map(Some).collect::<Vec<_>>();
        let arrays = || universe().into_iter().filter(|t| matches!(t, DataType::Array(_)));
        let candidates = |p: Param| -> Vec<Option<DataType>> {
            use DataType as T;
            match p {
                Param::Pattern => vec![None],
                Param::Any | Param::Element => some(universe()),
                Param::Data => some(vec![T::Str, T::Bytes]),
                Param::Str => some(vec![T::Str]),
                Param::Bytes => some(vec![T::Bytes]),
                Param::I64 => some(vec![T::I64]),
                // An `i64` widens to the `f64` the parameter takes.
                Param::F64 | Param::Num => some(vec![T::I64, T::F64]),
                Param::Bool => some(vec![T::Bool]),
                Param::DateTime => some(vec![T::DateTime]),
                Param::Duration => some(vec![T::Duration]),
                Param::Sized => some([T::Str, T::Bytes].into_iter().chain(arrays()).collect()),
                Param::Array | Param::List => some(arrays().collect()),
                Param::Printable => some(scalars()),
            }
        };
        let mut calls = Vec::new();
        for (ri, row) in ROWS.iter().enumerate() {
            // `now()` is the time a run reads it, which no two runs share: it is written only as `year(now())`.
            if row.func == StdFunc::Now {
                continue;
            }
            let mut params = row.params.to_vec();
            let mut arities = vec![params.len()];
            if row.func.is_variadic()
                && let Some(&last) = params.last()
            {
                params.push(last);
                arities.push(params.len());
            }
            for n in arities {
                let mut combos: Vec<Vec<Option<DataType>>> = vec![Vec::new()];
                for &p in &params[..n] {
                    combos = combos
                        .into_iter()
                        .flat_map(|c| {
                            candidates(p).into_iter().map(move |t| {
                                let mut c = c.clone();
                                c.push(t);
                                c
                            })
                        })
                        .collect();
                }
                for args in combos {
                    let typed: Vec<Arg> = args.iter().map(|a| a.clone().map_or(Arg::Pattern, Arg::Ty)).collect();
                    let this = row.func.row_for(&typed).is_some_and(|r| std::ptr::eq(r, row));
                    match row.func.result_type(&typed) {
                        Some(ret) if this && !matches!(ret, DataType::Null | DataType::Variant) => {
                            calls.push(Inst { row: ri, args, ret })
                        }
                        _ => {}
                    }
                }
            }
        }
        let mut bins = Vec::new();
        for op in BinOp::ALL {
            for l in universe() {
                for r in universe() {
                    if let Ok(ret) = binop_result(op, &l, &r)
                        && !matches!(ret, DataType::Null | DataType::Variant)
                    {
                        bins.push(Typed {
                            op,
                            args: vec![l.clone(), r],
                            ret,
                        });
                    }
                }
            }
        }
        let mut uns = Vec::new();
        for op in UnOp::ALL {
            for t in universe() {
                if let Ok(ret) = unop_result(op, &t)
                    && !matches!(ret, DataType::Null | DataType::Variant)
                {
                    uns.push(Typed { op, args: vec![t], ret });
                }
            }
        }
        Index { calls, bins, uns }
    })
}

/// Literal arguments a call means something with, by function and position: a form, a unit, a format, an address.
fn hint(f: StdFunc, i: usize) -> &'static [&'static str] {
    use StdFunc::*;
    match (f, i) {
        (ToString | ParseBytes, 1) => &["\"base64\"", "\"hex\"", "\"binary\""],
        (Decode | Encode, 1) => &["\"utf-8\"", "\"latin1\"", "\"utf-16le\"", "\"nope\""],
        (ParseInt, 1) => &["2", "10", "16", "36"],
        (ParseTime, 1) => &["\"%Y-%m-%d\"", "\"%d/%m/%Y %H:%M\"", "\"%s\""],
        (Duration | CountIn, 1) => &["\"s\"", "\"ms\"", "\"h\"", "\"us\"", "\"d\"", "\"w\"", "\"seconds\""],
        (Repeat | PadStart | PadEnd | Resize, 1) => &["0", "1", "3", "-1"],
        (Join | Split, 1) => &["\",\"", "\"\"", "\" \""],
        (InCidr, _) | (CidrNetwork | IpVersion | IpScope | IpNormalize | IsIp, 0) => {
            &["\"10.0.0.0/8\"", "\"10.1.2.3\"", "\"::1\"", "\"fe80::/10\"", "\"x\""]
        }
        (CidrNetwork, 1) => &["8", "24", "64"],
        (CommunityId, 0 | 2) => &["\"10.0.0.1\"", "\"::1\""],
        (CommunityId, 1 | 3) => &["80", "443", "0"],
        (CommunityId, 4) => &["\"tcp\"", "\"udp\"", "6"],
        (ParseUrl, 0) => &["\"http://a.b.example.co.uk:8080/p?q=1#f\"", "\"x\""],
        (RegisteredDomain, 0) => &["\"www.example.co.uk\"", "\"localhost\""],
        (ParseUserAgent, 0) => &["\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0 Safari/537.36\""],
        (UrlDecode | HtmlStrip, 0) => &["\"a%20b\"", "\"<b>x</b>\""],
        (ParseDuration, 0) => &["\"PT1S\"", "\"1h\"", "\"5s\""],
        (ParseTime, 0) => &["\"2024-01-02\"", "\"2024-01-02T03:04:05Z\"", "\"02/01/2024 03:04\""],
        _ => &[],
    }
}

/// Positions written only as a literal: one that sizes what the call builds, so no program builds a huge value, and a
/// unit of time, which the compiler reads.
fn literal_only(f: StdFunc, i: usize) -> bool {
    use StdFunc::*;
    matches!((f, i), (Repeat | PadStart | PadEnd | Resize | Duration | CountIn, 1))
}

/// The one use of `now()` whose value no two runs disagree on.
const NOW: &str = "(year(now()) > 2000)";

const REGEXES: &[&str] = &["/a/", "/b+/i", "/^A/m", "/[,]/", "/(x)./s"];

// -- programs -----------------------------------------------------------------

/// A name a program binds: a field of the record, or a local. A function's `ty` is its return type.
#[derive(Clone)]
struct Binding {
    name: String,
    ty: DataType,
    local: bool,
    /// Read and never written: a loop's counter, a caught exception, a function.
    readonly: bool,
    /// A function's parameters, each with whether its type is written.
    params: Option<Vec<(DataType, bool)>>,
}

/// How a call is spelled: a row of `ROWS`, a function of the prelude, or a function the program binds.
#[derive(Clone)]
enum Callee {
    Row(usize),
    Prelude(&'static str),
    Bound(usize),
}

/// An expression; `Var` is a binding by its index, so a rewrite renames a local everywhere it is meant.
#[derive(Clone)]
enum E {
    Lit(String),
    Var(usize),
    /// `e.key`.
    Member(Box<E>, String),
    Index(Box<E>, Box<E>),
    Call(Callee, Vec<E>),
    Regex(&'static str),
    Bin(BinOp, Box<E>, Box<E>),
    Un(UnOp, Box<E>),
    Tern(Box<E>, Box<E>, Box<E>),
    Arr(Vec<E>),
    /// Entries in order; a `None` key is a spread.
    Obj(Vec<(Option<String>, E)>),
    As(Box<E>, DataType),
    /// Text before each value, then the text after the last.
    Template(Vec<(&'static str, E)>, &'static str),
    /// `param => body`, the param a local of the body.
    Arrow(usize, Box<E>),
}

#[derive(Clone)]
enum S {
    Assign(usize, E),
    Let(usize, E),
    If(E, Vec<S>, Vec<S>),
    For(usize, E, Vec<S>),
    /// `let c = 0; while (c < n) { c = c + 1; body }`: the step comes first, so a `continue` takes it too.
    While(usize, i64, Vec<S>),
    /// `break` or `continue`.
    Leave(&'static str),
    /// `switch (e) { case v: body break; ... default: body }`: each case with whether it ends in `break`, so one that
    /// does not falls through.
    Switch(E, Vec<(E, Vec<S>, bool)>, Option<Vec<S>>),
    /// `function f(params): ret { body value }`, with whether the return type is written.
    Function(usize, Vec<usize>, Vec<S>, E, bool),
    /// `let f = (params) => value`.
    Arrow(usize, Vec<usize>, E),
    /// `return e`, in a function's body.
    Return(E),
    /// `try { body } catch (e) { .. } finally { .. }`, with a catch, a finally or both.
    Try(Vec<S>, Option<(usize, Vec<S>)>, Option<Vec<S>>),
    /// `throw e`, `e` what a catch around it binds.
    Throw(usize),
    /// `name.key = e` or `name[i] = e`.
    Put(usize, Place, E),
    /// `name = name op e`, or the same at a place under the name: what `name op= e`, `name++` and `name--` say.
    Update(usize, Option<Place>, BinOp, E),
    /// `Object.assign(name, e)`, or `Object.assign($, e)` with no name.
    Merge(Option<usize>, E),
    /// `$ = e`.
    Replace(E),
    /// `name: type = e`, the type the binding's: `e` is of it, or a variant it converts.
    Typed(usize, E),
    Delete(Vec<usize>),
    /// `move(from, to)`.
    Move(usize, usize),
    /// `unroll(name, keep)`, the array's binding: the element's is a new one of the same name.
    Unroll(usize, bool),
    /// `drop()`, under an `if`.
    Drop,
    /// A call whose value nothing takes.
    Bare(E),
    /// A parse of a text field into the record.
    Parse(Parse),
    /// A lookup of a text field in `TABLE`, into the record.
    Lookup(Lookup),
}

/// What a parser step reads and writes, and the code it is: `Object.assign($, json(input, {...}))` or the call of its
/// kind, with each field it writes first converted (`Writer::declare`).
#[derive(Clone)]
struct Parse {
    kind: Parser,
    split_dots: bool,
    /// A text field.
    input: usize,
    /// The object it writes into, each of `declared` a field of it; `None`, the record.
    into: Option<String>,
    /// Whether `into` is an object the program bound, rather than a key the record may hold.
    into_bound: bool,
    /// Each field it writes, with whether this parse is its first write, which declares it.
    declared: Vec<(usize, bool)>,
}

impl Parse {
    /// `id`'s name as the parse finds it: its key under `into`.
    fn key<'n>(&self, name: &'n str) -> &'n str {
        match &self.into {
            Some(into) => name
                .strip_prefix(into.as_str())
                .and_then(|k| k.strip_prefix('.'))
                .unwrap_or(name),
            None => name,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Parser {
    Json,
    Kv,
    Csv,
    Cef,
    Grok,
}

impl Parser {
    const ALL: [Parser; 5] = [Parser::Json, Parser::Kv, Parser::Csv, Parser::Cef, Parser::Grok];

    fn name(self) -> &'static str {
        match self {
            Parser::Json => "json",
            Parser::Kv => "kv",
            Parser::Csv => "csv",
            Parser::Cef => "cef",
            Parser::Grok => "grok",
        }
    }

    /// Whether it lays keys nobody declared into the record, which a guard refuses.
    fn dynamic(self) -> bool {
        matches!(self, Parser::Json | Parser::Kv | Parser::Cef)
    }
}

/// A lookup step: `input` matched exactly against `TABLE`'s `code` column, its `message` written to `out`, which
/// `fresh` says this writes first.
#[derive(Clone)]
struct Lookup {
    input: usize,
    out: usize,
    fresh: bool,
    multiplicity: Multiplicity,
    default: Option<&'static str>,
}

/// The table every lookup reads: a `code` column and a `message` one.
const TABLE: (&str, &[(&str, &str)]) = ("codes", &[("Ab", "ok"), ("x", "ex"), ("a", "A"), ("", "empty")]);

/// How a step of a program is written as a pipeline step. Its statements are the code it means, which the program
/// holds whatever the form: a rewrite writes them as code.
#[derive(Clone)]
enum Form {
    Code,
    /// Each statement an `Assign`.
    Eval,
    /// `if (c) { drop(); }`.
    Drop,
    /// Each statement a `Move`.
    Rename,
    /// One `Unroll`.
    Unroll,
    /// One `Parse`.
    Parser,
    /// One `Lookup`.
    Lookup,
    /// `if (c) { inner }`: the inner step's filter.
    Filter(Box<Form>),
    /// `if (c) { then } else { else_ }`, each branch steps by their form and how many statements each holds.
    If(Vec<(Form, usize)>, Vec<(Form, usize)>),
}

/// Where under a name a write lands.
#[derive(Clone)]
enum Place {
    Key(String),
    At(E),
}

struct Program {
    bindings: Vec<Binding>,
    /// The steps after `HEAD`, each the code it means as a list of statements.
    steps: Vec<Vec<S>>,
    /// How each step is written as a pipeline step.
    forms: Vec<Form>,
}

// -- generating ---------------------------------------------------------------

/// The fields `HEAD` declares, each program's first bindings.
fn declared() -> Vec<(&'static str, DataType)> {
    use DataType::*;
    vec![
        ("a", I64),
        ("b", I64),
        ("f", F64),
        ("s", Str),
        ("u", Str),
        ("bo", Bool),
        ("by", Bytes),
        ("t", DateTime),
        ("d", Duration),
        ("xs", arr(I64)),
        ("ys", arr(Str)),
        ("fs", arr(F64)),
        ("o.p", I64),
        ("o.q", Str),
        ("_raw", Str),
    ]
}

const MAX_DEPTH: usize = 4;

struct Gen {
    rng: Rng,
    bindings: Vec<Binding>,
    /// The record's fields, in the order they were bound.
    fields: Vec<usize>,
    /// The locals in scope, a list per open block.
    scopes: Vec<Vec<usize>>,
    /// Whether the statement being written is in a loop's body.
    looping: bool,
    /// Locals let as `[]` that no write has typed yet.
    untyped: Vec<usize>,
    /// Whether the statement being written is in a `switch` case, where a `break` leaves the case.
    switching: bool,
    /// The exceptions a `throw` here may name: the catches around it, up to a loop.
    catches: Vec<usize>,
    /// A name no expression here reads.
    hidden: Option<String>,
    /// Whether a local here takes a name of its own, never a field's.
    unshadowed: bool,
    /// The fields a function or an arrow reads, which it reads where it is called: none is deleted, moved or
    /// retyped.
    pinned: BTreeSet<usize>,
}

impl Gen {
    fn new(seed: u64) -> Gen {
        let mut g = Gen {
            rng: Rng::seeded(seed),
            bindings: Vec::new(),
            fields: Vec::new(),
            scopes: Vec::new(),
            looping: false,
            untyped: Vec::new(),
            switching: false,
            catches: Vec::new(),
            hidden: None,
            unshadowed: false,
            pinned: BTreeSet::new(),
        };
        for (name, ty) in declared() {
            g.field(name.to_string(), ty);
        }
        g
    }

    fn field(&mut self, name: String, ty: DataType) -> usize {
        self.bindings.push(Binding {
            name,
            ty,
            local: false,
            readonly: false,
            params: None,
        });
        self.fields.push(self.bindings.len() - 1);
        self.bindings.len() - 1
    }

    fn local(&mut self, ty: DataType) -> usize {
        // A local sometimes takes a field's name and shadows it from here to the end of its block, unless its block
        // has one of that name.
        let name = match self.rng.below(4) {
            0 if !self.unshadowed => {
                let f = *self.rng.pick(&self.fields);
                let name = &self.bindings[f].name;
                let mine = self.scopes.last().expect("a local is bound in a block");
                if name.contains('.') || name.starts_with('_') || mine.iter().any(|&l| self.bindings[l].name == *name) {
                    format!("l{}", self.bindings.len())
                } else {
                    name.clone()
                }
            }
            _ => format!("l{}", self.bindings.len()),
        };
        self.bindings.push(Binding {
            name,
            ty,
            local: true,
            readonly: false,
            params: None,
        });
        let id = self.bindings.len() - 1;
        self.scopes.last_mut().expect("a local is bound in a block").push(id);
        id
    }

    /// A local that is read and never written, of `ty`, or a function of `params` returning `ty`. A function takes a
    /// name of its own: a declared one is hoisted, so a field's name would mean it before it is written.
    fn fixed(&mut self, ty: DataType, params: Option<Vec<(DataType, bool)>>) -> usize {
        let id = self.local(ty);
        self.bindings[id].readonly = true;
        if params.is_some() {
            self.bindings[id].name = format!("g{id}");
        }
        self.bindings[id].params = params;
        id
    }

    /// The bindings a name reaches here, each name's innermost.
    fn visible(&self) -> Vec<usize> {
        let mut seen: Vec<usize> = Vec::new();
        for &id in self.fields.iter().chain(self.scopes.iter().flatten()) {
            let name = &self.bindings[id].name;
            // A local named `o` would hide `o.p`; none is, since a local takes no dotted name and no field is `o`.
            seen.retain(|&s| self.bindings[s].name != *name);
            seen.push(id);
        }
        seen.retain(|&s| self.hidden.as_ref() != Some(&self.bindings[s].name));
        seen
    }

    fn var(&mut self, ty: &DataType) -> Option<usize> {
        let of: Vec<usize> = self
            .visible()
            .into_iter()
            .filter(|&id| self.bindings[id].ty == *ty && self.bindings[id].params.is_none())
            .collect();
        (!of.is_empty()).then(|| *self.rng.pick(&of))
    }

    /// A name of `ty` a statement may write.
    fn target(&mut self, ty: &DataType) -> Option<usize> {
        let of: Vec<usize> = self
            .visible()
            .into_iter()
            .filter(|&id| {
                let b = &self.bindings[id];
                b.ty == *ty && b.params.is_none() && !b.readonly
            })
            .collect();
        (!of.is_empty()).then(|| *self.rng.pick(&of))
    }

    /// A type for a new name.
    fn ty(&mut self) -> DataType {
        match self.rng.below(16) {
            0..=8 => self.rng.pick(&scalars()).clone(),
            9..=12 => arr(self.rng.pick(&scalars()).clone()),
            13 => arr(arr(DataType::I64)),
            14 => self.struct_ty(),
            _ => DataType::Variant,
        }
    }

    /// A closed object of a few scalar fields.
    fn struct_ty(&mut self) -> DataType {
        let n = 1 + self.rng.below(3);
        let fields = (0..n)
            .map(|i| (format!("k{i}"), self.rng.pick(&scalars()).clone()))
            .collect();
        DataType::Struct(fields, false)
    }

    fn literal(&mut self, ty: &DataType, depth: usize) -> E {
        use DataType as T;
        let lit = |s: &str| E::Lit(s.to_string());
        match ty {
            T::Bool => lit(self.rng.pick(&["true", "false", NOW])),
            T::I64 => lit(self.rng.pick(&["0", "1", "-3", "7", "255", "4611686018427387904"])),
            T::F64 => lit(self.rng.pick(&["1.5", "-0.25", "1e3", "0.0", "2.5"])),
            T::Str => lit(self.rng.pick(&[
                "\"\"",
                "\"x\"",
                "\"Ab\"",
                "\"a,b\"",
                "\" pad \"",
                "\"10.0.0.1\"",
                "\"2024-01-02T03:04:05Z\"",
                "\"\\u00e9\\n\\\"q\\\"\"",
                "'single'",
            ])),
            T::Bytes => lit(self.rng.pick(&["(\"aGk=\" as bytes)", "parseBytes(\"ff00\", \"hex\")"])),
            T::DateTime => lit(self
                .rng
                .pick(&["(\"2024-01-02T03:04:05Z\" as datetime)", "fromEpochSeconds(0)"])),
            T::Duration => lit(self.rng.pick(&["duration(5, \"s\")", "(90 as duration)"])),
            T::Array(e) => {
                let e = (**e).clone();
                let n = 1 + self.rng.below(3);
                E::Arr((0..n).map(|_| self.leaf(&e, depth + 1)).collect())
            }
            T::Struct(fields, _) => {
                let fields = fields.clone();
                E::Obj(
                    fields
                        .iter()
                        .map(|(k, t)| (Some(k.clone()), self.leaf(t, depth + 1)))
                        .collect(),
                )
            }
            T::Variant => self.variant(depth),
            T::Null => lit("null"),
        }
    }

    /// A read the record's open objects answer only by row: a variant.
    fn variant(&mut self, depth: usize) -> E {
        match self.rng.below(4) {
            0 => E::Lit("o.r".into()),
            1 => E::Lit("$.zz".into()),
            2 => E::Index(
                Box::new(E::Lit("o".into())),
                Box::new(self.expr(&DataType::Str, depth + 1)),
            ),
            _ => E::Index(
                Box::new(E::Lit("$".into())),
                Box::new(self.expr(&DataType::Str, depth + 1)),
            ),
        }
    }

    fn leaf(&mut self, ty: &DataType, depth: usize) -> E {
        if self.rng.below(3) > 0
            && let Some(id) = self.var(ty)
        {
            return E::Var(id);
        }
        self.literal(ty, depth)
    }

    fn expr(&mut self, ty: &DataType, depth: usize) -> E {
        if depth >= MAX_DEPTH || self.rng.chance(4) {
            return self.leaf(ty, depth);
        }
        for _ in 0..6 {
            if let Some(e) = self.compound(ty, depth + 1) {
                return e;
            }
        }
        self.leaf(ty, depth)
    }

    /// An expression of `ty` built of others, or `None` where the form picked makes no `ty`.
    fn compound(&mut self, ty: &DataType, d: usize) -> Option<E> {
        use DataType as T;
        let b = Box::new;
        Some(match self.rng.below(15) {
            0 => E::Tern(b(self.expr(&T::Bool, d)), b(self.expr(ty, d)), b(self.expr(ty, d))),
            1 => {
                let l = match self.rng.chance(4) {
                    true => E::Lit("null".into()),
                    false => self.expr(ty, d),
                };
                E::Bin(BinOp::Coalesce, b(l), b(self.expr(ty, d)))
            }
            2 | 3 => {
                let of: Vec<&Typed<BinOp>> = index()
                    .bins
                    .iter()
                    .filter(|x| x.ret == *ty && x.op != BinOp::Coalesce)
                    .collect();
                if of.is_empty() {
                    return None;
                }
                let x = *self.rng.pick(&of);
                E::Bin(x.op, b(self.expr(&x.args[0], d)), b(self.expr(&x.args[1], d)))
            }
            4 => {
                let of: Vec<&Typed<UnOp>> = index().uns.iter().filter(|x| x.ret == *ty).collect();
                if of.is_empty() {
                    return None;
                }
                let x = *self.rng.pick(&of);
                E::Un(x.op, b(self.expr(&x.args[0], d)))
            }
            5..=7 => {
                let of: Vec<&Inst> = index().calls.iter().filter(|x| x.ret == *ty).collect();
                if of.is_empty() {
                    return None;
                }
                let x = *self.rng.pick(&of);
                self.call(x, d)
            }
            8 => self.member(ty, d)?,
            9 => match ty {
                T::Variant | T::Struct(..) => return None,
                T::Str if self.rng.chance(2) => E::Index(b(self.expr(&T::Str, d)), b(self.expr(&T::I64, d))),
                t => E::Index(b(self.expr(&arr(t.clone()), d)), b(self.expr(&T::I64, d))),
            },
            10 => {
                let from: Vec<DataType> = match ty {
                    T::Str => universe(),
                    T::Bool | T::I64 | T::F64 | T::Bytes | T::DateTime | T::Duration => Self::converts(ty),
                    T::Array(e) => match &**e {
                        T::Array(_) | T::Struct(..) | T::Variant | T::Null => return None,
                        e => Self::converts(e).into_iter().map(arr).collect(),
                    },
                    _ => return None,
                };
                let from = self.rng.pick(&from).clone();
                E::As(b(self.expr(&from, d)), ty.clone())
            }
            11 if *ty == T::Str => {
                let n = 1 + self.rng.below(3);
                let parts = (0..n)
                    .map(|_| {
                        let t = self.rng.pick(&scalars()).clone();
                        (*self.rng.pick(&["", "a", " - ", "x:"]), self.expr(&t, d))
                    })
                    .collect();
                E::Template(parts, self.rng.pick(&["", "!", " end"]))
            }
            12 => match ty {
                T::Array(elem) => {
                    let elem = (**elem).clone();
                    self.scopes.push(Vec::new());
                    let e = match self.rng.chance(2) {
                        // `map(xs, x => …)` from an array of any scalar.
                        true => {
                            let from = self.rng.pick(&scalars()).clone();
                            let src = self.expr(&arr(from.clone()), d);
                            let x = self.local(from);
                            let body = self.expr(&elem, d);
                            E::Call(Callee::Prelude("map"), vec![src, E::Arrow(x, b(body))])
                        }
                        false => {
                            let src = self.expr(ty, d);
                            let x = self.local(elem);
                            let body = self.expr(&T::Bool, d);
                            E::Call(Callee::Prelude("filter"), vec![src, E::Arrow(x, b(body))])
                        }
                    };
                    self.scopes.pop();
                    e
                }
                _ => return None,
            },
            13 => match ty {
                T::Array(elem) => {
                    let elem = (**elem).clone();
                    let n = 1 + self.rng.below(3);
                    E::Arr((0..n).map(|_| self.expr(&elem, d)).collect())
                }
                T::Struct(fields, _) => {
                    let fields = fields.clone();
                    let mut entries = Vec::new();
                    match (self.rng.below(3), self.var(ty)) {
                        // A name of the type spread, some of its fields written again over it.
                        (0, Some(id)) => {
                            entries.push((None, E::Var(id)));
                            for (k, t) in &fields {
                                if self.rng.chance(2) {
                                    entries.push((Some(k.clone()), self.expr(t, d)));
                                }
                            }
                        }
                        // A literal spread of the first fields, the rest after it: a literal names each key once.
                        (1, _) => {
                            let cut = self.rng.below(fields.len() + 1);
                            let spread = fields[..cut]
                                .iter()
                                .map(|(k, t)| (Some(k.clone()), self.expr(t, d)))
                                .collect();
                            entries.push((None, E::Obj(spread)));
                            for (k, t) in &fields[cut..] {
                                entries.push((Some(k.clone()), self.expr(t, d)));
                            }
                        }
                        _ => {
                            for (k, t) in &fields {
                                entries.push((Some(k.clone()), self.expr(t, d)));
                            }
                        }
                    }
                    E::Obj(entries)
                }
                _ => return None,
            },
            14 => {
                let fns: Vec<usize> = self
                    .visible()
                    .into_iter()
                    .filter(|&id| self.bindings[id].params.is_some() && self.bindings[id].ty == *ty)
                    .collect();
                if fns.is_empty() {
                    return None;
                }
                let f = *self.rng.pick(&fns);
                let params = self.bindings[f].params.clone().expect("a function has parameters");
                let args = params.iter().map(|(t, _)| self.expr(t, d)).collect();
                E::Call(Callee::Bound(f), args)
            }
            _ => return None,
        })
    }

    /// The scalar types `as` converts to the scalar `to` by the coercion table, `to` among them.
    fn converts(to: &DataType) -> Vec<DataType> {
        use DataType as T;
        match to {
            T::Str => scalars(),
            T::I64 | T::F64 => vec![T::Str, T::I64, T::F64],
            T::DateTime | T::Duration => vec![T::Str, T::I64, T::F64, to.clone()],
            T::Bool | T::Bytes => vec![T::Str, to.clone()],
            _ => vec![],
        }
    }

    /// A call of `x`, each argument of its type: a regex, a literal the function means something with, or any value.
    fn call(&mut self, x: &Inst, d: usize) -> E {
        let f = ROWS[x.row].func;
        let args = x
            .args
            .iter()
            .enumerate()
            .map(|(i, t)| match t {
                None => E::Regex(self.rng.pick(REGEXES)),
                Some(_) if literal_only(f, i) => E::Lit(self.rng.pick(hint(f, i)).to_string()),
                Some(_) if !hint(f, i).is_empty() && self.rng.chance(2) => {
                    E::Lit(self.rng.pick(hint(f, i)).to_string())
                }
                Some(t) => self.expr(t, d),
            })
            .collect();
        E::Call(Callee::Row(x.row), args)
    }

    /// A field of type `ty` read from an object: a name that holds one, a call that gives one, or a literal.
    fn member(&mut self, ty: &DataType, d: usize) -> Option<E> {
        let has = |t: &DataType| -> Vec<String> {
            match t {
                DataType::Struct(fields, _) => fields
                    .iter()
                    .filter(|(_, ft)| ft == ty)
                    .map(|(k, _)| k.clone())
                    .collect(),
                _ => Vec::new(),
            }
        };
        let names: Vec<usize> = self
            .visible()
            .into_iter()
            .filter(|&id| !has(&self.bindings[id].ty).is_empty())
            .collect();
        let calls: Vec<&Inst> = index().calls.iter().filter(|x| !has(&x.ret).is_empty()).collect();
        let (base, keys) = match self.rng.below(3) {
            0 if !names.is_empty() => {
                let id = *self.rng.pick(&names);
                (E::Var(id), has(&self.bindings[id].ty))
            }
            1 if !calls.is_empty() => {
                let x = *self.rng.pick(&calls);
                (self.call(x, d), has(&x.ret))
            }
            _ => {
                let k = "k".to_string();
                (E::Obj(vec![(Some(k.clone()), self.expr(ty, d))]), vec![k])
            }
        };
        let k = self.rng.pick(&keys).clone();
        Some(E::Member(Box::new(base), k))
    }

    fn block(&mut self, depth: usize, n: usize) -> Vec<S> {
        self.scopes.push(Vec::new());
        let out = self.stmts(depth, n);
        self.scopes.pop();
        out
    }

    /// `n` statements in the open block. A local let as `[]` is written next, so it has a type.
    fn stmts(&mut self, depth: usize, n: usize) -> Vec<S> {
        let mut out = Vec::new();
        for _ in 0..n {
            out.push(self.stmt(depth));
            for id in std::mem::take(&mut self.untyped) {
                // Its write cannot read it, nor a name it hides: it has no type until the write gives it one.
                self.hidden = Some(self.bindings[id].name.clone());
                let ty = self.bindings[id].ty.clone();
                out.push(S::Assign(id, self.expr(&ty, 0)));
                self.hidden = None;
            }
        }
        out
    }

    /// A `let`'s value, which may be `[]`: the local takes its type from the write after it.
    fn let_value(&mut self, ty: &DataType) -> E {
        match ty {
            DataType::Array(_) if self.rng.chance(6) => E::Lit("[]".into()),
            _ => self.expr(ty, 0),
        }
    }

    /// `let x = value;`, with `x` queued for a write where the value is `[]`.
    fn let_stmt(&mut self, ty: DataType) -> S {
        let e = self.let_value(&ty);
        let id = self.local(ty);
        if matches!(&e, E::Lit(l) if l == "[]") {
            self.untyped.push(id);
        }
        S::Let(id, e)
    }

    /// A statement. A new field is bound only at a step's top level; inside a block a write goes to a name already
    /// bound, at its type.
    fn stmt(&mut self, depth: usize) -> S {
        let top = depth == 0;
        if depth < 2
            && self.rng.chance(3)
            && let Some(s) = self.control(depth)
        {
            return s;
        }
        if self.rng.chance(4)
            && let Some(s) = self.write(depth)
        {
            return s;
        }
        match self.rng.below(if depth >= 2 { 6 } else { 10 }) {
            0 | 1 if top => {
                // A variant field is what a typed write converts: there is one in most programs.
                let ty = match self.rng.chance(4) {
                    true => DataType::Variant,
                    false => self.ty(),
                };
                let e = self.expr(&ty, 0);
                let id = self.field(format!("f{}", self.bindings.len()), ty);
                S::Assign(id, e)
            }
            0..=2 => {
                let ty = self.ty();
                // An array written in a loop may grow with every trip, and a loop nested in it then runs past its
                // budget, which is time and so differs from run to run.
                match self
                    .target(&ty)
                    .filter(|_| !(self.looping && matches!(ty, DataType::Array(_))))
                {
                    Some(id) => {
                        let e = match self.rng.chance(8) {
                            true => E::Lit("null".into()),
                            false => self.expr(&ty, 0),
                        };
                        S::Assign(id, e)
                    }
                    None => self.let_stmt(ty),
                }
            }
            3..=5 => {
                let ty = self.ty();
                self.let_stmt(ty)
            }
            6..=7 => {
                let cond = self.expr(&DataType::Bool, 0);
                let n = 1 + self.rng.below(3);
                let then = self.block(depth + 1, n);
                let else_ = if self.rng.chance(2) {
                    Vec::new()
                } else {
                    let n = 1 + self.rng.below(2);
                    self.block(depth + 1, n)
                };
                S::If(cond, then, else_)
            }
            _ => {
                let elem = match self.rng.chance(4) {
                    true => arr(DataType::I64),
                    false => self.rng.pick(&scalars()).clone(),
                };
                let over = self.expr(&arr(elem.clone()), 1);
                self.scopes.push(Vec::new());
                let var = self.local(elem);
                let n = 1 + self.rng.below(3);
                let body = self.loop_body(depth + 1, n);
                self.scopes.pop();
                S::For(var, over, body)
            }
        }
    }

    /// A loop's body, where a `break` or `continue` leaves the loop and no `throw` reaches a catch outside it.
    fn loop_body(&mut self, depth: usize, n: usize) -> Vec<S> {
        let outer = (
            std::mem::replace(&mut self.looping, true),
            std::mem::replace(&mut self.switching, false),
            std::mem::take(&mut self.catches),
        );
        let body = self.stmts(depth, n);
        (self.looping, self.switching, self.catches) = outer;
        body
    }

    /// A statement that steers which statements run, or `None` where the form picked has no place here.
    fn control(&mut self, depth: usize) -> Option<S> {
        use DataType as T;
        let top = depth == 0;
        Some(match self.rng.below(8) {
            0 => {
                let n = 1 + self.rng.below(3) as i64;
                // A name of its own: it stays bound in the block after the loop.
                let counter = self.fixed(T::I64, None);
                self.bindings[counter].name = format!("c{counter}");
                let k = 1 + self.rng.below(3);
                self.scopes.push(Vec::new());
                let body = self.loop_body(depth + 1, k);
                self.scopes.pop();
                S::While(counter, n, body)
            }
            1 if self.looping && !self.switching => {
                let leave = *self.rng.pick(&["break", "continue"]);
                S::If(self.expr(&T::Bool, 0), vec![S::Leave(leave)], Vec::new())
            }
            2 => {
                let (ty, values): (T, &[&str]) = match self.rng.chance(2) {
                    true => (T::I64, &["0", "1", "2", "-7", "3"]),
                    false => (T::Str, &["\"Ab\"", "\"\"", "\"x\"", "\"ab,c\""]),
                };
                let on = self.expr(&ty, 1);
                let mut values = values.to_vec();
                let n = 1 + self.rng.below(3);
                let outer = std::mem::replace(&mut self.switching, true);
                let cases = (0..n)
                    .map(|_| {
                        let v = values.remove(self.rng.below(values.len()));
                        let k = self.rng.below(3);
                        // A case that falls through runs the next one's body in its own block, so its locals reach
                        // there: it shadows no name the next one reads.
                        let brk = !self.rng.chance(3);
                        let outer = std::mem::replace(&mut self.unshadowed, !brk);
                        let body = self.block(depth + 1, k);
                        self.unshadowed = outer;
                        (E::Lit(v.to_string()), body, brk)
                    })
                    .collect();
                let default = self.rng.chance(2).then(|| {
                    let k = 1 + self.rng.below(2);
                    self.block(depth + 1, k)
                });
                self.switching = outer;
                S::Switch(on, cases, default)
            }
            3 => {
                let n = 1 + self.rng.below(3);
                let body = self.block(depth + 1, n);
                let (catch, finally) = match self.rng.below(3) {
                    0 => (true, false),
                    1 => (false, true),
                    _ => (true, true),
                };
                let catch = catch.then(|| {
                    self.scopes.push(Vec::new());
                    let kind = T::Struct(vec![("kind".into(), T::Str), ("message".into(), T::Str)], false);
                    // A name of its own: the catch's body is the block it binds in.
                    let e = self.fixed(kind, None);
                    self.bindings[e].name = format!("e{e}");
                    self.catches.push(e);
                    let k = 1 + self.rng.below(2);
                    let mut body = self.stmts(depth + 1, k);
                    if self.rng.chance(3) {
                        body.push(S::If(self.expr(&T::Bool, 0), vec![S::Throw(e)], Vec::new()));
                    }
                    self.catches.pop();
                    self.scopes.pop();
                    (e, body)
                });
                let finally = finally.then(|| {
                    let k = 1 + self.rng.below(2);
                    self.block(depth + 1, k)
                });
                S::Try(body, catch, finally)
            }
            4 if !self.catches.is_empty() => {
                let e = *self.rng.pick(&self.catches);
                S::If(self.expr(&T::Bool, 0), vec![S::Throw(e)], Vec::new())
            }
            5 if top => {
                let f = self.function();
                self.pin(&f);
                f
            }
            6 => {
                // An arrow reads the names where it is written.
                let ret = self.rng.pick(&scalars()).clone();
                self.scopes.push(Vec::new());
                let n = 1 + self.rng.below(2);
                let params: Vec<usize> = (0..n)
                    .map(|_| {
                        let t = self.rng.pick(&scalars()).clone();
                        self.local(t)
                    })
                    .collect();
                let body = self.expr(&ret, 1);
                self.scopes.pop();
                let types = params.iter().map(|&p| (self.bindings[p].ty.clone(), false)).collect();
                let f = self.fixed(ret, Some(types));
                let arrow = S::Arrow(f, params, body);
                self.pin(&arrow);
                arrow
            }
            7 if !self.looping => S::If(self.expr(&T::Bool, 0), vec![S::Drop], Vec::new()),
            _ => return None,
        })
    }

    /// Pins each field `s` reads.
    fn pin(&mut self, s: &S) {
        let mut read = Vec::new();
        each(std::slice::from_ref(s), &mut |n| {
            if let Node::E(E::Var(id)) = n {
                read.push(*id);
            }
        });
        self.pinned
            .extend(read.into_iter().filter(|&id| !self.bindings[id].local));
    }

    /// The names here a statement may write whose type `keep` takes.
    fn writable(&self, keep: impl Fn(&DataType) -> bool) -> Vec<usize> {
        self.visible()
            .into_iter()
            .filter(|&id| {
                let b = &self.bindings[id];
                !b.readonly && b.params.is_none() && keep(&b.ty)
            })
            .collect()
    }

    /// The record's fields here that a statement may take away or retype, whose type `keep` takes.
    fn movable(&self, keep: impl Fn(&DataType) -> bool) -> Vec<usize> {
        self.writable(keep)
            .into_iter()
            .filter(|&id| !self.bindings[id].local && !self.pinned.contains(&id))
            .collect()
    }

    /// A field the record's object takes by its key: `Object.assign($, …)` and `$ = {…}` write it. At the top level it
    /// may be a new one.
    fn record_entries(&mut self, top: bool) -> Vec<(Option<String>, E)> {
        let looping = self.looping;
        let mut of: Vec<usize> = self
            .writable(|t| !(looping && matches!(t, DataType::Array(_))))
            .into_iter()
            .filter(|&id| {
                let b = &self.bindings[id];
                !b.local && !b.name.contains('.') && !b.name.starts_with('_')
            })
            .collect();
        let n = 1 + self.rng.below(3);
        let mut out = Vec::new();
        for _ in 0..n {
            let id = match of.is_empty() || (top && self.rng.chance(3)) {
                true if top => {
                    let ty = self.ty();
                    self.field(format!("f{}", self.bindings.len()), ty)
                }
                true => break,
                false => of.remove(self.rng.below(of.len())),
            };
            let ty = self.bindings[id].ty.clone();
            out.push((Some(self.bindings[id].name.clone()), self.expr(&ty, 1)));
        }
        out
    }

    /// A write to the record or under a name, or `None` where the form picked has nothing here to write.
    fn write(&mut self, depth: usize) -> Option<S> {
        let form = self.rng.below(13);
        self.write_as(form, depth)
    }

    /// The write `write` picks as `form`: 10 a `move`, 11 an `unroll`.
    fn write_as(&mut self, form: usize, depth: usize) -> Option<S> {
        use DataType as T;
        let top = depth == 0;
        let looping = self.looping;
        Some(match form {
            0 | 1 => {
                let of = self.writable(|t| matches!(t, T::Struct(..) | T::Array(_)));
                if of.is_empty() {
                    return None;
                }
                let id = *self.rng.pick(&of);
                let (place, ty) = self.place(id);
                S::Put(id, place, self.expr(&ty, 1))
            }
            2 | 3 => {
                // A slot whose type an assigning operator keeps: the name, or a key or a place under it.
                let ops = |t: &DataType| -> Vec<BinOp> {
                    index()
                        .bins
                        .iter()
                        .filter(|x| x.ret == *t && x.args[0] == *t && !x.op.is_comparison())
                        .map(|x| x.op)
                        .collect()
                };
                let of = self.writable(|t| !(looping && matches!(t, T::Array(_))) && *t != T::Variant);
                if of.is_empty() {
                    return None;
                }
                let id = *self.rng.pick(&of);
                let (place, ty) = match self.bindings[id].ty {
                    T::Struct(..) | T::Array(_) if self.rng.chance(2) => {
                        let (p, t) = self.place(id);
                        (Some(p), t)
                    }
                    ref t => (None, t.clone()),
                };
                let ops = ops(&ty);
                if ops.is_empty() {
                    return None;
                }
                let op = *self.rng.pick(&ops);
                let rhs = index()
                    .bins
                    .iter()
                    .find(|x| x.op == op && x.ret == ty && x.args[0] == ty)
                    .expect("an operator of the slot's type")
                    .args[1]
                    .clone();
                let e = match (op, &rhs) {
                    (BinOp::Add | BinOp::Sub, T::I64) if self.rng.chance(2) => E::Lit("1".into()),
                    _ => self.expr(&rhs, 1),
                };
                S::Update(id, place, op, e)
            }
            4 => {
                let of = self.writable(|t| matches!(t, T::Struct(..)));
                if of.is_empty() {
                    return None;
                }
                let id = *self.rng.pick(&of);
                let ty = self.bindings[id].ty.clone();
                let T::Struct(fields, _) = &ty else { unreachable!() };
                let from = match self.var(&ty) {
                    Some(v) if self.rng.chance(3) => E::Var(v),
                    _ => {
                        let mut entries = Vec::new();
                        for (k, t) in fields {
                            if entries.is_empty() || self.rng.chance(2) {
                                entries.push((Some(k.clone()), self.expr(t, 1)));
                            }
                        }
                        E::Obj(entries)
                    }
                };
                S::Merge(Some(id), from)
            }
            5 => {
                let entries = self.record_entries(top);
                if entries.is_empty() {
                    return None;
                }
                S::Merge(None, E::Obj(entries))
            }
            6 if top => {
                let mut entries = vec![(None, E::Lit("$".into()))];
                entries.extend(self.record_entries(true));
                S::Replace(E::Obj(entries))
            }
            // A record key no field binds, converted: `zz` while it is unbound, else one the record lacks.
            7 if top => {
                let ty = self.ty();
                if ty == T::Variant {
                    return None;
                }
                let name = match self.bindings.iter().any(|b| b.name == "zz") {
                    true => format!("f{}", self.bindings.len()),
                    false => "zz".to_string(),
                };
                let id = self.field(name, ty);
                S::Typed(id, E::Var(id))
            }
            8 if top => {
                let ty = self.ty();
                if ty == T::Variant {
                    return None;
                }
                let of = self.movable(|t| *t == T::Variant);
                match self.rng.chance(2) {
                    // A variant field converted where it stands.
                    true if !of.is_empty() => {
                        let old = *self.rng.pick(&of);
                        let name = self.bindings[old].name.clone();
                        self.fields.retain(|&f| f != old);
                        S::Typed(self.field(name, ty), E::Var(old))
                    }
                    convert => {
                        let e = match convert || self.rng.chance(2) {
                            true => self.variant(1),
                            false => self.expr(&ty, 1),
                        };
                        S::Typed(self.field(format!("f{}", self.bindings.len()), ty), e)
                    }
                }
            }
            9 if top => {
                let mut of = self.movable(|_| true);
                if of.is_empty() {
                    return None;
                }
                let n = 1 + self.rng.below(2.min(of.len()));
                let gone: Vec<usize> = (0..n).map(|_| of.remove(self.rng.below(of.len()))).collect();
                self.fields.retain(|f| !gone.contains(f));
                S::Delete(gone)
            }
            10 if top => {
                let of = self.movable(|_| true);
                if of.is_empty() {
                    return None;
                }
                let from = *self.rng.pick(&of);
                let ty = self.bindings[from].ty.clone();
                self.fields.retain(|&f| f != from);
                S::Move(from, self.field(format!("f{}", self.bindings.len()), ty))
            }
            11 if top => {
                let of = self.movable(|t| matches!(t, T::Array(_)));
                if of.is_empty() {
                    return None;
                }
                let old = *self.rng.pick(&of);
                let T::Array(elem) = self.bindings[old].ty.clone() else {
                    unreachable!()
                };
                let name = self.bindings[old].name.clone();
                self.fields.retain(|&f| f != old);
                self.field(name, *elem);
                S::Unroll(old, self.rng.chance(2))
            }
            12 => {
                let x = *self.rng.pick(&index().calls.iter().collect::<Vec<&Inst>>());
                S::Bare(self.call(x, 1))
            }
            _ => return None,
        })
    }

    /// A key of the object `id` holds, or a place in the array it holds, with the type there.
    fn place(&mut self, id: usize) -> (Place, DataType) {
        match self.bindings[id].ty.clone() {
            DataType::Struct(fields, _) => {
                let (k, t) = self.rng.pick(&fields).clone();
                (Place::Key(k), t)
            }
            DataType::Array(elem) => (Place::At(self.expr(&DataType::I64, 2)), *elem),
            _ => unreachable!("a place is under an object or an array"),
        }
    }

    /// `function f(...) { ... }`. Its body reads the names where it is written and its parameters, and writes none.
    fn function(&mut self) -> S {
        let ret = self.rng.pick(&scalars()).clone();
        self.scopes.push(Vec::new());
        let outer = (
            std::mem::replace(&mut self.looping, false),
            std::mem::replace(&mut self.switching, false),
            std::mem::take(&mut self.catches),
        );
        let n = self.rng.below(3);
        let params: Vec<usize> = (0..n)
            .map(|_| {
                let t = self.rng.pick(&scalars()).clone();
                self.local(t)
            })
            .collect();
        let mut body = Vec::new();
        for _ in 0..self.rng.below(3) {
            let t = self.ty();
            body.push(self.let_stmt(t));
            for id in std::mem::take(&mut self.untyped) {
                self.hidden = Some(self.bindings[id].name.clone());
                let ty = self.bindings[id].ty.clone();
                body.push(S::Assign(id, self.expr(&ty, 0)));
                self.hidden = None;
            }
        }
        if self.rng.chance(2) {
            let cond = self.expr(&DataType::Bool, 1);
            body.push(S::If(cond, vec![S::Return(self.expr(&ret, 1))], Vec::new()));
        }
        let value = self.expr(&ret, 1);
        self.scopes.pop();
        (self.looping, self.switching, self.catches) = outer;
        let types = params
            .iter()
            .map(|&p| (self.bindings[p].ty.clone(), self.rng.chance(2)))
            .collect();
        let f = self.fixed(ret, Some(types));
        S::Function(f, params, body, value, self.rng.chance(2))
    }

    fn program(mut self) -> Program {
        let (mut steps, mut forms) = (Vec::new(), Vec::new());
        for _ in 0..1 + self.rng.below(4) {
            for (form, step) in self.step() {
                forms.push(form);
                steps.push(step);
            }
        }
        Program {
            bindings: self.bindings,
            steps,
            forms,
        }
    }

    /// A step, or the steps a parse needs: its text first.
    fn step(&mut self) -> Vec<(Form, Vec<S>)> {
        self.scopes.push(Vec::new());
        let out = match self.rng.below(12) {
            0..=4 => {
                self.scopes.pop();
                let n = 1 + self.rng.below(7);
                return vec![(Form::Code, self.block(0, n))];
            }
            5 => self.eval(true).map(|s| vec![(Form::Eval, s)]),
            6 => self
                .write_as(10, 0)
                .map(|m| vec![(Form::Rename, vec![m])])
                .or_else(|| self.write_as(11, 0).map(|u| vec![(Form::Unroll, vec![u])])),
            7 => self.write_as(11, 0).map(|u| vec![(Form::Unroll, vec![u])]),
            8 => self.parse_steps(true),
            9 => Some(vec![self.guarded()]),
            10 => {
                let c = self.expr(&DataType::Bool, 0);
                let then = self.guarded_steps();
                let else_ = match self.rng.chance(2) {
                    true => self.guarded_steps(),
                    false => Vec::new(),
                };
                let then_s = then.iter().flat_map(|(_, s)| s.clone()).collect();
                let else_s = else_.iter().flat_map(|(_, s)| s.clone()).collect();
                let len = |v: Vec<(Form, Vec<S>)>| v.into_iter().map(|(f, s)| (f, s.len())).collect();
                Some(vec![(Form::If(len(then), len(else_)), vec![S::If(c, then_s, else_s)])])
            }
            _ => self.lookup(true).map(|l| vec![(Form::Lookup, vec![l])]),
        };
        let out = out.unwrap_or_else(|| vec![(Form::Drop, vec![self.drop_stmt()])]);
        self.scopes.pop();
        out
    }

    /// One to three steps a guard may hold, none binding a field.
    fn guarded_steps(&mut self) -> Vec<(Form, Vec<S>)> {
        (0..1 + self.rng.below(2)).map(|_| self.guarded()).collect()
    }

    /// A step a guard may hold, sometimes under a filter of its own: one that writes only names already bound.
    fn guarded(&mut self) -> (Form, Vec<S>) {
        let (form, stmts) = match self.rng.below(4) {
            0 => self.eval(false).map(|s| (Form::Eval, s)),
            1 => self.lookup(false).map(|l| (Form::Lookup, vec![l])),
            2 => self.parse_steps(false).and_then(|mut v| v.pop()),
            _ => None,
        }
        .unwrap_or_else(|| (Form::Drop, vec![self.drop_stmt()]));
        match self.rng.chance(3) {
            true => {
                let c = self.expr(&DataType::Bool, 0);
                (Form::Filter(Box::new(form)), vec![S::If(c, stmts, Vec::new())])
            }
            false => (form, stmts),
        }
    }

    fn drop_stmt(&mut self) -> S {
        S::If(self.expr(&DataType::Bool, 0), vec![S::Drop], Vec::new())
    }

    /// An eval step's assignments: at the top level some bind a new field, under a guard each writes one bound.
    fn eval(&mut self, top: bool) -> Option<Vec<S>> {
        let mut out = Vec::new();
        for _ in 0..1 + self.rng.below(3) {
            let ty = self.ty();
            let bound = self.target(&ty).filter(|&id| !self.bindings[id].local);
            let id = match bound {
                Some(id) if !top || self.rng.chance(2) => id,
                _ if top => {
                    let e = self.expr(&ty, 0);
                    out.push(S::Assign(self.field(format!("f{}", self.bindings.len()), ty), e));
                    continue;
                }
                _ => continue,
            };
            out.push(S::Assign(id, self.expr(&ty, 0)));
        }
        (!out.is_empty()).then_some(out)
    }

    /// A field of `ty` a step under a guard may write, or at the top level a new one. Its name is a plain key, since a
    /// step's names can be split at dots where code's keys are not.
    fn out_field(&mut self, ty: DataType, top: bool) -> Option<(usize, bool)> {
        let plain = |b: &Binding| !b.local && !b.name.contains('.') && !b.name.starts_with('_');
        match self.target(&ty).filter(|&id| plain(&self.bindings[id])) {
            Some(id) if !top || self.rng.chance(3) => Some((id, false)),
            _ if top => Some((self.field(format!("f{}", self.bindings.len()), ty), true)),
            _ => None,
        }
    }

    /// A lookup of a text field, its message written to a field.
    fn lookup(&mut self, top: bool) -> Option<S> {
        let input = self.var(&DataType::Str)?;
        let multiplicity = *self
            .rng
            .pick(&[Multiplicity::Best, Multiplicity::First, Multiplicity::All]);
        let ty = match multiplicity {
            Multiplicity::All => arr(DataType::Str),
            _ => DataType::Str,
        };
        let (out, fresh) = self.out_field(ty, top)?;
        let default = *self.rng.pick(&[None, Some("-")]);
        Some(S::Lookup(Lookup {
            input,
            out,
            fresh,
            multiplicity,
            default,
        }))
    }

    /// A parse of a text field, with the eval step that writes the text first at the top level. Under a guard the
    /// parse reads a text field already bound, and is of a kind that declares every key it writes. At the top level it
    /// may write into an object, its fields new: one of its own, `o`, which holds fields already, or `zz`, a key some
    /// records hold as text.
    fn parse_steps(&mut self, top: bool) -> Option<Vec<(Form, Vec<S>)>> {
        let kinds: Vec<Parser> = Parser::ALL.into_iter().filter(|k| top || !k.dynamic()).collect();
        let kind = *self.rng.pick(&kinds);
        let into = (top && self.rng.chance(3)).then(|| match self.rng.below(3) {
            0 => "o".to_string(),
            1 => "zz".to_string(),
            _ => format!("p{}", self.bindings.len()),
        });
        let into_bound = into.as_deref() == Some("o");
        let mut declared = Vec::new();
        for _ in 0..1 + self.rng.below(3) {
            let ty = self
                .rng
                .pick(&[DataType::I64, DataType::F64, DataType::Str, DataType::Bool])
                .clone();
            let field = match &into {
                Some(into) => Some((self.field(format!("{into}.f{}", self.bindings.len()), ty), true)),
                None => self.out_field(ty, top),
            };
            if let Some(d) = field {
                declared.push(d);
            }
        }
        if declared.is_empty() {
            return None;
        }
        let mut out = Vec::new();
        let input = match top {
            true => {
                let text = self.text(kind, into.as_deref(), &declared);
                let id = self.field(format!("f{}", self.bindings.len()), DataType::Str);
                out.push((Form::Eval, vec![S::Assign(id, text)]));
                id
            }
            false => self.var(&DataType::Str)?,
        };
        let parse = Parse {
            kind,
            split_dots: self.rng.chance(2),
            input,
            into,
            into_bound,
            declared,
        };
        out.push((Form::Parser, vec![S::Parse(parse)]));
        Some(out)
    }

    /// A line of `kind` holding a value of each of `declared`, the keys their names.
    fn text(&mut self, kind: Parser, into: Option<&str>, declared: &[(usize, bool)]) -> E {
        let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
        let mut parts = Vec::new();
        let mut before = match kind {
            Parser::Json => "{".to_string(),
            Parser::Cef => "CEF:0|v|p|1|sig|nm|5|".to_string(),
            _ => String::new(),
        };
        for (i, &(id, _)) in declared.iter().enumerate() {
            let (name, ty) = (self.bindings[id].name.clone(), self.bindings[id].ty.clone());
            let name = into.map_or(name.as_str(), |into| &name[into.len() + 1..]).to_string();
            let sep = match (i, kind) {
                (0, _) => "",
                (_, Parser::Json) => ", ",
                (_, Parser::Csv) => ",",
                _ => " ",
            };
            let quote = matches!((kind, &ty), (Parser::Json, DataType::Str));
            before.push_str(sep);
            match kind {
                Parser::Json => before.push_str(&format!("\"{name}\": ")),
                Parser::Kv | Parser::Cef => before.push_str(&format!("{name}=")),
                Parser::Csv | Parser::Grok => {}
            }
            if quote {
                before.push('"');
            }
            parts.push((leak(std::mem::take(&mut before)), self.expr(&ty, 1)));
            if quote {
                before.push('"');
            }
        }
        if kind == Parser::Json {
            before.push('}');
        }
        E::Template(parts, leak(before))
    }
}

// -- writing ------------------------------------------------------------------

/// How a program is written. The default writes it as generated; each other choice keeps what it means.
#[derive(Default, Clone, Copy)]
struct Style {
    rename_locals: bool,
    through_let: bool,
    dead_lets: bool,
    split_steps: bool,
    methods: bool,
    /// `x = x op e` as `x op= e`, and `x = x + 1` as `x++`.
    compound: bool,
    /// Each step as the code it means.
    as_code: bool,
}

struct Writer<'p> {
    p: &'p Program,
    style: Style,
    temps: usize,
    /// Whether the statement being written is in a `try`.
    trying: bool,
}

impl Writer<'_> {
    fn name(&self, id: usize) -> String {
        let b = &self.p.bindings[id];
        if b.local && self.style.rename_locals {
            format!("z{id}")
        } else {
            b.name.clone()
        }
    }

    /// `e` as an operand a member, an index or a method can follow.
    fn atom(&self, e: &E) -> String {
        match e {
            E::Var(id) => self.name(*id),
            E::Lit(l) if l == "o" || l == "$" => l.clone(),
            e => format!("({})", self.expr(e)),
        }
    }

    fn expr(&self, e: &E) -> String {
        let callee = |c: &Callee| match c {
            Callee::Row(ri) => ROWS[*ri].name.to_string(),
            Callee::Prelude(f) => f.to_string(),
            Callee::Bound(id) => self.name(*id),
        };
        match e {
            E::Lit(l) => l.clone(),
            E::Var(id) => self.name(*id),
            E::Member(base, k) => format!("{}.{k}", self.atom(base)),
            E::Index(base, i) => format!("{}[{}]", self.atom(base), self.expr(i)),
            // A function the program binds is called by its name alone.
            E::Call(c, args) if self.style.methods && !args.is_empty() && !matches!(c, Callee::Bound(_)) => {
                let rest: Vec<String> = args[1..].iter().map(|a| self.expr(a)).collect();
                format!("{}.{}({})", self.atom(&args[0]), callee(c), rest.join(", "))
            }
            E::Call(c, args) => {
                let args: Vec<String> = args.iter().map(|a| self.expr(a)).collect();
                format!("{}({})", callee(c), args.join(", "))
            }
            E::Regex(r) => r.to_string(),
            E::Bin(op, l, r) => format!("({} {} {})", self.expr(l), op.symbol(), self.expr(r)),
            // The operand in its own parentheses: `-(-3)`, never the decrement `--3`.
            E::Un(op, x) => format!("{}({})", op.symbol(), self.expr(x)),
            E::Tern(c, t, f) => format!("({} ? {} : {})", self.expr(c), self.expr(t), self.expr(f)),
            E::Arr(items) => {
                let items: Vec<String> = items.iter().map(|a| self.expr(a)).collect();
                format!("[{}]", items.join(", "))
            }
            E::Obj(entries) => {
                let entries: Vec<String> = entries
                    .iter()
                    .map(|(k, v)| match k {
                        Some(k) => format!("{k}: {}", self.expr(v)),
                        None => format!("...{}", self.atom(v)),
                    })
                    .collect();
                format!("{{{}}}", entries.join(", "))
            }
            E::As(x, ty) => format!("({} as {})", self.expr(x), ty.spell()),
            E::Template(parts, tail) => {
                let mut out = String::from("`");
                for (text, x) in parts {
                    out.push_str(&format!("{text}${{{}}}", self.expr(x)));
                }
                out.push_str(tail);
                out.push('`');
                out
            }
            // An object body takes parentheses, as in TypeScript.
            E::Arrow(x, body) => format!("({}) => ({})", self.name(*x), self.expr(body)),
        }
    }

    fn stmt(&mut self, s: &S, out: &mut String) {
        if self.style.dead_lets {
            out.push_str(&format!("let dead{} = \"{}\"; ", self.temps, self.temps));
            self.temps += 1;
        }
        match s {
            S::Assign(id, e) if self.style.through_let && !self.trying => {
                let t = self.temps;
                self.temps += 1;
                let ty = self.p.bindings[*id].ty.spell();
                let (e, name) = (self.expr(e), self.name(*id));
                out.push_str(&format!("let tmp{t}: {ty} = {e}; {name} = tmp{t}; "));
            }
            S::Assign(id, e) => out.push_str(&format!("{} = {}; ", self.name(*id), self.expr(e))),
            S::Let(id, e) => out.push_str(&format!("let {} = {}; ", self.name(*id), self.expr(e))),
            S::If(c, then, else_) => {
                out.push_str(&format!("if ({}) {{ ", self.expr(c)));
                then.iter().for_each(|s| self.stmt(s, out));
                out.push_str("} ");
                if !else_.is_empty() {
                    out.push_str("else { ");
                    else_.iter().for_each(|s| self.stmt(s, out));
                    out.push_str("} ");
                }
            }
            S::For(elem, over, body) => {
                out.push_str(&format!("for (let {} of {}) {{ ", self.name(*elem), self.expr(over)));
                body.iter().for_each(|s| self.stmt(s, out));
                out.push_str("} ");
            }
            S::While(c, n, body) => {
                let c = self.name(*c);
                out.push_str(&format!("let {c} = 0; while ({c} < {n}) {{ {c} = {c} + 1; "));
                body.iter().for_each(|s| self.stmt(s, out));
                out.push_str("} ");
            }
            S::Leave(how) => out.push_str(&format!("{how}; ")),
            S::Switch(on, cases, default) => {
                out.push_str(&format!("switch ({}) {{ ", self.expr(on)));
                for (v, body, brk) in cases {
                    out.push_str(&format!("case {}: ", self.expr(v)));
                    body.iter().for_each(|s| self.stmt(s, out));
                    if *brk {
                        out.push_str("break; ");
                    }
                }
                if let Some(body) = default {
                    out.push_str("default: ");
                    body.iter().for_each(|s| self.stmt(s, out));
                }
                out.push_str("} ");
            }
            S::Function(f, params, body, value, typed) => {
                let types = self.p.bindings[*f].params.clone().expect("a function has parameters");
                let params: Vec<String> = params
                    .iter()
                    .zip(&types)
                    .map(|(&p, (ty, written))| match written {
                        true => format!("{}: {}", self.name(p), ty.spell()),
                        false => self.name(p),
                    })
                    .collect();
                let ret = match typed {
                    true => format!(": {}", self.p.bindings[*f].ty.spell()),
                    false => String::new(),
                };
                out.push_str(&format!("function {}({}){ret} {{ ", self.name(*f), params.join(", ")));
                body.iter().for_each(|s| self.stmt(s, out));
                out.push_str(&format!("{} }} ", self.expr(value)));
            }
            S::Arrow(f, params, value) => {
                let params: Vec<String> = params.iter().map(|&p| self.name(p)).collect();
                out.push_str(&format!(
                    "let {} = ({}) => ({}); ",
                    self.name(*f),
                    params.join(", "),
                    self.expr(value)
                ));
            }
            S::Return(e) => out.push_str(&format!("return {}; ", self.expr(e))),
            S::Try(body, catch, finally) => {
                let outer = std::mem::replace(&mut self.trying, true);
                out.push_str("try { ");
                body.iter().for_each(|s| self.stmt(s, out));
                out.push_str("} ");
                if let Some((e, body)) = catch {
                    out.push_str(&format!("catch ({}) {{ ", self.name(*e)));
                    body.iter().for_each(|s| self.stmt(s, out));
                    out.push_str("} ");
                }
                if let Some(body) = finally {
                    out.push_str("finally { ");
                    body.iter().for_each(|s| self.stmt(s, out));
                    out.push_str("} ");
                }
                self.trying = outer;
            }
            S::Throw(e) => out.push_str(&format!("throw {}; ", self.name(*e))),
            S::Put(id, place, e) => out.push_str(&format!("{} = {}; ", self.place(*id, place), self.expr(e))),
            S::Update(id, place, op, e) => {
                let to = match place {
                    Some(place) => self.place(*id, place),
                    None => self.name(*id),
                };
                let one = matches!(e, E::Lit(l) if l == "1");
                out.push_str(&match (self.style.compound, op) {
                    (true, BinOp::Add) if one => format!("{to}++; "),
                    (true, BinOp::Sub) if one => format!("{to}--; "),
                    (true, op) => format!("{to} {}= {}; ", op.symbol(), self.expr(e)),
                    (false, op) => format!("{to} = ({to} {} {}); ", op.symbol(), self.expr(e)),
                });
            }
            S::Merge(into, e) => {
                let into = into.map_or("$".to_string(), |id| self.name(id));
                out.push_str(&format!("Object.assign({into}, {}); ", self.expr(e)));
            }
            S::Replace(e) => out.push_str(&format!("$ = {}; ", self.expr(e))),
            S::Typed(id, e) => out.push_str(&format!(
                "{}: {} = {}; ",
                self.name(*id),
                self.p.bindings[*id].ty.spell(),
                self.expr(e)
            )),
            S::Delete(ids) => {
                let names: Vec<String> = ids.iter().map(|&id| self.name(id)).collect();
                out.push_str(&format!("delete({}); ", names.join(", ")));
            }
            S::Move(from, to) => out.push_str(&format!("move({}, {}); ", self.name(*from), self.name(*to))),
            S::Unroll(id, keep) => out.push_str(&format!("unroll({}, {keep}); ", self.name(*id))),
            S::Drop => out.push_str("drop(); "),
            S::Bare(e) => out.push_str(&format!("{}; ", self.expr(e))),
            S::Parse(p) => {
                // What `plan::lower` writes for an object the open record may hold as a key: it, converted to an
                // object, before the fields that convert under it.
                if let Some(into) = p.into.as_ref().filter(|_| !p.into_bound) {
                    out.push_str(&format!("{into}: {{...}} = {into}; "));
                }
                if !p.kind.dynamic() {
                    self.declare(p.declared.iter().copied(), out);
                }
                let names: Vec<String> = p
                    .declared
                    .iter()
                    .map(|&(id, _)| p.key(&self.name(id)).to_string())
                    .collect();
                let dots = format!("splitDots: {}", p.split_dots);
                let input = self.name(p.input);
                let call = match p.kind {
                    Parser::Json | Parser::Cef => format!("{}({input}, {{{dots}}})", p.kind.name()),
                    Parser::Kv => format!(
                        "kv({input}, {{pairSep: \" \", kvSep: \"=\", quote: \"\\\"\", escapes: false, nextKey: true, {dots}}})"
                    ),
                    Parser::Csv => format!(
                        "csv({input}, [{}], {{delimiter: \",\", quote: \"\\\"\", escape: null, squeeze: false, trim: false, \
                         {dots}}})",
                        names.iter().map(|n| format!("\"{n}\"")).collect::<Vec<_>>().join(", ")
                    ),
                    Parser::Grok => format!("grok({input}, [\"{}\"], {{{dots}}})", grok_pattern(&names)),
                };
                out.push_str(&format!(
                    "Object.assign({}, {call}); ",
                    p.into.as_deref().unwrap_or("$")
                ));
                if p.kind.dynamic() {
                    self.declare(p.declared.iter().copied(), out);
                }
            }
            S::Lookup(l) => {
                self.declare([(l.out, l.fresh)], out);
                let pick = match l.multiplicity {
                    Multiplicity::Best => "",
                    Multiplicity::First => "first: true, ",
                    Multiplicity::All => "all: true, ",
                };
                let default = l.default.map_or("null".to_string(), |d| format!("\"{d}\""));
                out.push_str(&format!(
                    "Object.assign($, lookup(\"{}\", {}, {{match: \"exact\", column: \"code\", {pick}fields: {{{}: [\"message\", {default}]}}}})); ",
                    TABLE.0,
                    self.name(l.input),
                    self.name(l.out)
                ));
            }
        }
    }

    /// `name: type = name;` for each field a step writes first: after `HEAD`'s parse the name may be a key of the
    /// record, which this converts. A dynamic parse converts after it lays its keys, any other step before it writes.
    fn declare(&self, fields: impl IntoIterator<Item = (usize, bool)>, out: &mut String) {
        for (id, fresh) in fields {
            if fresh {
                let name = self.name(id);
                out.push_str(&format!("{name}: {} = {name}; ", self.p.bindings[id].ty.spell()));
            }
        }
    }

    fn place(&self, id: usize, place: &Place) -> String {
        match place {
            Place::Key(k) => format!("{}.{k}", self.name(id)),
            Place::At(i) => format!("{}[{}]", self.name(id), self.expr(i)),
        }
    }

    /// The pipeline's steps, `HEAD` first.
    fn steps(mut self) -> Vec<Step> {
        let mut out = vec![code(0, HEAD)];
        let mut id = 1;
        for (step, form) in self.p.steps.iter().zip(&self.p.forms) {
            if self.style.as_code || matches!(form, Form::Code) {
                for part in split(self.p, step, self.style.split_steps) {
                    let mut text = String::new();
                    part.iter().for_each(|s| self.stmt(s, &mut text));
                    out.push(code(id, &text));
                    id += 1;
                }
            } else {
                out.push(self.step(form, step, &mut id));
            }
        }
        out
    }

    /// `stmts` written as the pipeline step `form` says, numbered from `id`.
    fn step(&mut self, form: &Form, stmts: &[S], id: &mut u32) -> Step {
        let this = *id;
        *id += 1;
        let func = |kind| Step::Function(FunctionConfig::new(this, kind));
        match (form, stmts) {
            (Form::Eval, _) => func(FunctionKind::Eval {
                assignments: stmts
                    .iter()
                    .map(|s| match s {
                        S::Assign(to, e) => (self.name(*to), self.expr(e)),
                        _ => unreachable!("an eval step holds assignments"),
                    })
                    .collect(),
            }),
            (Form::Drop, [S::If(c, ..)]) => func(FunctionKind::Drop { cond: self.expr(c) }),
            (Form::Rename, _) => func(FunctionKind::Rename {
                pairs: stmts
                    .iter()
                    .map(|s| match s {
                        S::Move(from, to) => (self.name(*from), self.name(*to)),
                        _ => unreachable!("a rename step holds moves"),
                    })
                    .collect(),
            }),
            (Form::Unroll, [S::Unroll(f, keep)]) => func(FunctionKind::Unroll {
                field: self.name(*f),
                keep_parent_fields: *keep,
            }),
            (Form::Parser, [S::Parse(p)]) => {
                let names: Vec<String> = p
                    .declared
                    .iter()
                    .map(|&(id, _)| p.key(&self.name(id)).to_string())
                    .collect();
                let split_dots = p.split_dots;
                let kind = match p.kind {
                    Parser::Json => ParserKind::Json { split_dots },
                    Parser::Cef => ParserKind::Cef { split_dots },
                    Parser::Kv => ParserKind::KeyValue(KvConfig {
                        split_dots,
                        ..KvConfig::default()
                    }),
                    Parser::Csv => ParserKind::Csv(CsvConfig {
                        headers: names.iter().map(|n| Some(n.clone())).collect(),
                        split_dots,
                        ..CsvConfig::default()
                    }),
                    Parser::Grok => ParserKind::Grok(GrokConfig {
                        patterns: vec![grok_pattern(&names)],
                        custom_patterns: Vec::new(),
                        split_dots,
                    }),
                };
                func(FunctionKind::Parser {
                    input: self.name(p.input),
                    into: p.into.clone().unwrap_or_default(),
                    kind,
                    declared: p
                        .declared
                        .iter()
                        .map(|&(id, _)| DeclaredField {
                            name: p.key(&self.name(id)).to_string(),
                            ty: self.p.bindings[id].ty.clone(),
                            time_source: None,
                        })
                        .collect(),
                })
            }
            (Form::Lookup, [S::Lookup(l)]) => {
                let ty = match &self.p.bindings[l.out].ty {
                    DataType::Array(elem) => (**elem).clone(),
                    ty => ty.clone(),
                };
                func(FunctionKind::Lookup {
                    input: self.name(l.input),
                    table: TABLE.0.to_string(),
                    match_kind: MatchKind::Exact,
                    multiplicity: l.multiplicity,
                    key_column: "code".to_string(),
                    outputs: vec![LookupOutput {
                        source: "message".to_string(),
                        field: self.name(l.out),
                        default: l.default.map(str::to_string),
                        ty,
                        time_source: None,
                    }],
                })
            }
            (Form::Filter(inner), [S::If(c, body, _)]) => {
                *id -= 1;
                match self.step(inner, body, id) {
                    Step::Function(mut f) => {
                        f.filter = Some(self.expr(c));
                        Step::Function(f)
                    }
                    Step::If { .. } => unreachable!("a filter is on a function"),
                }
            }
            (Form::If(then, else_), [S::If(c, then_s, else_s)]) => {
                let mut branch = |forms: &[(Form, usize)], mut stmts: &[S], id: &mut u32| {
                    let mut out = Vec::new();
                    for (form, n) in forms {
                        out.push(self.step(form, &stmts[..*n], id));
                        stmts = &stmts[*n..];
                    }
                    out
                };
                let then = branch(then, then_s, id);
                let else_ = branch(else_, else_s, id);
                Step::If {
                    id: this,
                    cond: self.expr(c),
                    then,
                    else_,
                }
            }
            _ => unreachable!("a step's statements are of its form"),
        }
    }
}

/// A grok pattern capturing one run of non-space characters into each of `names`, a space between each. The first
/// is named through `SHARED_PATTERN`, the bundle's, and the rest through a built-in, so one pattern resolves both.
fn grok_pattern(names: &[String]) -> String {
    names
        .iter()
        .enumerate()
        .map(|(i, n)| match i {
            0 => format!("%{{{}:{n}}}", SHARED_PATTERN.0),
            _ => format!("%{{NOTSPACE:{n}}}"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// A grok pattern the bundle declares, which every generated grok names.
const SHARED_PATTERN: (&str, &str) = ("TOKEN", "%{NOTSPACE}");

/// A statement or an expression of a program, as `each` meets it.
enum Node<'a> {
    S(&'a S),
    E(&'a E),
}

/// Calls `f` on each statement in `stmts` and each expression in them, outermost first.
fn each<'a>(stmts: &'a [S], f: &mut dyn FnMut(Node<'a>)) {
    fn expr<'a>(e: &'a E, f: &mut dyn FnMut(Node<'a>)) {
        f(Node::E(e));
        match e {
            E::Lit(_) | E::Var(_) | E::Regex(_) => {}
            E::Member(x, _) | E::Un(_, x) | E::As(x, _) | E::Arrow(_, x) => expr(x, f),
            E::Index(l, r) | E::Bin(_, l, r) => {
                expr(l, f);
                expr(r, f);
            }
            E::Tern(c, t, e) => [c, t, e].into_iter().for_each(|x| expr(x, f)),
            E::Call(_, items) | E::Arr(items) => items.iter().for_each(|x| expr(x, f)),
            E::Obj(entries) => entries.iter().for_each(|(_, x)| expr(x, f)),
            E::Template(parts, _) => parts.iter().for_each(|(_, x)| expr(x, f)),
        }
    }
    for s in stmts {
        f(Node::S(s));
        match s {
            S::Assign(_, e) | S::Let(_, e) => expr(e, f),
            S::If(c, then, else_) => {
                expr(c, f);
                each(then, f);
                each(else_, f);
            }
            S::For(_, over, body) => {
                expr(over, f);
                each(body, f);
            }
            S::While(_, _, body) => each(body, f),
            S::Leave(_) | S::Throw(_) => {}
            S::Switch(on, cases, default) => {
                expr(on, f);
                for (v, body, _) in cases {
                    expr(v, f);
                    each(body, f);
                }
                default.iter().for_each(|body| each(body, f));
            }
            S::Function(_, _, body, value, _) => {
                each(body, f);
                expr(value, f);
            }
            S::Arrow(_, _, value) | S::Return(value) => expr(value, f),
            S::Try(body, catch, finally) => {
                each(body, f);
                catch.iter().for_each(|(_, body)| each(body, f));
                finally.iter().for_each(|body| each(body, f));
            }
            S::Put(_, place, e) | S::Update(_, Some(place), _, e) => {
                if let Place::At(i) = place {
                    expr(i, f);
                }
                expr(e, f);
            }
            S::Update(_, None, _, e) | S::Merge(_, e) | S::Replace(e) | S::Typed(_, e) | S::Bare(e) => expr(e, f),
            S::Delete(_) | S::Move(..) | S::Unroll(..) | S::Drop | S::Parse(_) | S::Lookup(_) => {}
        }
    }
}

/// The locals `s` reads or writes that it does not bind itself.
fn free_locals(p: &Program, s: &S, out: &mut BTreeSet<usize>) {
    let (mut used, mut bound) = (BTreeSet::new(), BTreeSet::new());
    // A block's own locals are bound after the uses that came before them, so a local a block reads is free unless the
    // block bound it; its bindings are collected, and taken away at the end.
    each(std::slice::from_ref(s), &mut |n| match n {
        Node::S(S::Assign(id, _) | S::Throw(id) | S::Put(id, ..) | S::Update(id, ..) | S::Merge(Some(id), _))
        | Node::E(E::Var(id) | E::Call(Callee::Bound(id), _))
            if p.bindings[*id].local =>
        {
            used.insert(*id);
        }
        Node::S(S::Let(id, _) | S::For(id, ..) | S::While(id, ..)) | Node::E(E::Arrow(id, _)) => {
            bound.insert(*id);
        }
        Node::S(S::Function(id, params, ..) | S::Arrow(id, params, _)) => {
            bound.insert(*id);
            bound.extend(params);
        }
        Node::S(S::Try(_, Some((id, _)), _)) => {
            bound.insert(*id);
        }
        _ => {}
    });
    out.extend(used.difference(&bound));
}

/// `step` cut before each statement no local crosses into, or whole.
fn split<'s>(p: &Program, step: &'s [S], cut: bool) -> Vec<&'s [S]> {
    if !cut {
        return vec![step];
    }
    let mut parts = Vec::new();
    let mut start = 0;
    for k in 1..step.len() {
        let mut later = BTreeSet::new();
        step[k..].iter().for_each(|s| free_locals(p, s, &mut later));
        if later.is_empty() {
            parts.push(&step[start..k]);
            start = k;
        }
    }
    parts.push(&step[start..]);
    parts
}

// -- coverage -----------------------------------------------------------------

/// The constructs `covers` names, besides each operator and each row of `ROWS`.
const CONSTRUCTS: &[&str] = &[
    "ternary",
    "coalesce null",
    "member",
    "index",
    "as",
    "template",
    "array",
    "object",
    "spread",
    "map",
    "filter",
    "regex",
    "variant",
    "null write",
    "for",
    "if",
    "else",
    "let",
    "shadow",
    "while",
    "break",
    "continue",
    "switch",
    "fallthrough",
    "default",
    "function",
    "typed function",
    "return",
    "arrow",
    "call",
    "try",
    "catch",
    "finally",
    "throw",
    "key write",
    "index write",
    "update",
    "compound",
    "increment",
    "assign object",
    "assign record",
    "replace record",
    "convert key",
    "typed write",
    "convert",
    "delete",
    "move",
    "unroll",
    "unroll keep",
    "drop",
    "bare call",
    "parse into",
    "parse into an object",
    "parse into a record key",
];

/// What `p` writes, by the names `expected` gives.
fn covers(p: &Program) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let fields: BTreeSet<&str> = p
        .bindings
        .iter()
        .filter(|b| !b.local)
        .map(|b| b.name.as_str())
        .collect();
    for form in &p.forms {
        form_covers(form, &mut out);
    }
    for step in &p.steps {
        each(step, &mut |n| {
            let name: String = match n {
                Node::S(S::Assign(_, E::Lit(l))) if l == "null" => "null write".into(),
                Node::S(S::Let(id, _)) if fields.contains(p.bindings[*id].name.as_str()) => "shadow".into(),
                Node::S(S::Let(..)) => "let".into(),
                Node::S(S::If(_, _, e)) if !e.is_empty() => "else".into(),
                Node::S(S::If(..)) => "if".into(),
                Node::S(S::For(..)) => "for".into(),
                Node::S(S::While(..)) => "while".into(),
                Node::S(S::Leave(how)) => how.to_string(),
                Node::S(S::Switch(_, cases, default)) => {
                    if cases.iter().any(|(_, _, brk)| !brk) {
                        out.insert("fallthrough".into());
                    }
                    if default.is_some() {
                        out.insert("default".into());
                    }
                    "switch".into()
                }
                Node::S(S::Function(f, _, _, _, typed)) => {
                    if *typed || p.bindings[*f].params.iter().flatten().any(|(_, w)| *w) {
                        out.insert("typed function".into());
                    }
                    "function".into()
                }
                Node::S(S::Arrow(..)) => "arrow".into(),
                Node::S(S::Return(_)) => "return".into(),
                Node::S(S::Try(_, catch, finally)) => {
                    if catch.is_some() {
                        out.insert("catch".into());
                    }
                    if finally.is_some() {
                        out.insert("finally".into());
                    }
                    "try".into()
                }
                Node::S(S::Throw(_)) => "throw".into(),
                Node::S(S::Put(_, Place::Key(_), _)) => "key write".into(),
                Node::S(S::Put(_, Place::At(_), _)) => "index write".into(),
                Node::S(S::Update(_, place, op, e)) => {
                    out.insert("compound".into());
                    if place.is_some() {
                        out.insert("update".into());
                    }
                    match (op, e) {
                        (BinOp::Add | BinOp::Sub, E::Lit(l)) if l == "1" => "increment".into(),
                        _ => return,
                    }
                }
                Node::S(S::Merge(Some(_), _)) => "assign object".into(),
                Node::S(S::Merge(None, _)) => "assign record".into(),
                Node::S(S::Replace(_)) => "replace record".into(),
                Node::S(S::Typed(id, e)) => match e {
                    E::Var(v) if v == id => "convert key".into(),
                    E::Var(v) if p.bindings[*v].ty == DataType::Variant => "convert".into(),
                    _ => "typed write".into(),
                },
                Node::S(S::Delete(_)) => "delete".into(),
                Node::S(S::Move(..)) => "move".into(),
                Node::S(S::Unroll(_, keep)) => match keep {
                    true => "unroll keep".into(),
                    false => "unroll".into(),
                },
                Node::S(S::Drop) => "drop".into(),
                Node::S(S::Bare(_)) => "bare call".into(),
                Node::S(S::Parse(p)) if p.into_bound => "parse into an object".into(),
                Node::S(S::Parse(p)) if p.into.as_deref() == Some("zz") => "parse into a record key".into(),
                Node::S(S::Parse(p)) if p.into.is_some() => "parse into".into(),
                Node::S(S::Parse(p)) => format!("{} call", p.kind.name()),
                Node::S(S::Lookup(_)) => "lookup call".into(),
                Node::S(S::Assign(..)) => return,
                Node::E(e) => match e {
                    E::Call(Callee::Row(ri), _) => format!("row {ri}"),
                    E::Call(Callee::Prelude(f), _) => f.to_string(),
                    E::Call(Callee::Bound(_), _) => "call".into(),
                    E::Bin(BinOp::Coalesce, l, _) if matches!(&**l, E::Lit(n) if n == "null") => "coalesce null".into(),
                    E::Bin(op, ..) => format!("op {}", op.symbol()),
                    E::Un(op, _) => format!("unary {}", op.symbol()),
                    E::Tern(..) => "ternary".into(),
                    E::Member(..) => "member".into(),
                    E::Index(base, _) if matches!(&**base, E::Lit(n) if n == "o" || n == "$") => "variant".into(),
                    E::Index(..) => "index".into(),
                    E::As(..) => "as".into(),
                    E::Template(..) => "template".into(),
                    E::Arr(_) => "array".into(),
                    E::Obj(entries) if entries.iter().any(|(k, _)| k.is_none()) => "spread".into(),
                    E::Obj(_) => "object".into(),
                    E::Regex(_) => "regex".into(),
                    E::Lit(l) if l == "o.r" || l == "$.zz" => "variant".into(),
                    E::Lit(l) if l == NOW => {
                        let ri = ROWS
                            .iter()
                            .position(|r| r.func == StdFunc::Now)
                            .expect("now() is a row");
                        format!("row {ri}")
                    }
                    _ => return,
                },
            };
            out.insert(name);
        });
    }
    out
}

/// The steps `covers` names by their form.
const STEPS: &[&str] = &[
    "code step",
    "eval step",
    "drop step",
    "rename step",
    "unroll step",
    "parser step",
    "lookup step",
    "step filter",
    "if step",
    "else step",
];

fn form_covers(form: &Form, out: &mut BTreeSet<String>) {
    let name = match form {
        Form::Code => "code step",
        Form::Eval => "eval step",
        Form::Drop => "drop step",
        Form::Rename => "rename step",
        Form::Unroll => "unroll step",
        Form::Parser => "parser step",
        Form::Lookup => "lookup step",
        Form::Filter(inner) => {
            form_covers(inner, out);
            "step filter"
        }
        Form::If(then, else_) => {
            then.iter().chain(else_).for_each(|(f, _)| form_covers(f, out));
            match else_.is_empty() {
                true => "if step",
                false => "else step",
            }
        }
    };
    out.insert(name.to_string());
}

/// Every name `covers` gives over the seed range: each row of `ROWS`, each operator, each construct, each step and
/// each parse call.
fn expected() -> BTreeSet<String> {
    let mut out: BTreeSet<String> = CONSTRUCTS.iter().chain(STEPS).map(|c| c.to_string()).collect();
    out.extend(Parser::ALL.iter().map(|k| format!("{} call", k.name())));
    out.insert("lookup call".into());
    out.extend((0..ROWS.len()).map(|ri| format!("row {ri}")));
    out.extend(
        BinOp::ALL
            .iter()
            .filter(|op| **op != BinOp::Coalesce)
            .map(|op| format!("op {}", op.symbol())),
    );
    out.extend(UnOp::ALL.iter().map(|op| format!("unary {}", op.symbol())));
    out
}

/// A name of `expected` as a failure shows it: a row with its function and parameters.
fn show_covered(name: &str) -> String {
    match name.strip_prefix("row ").and_then(|ri| ri.parse::<usize>().ok()) {
        Some(ri) => format!("{}{}", ROWS[ri].name, ROWS[ri].describe()),
        None => name.to_string(),
    }
}

// -- breaking -----------------------------------------------------------------

/// The ways `break_at` breaks a statement, each a compile error whatever the program around it.
const BREAKS: &[&str] = &[
    "a write of another type",
    "an operator on an operand it does not take",
    "a condition that is not bool",
    "a local declared twice in its block",
];

/// Calls `f` on each statement, by its block and place, in program order, until `f` says it is done. A function's body
/// is left out: it compiles only where the function is called.
fn visit(block: &mut Vec<S>, f: &mut dyn FnMut(&mut Vec<S>, usize) -> bool) -> bool {
    let mut i = 0;
    while i < block.len() {
        if f(block, i) {
            return true;
        }
        let done = match &mut block[i] {
            S::If(_, then, else_) => visit(then, f) || visit(else_, f),
            S::For(_, _, body) | S::While(_, _, body) => visit(body, f),
            S::Switch(_, cases, default) => {
                cases.iter_mut().any(|(_, body, _)| visit(body, f)) || default.as_mut().is_some_and(|b| visit(b, f))
            }
            S::Try(body, catch, finally) => {
                visit(body, f)
                    || catch.as_mut().is_some_and(|(_, b)| visit(b, f))
                    || finally.as_mut().is_some_and(|b| visit(b, f))
            }
            _ => false,
        };
        if done {
            return true;
        }
        i += 1;
    }
    false
}

/// `e` as a value of another type than `ty`, which no write to a `ty` name takes; `None` for a variant, which takes
/// any.
fn retyped(ty: &DataType, e: E) -> Option<E> {
    use DataType as T;
    let call = |f: &str, args: Vec<E>| {
        let ri = ROWS.iter().position(|r| r.name == f).expect("a function of ROWS");
        E::Call(Callee::Row(ri), args)
    };
    let int = |c: E| E::Tern(Box::new(c), Box::new(E::Lit("1".into())), Box::new(E::Lit("0".into())));
    Some(match ty {
        T::I64 | T::F64 => call("toString", vec![e]),
        T::Str | T::Bytes | T::Array(_) => call("length", vec![e]),
        T::Bool => int(e),
        T::DateTime => call("epochMillis", vec![e]),
        T::Duration => call("countIn", vec![e, E::Lit("\"s\"".into())]),
        T::Struct(..) => E::Lit("1".into()),
        T::Variant | T::Null => return None,
    })
}

/// `p` with one statement broken the way `BREAKS[kind]` names, or `None` where it has no statement to break so.
fn break_at(p: &Program, kind: usize, rng: &mut Rng) -> Option<Program> {
    let mut steps = p.steps.clone();
    let bindings = &p.bindings;
    let declared = declared().len();
    // A field's first write binds it, at any type, as does a local's let as `[]`'s: only a write to a name already
    // bound has a type to break. A variant takes a value of any type, and `null` is of every type, wrapped or not.
    let mut empty = BTreeSet::new();
    for step in &p.steps {
        each(step, &mut |n| {
            if let Node::S(S::Let(id, E::Lit(l))) = n
                && l == "[]"
            {
                empty.insert(*id);
            }
        });
    }
    let fits = |s: &S, first: &mut BTreeSet<usize>| match (kind, s) {
        (0 | 1, S::Assign(id, e)) => {
            let bound = (bindings[*id].local && !empty.contains(id)) || *id < declared;
            let rebinds = bound || !first.insert(*id);
            let null = matches!(e, E::Lit(l) if l == "null");
            rebinds && !null && !matches!(bindings[*id].ty, DataType::Variant)
        }
        (2, S::If(..)) | (3, S::Let(..)) => true,
        _ => false,
    };
    let (mut n, mut first) = (0, BTreeSet::new());
    for step in &mut steps {
        visit(step, &mut |block, i| {
            n += usize::from(fits(&block[i], &mut first));
            false
        });
    }
    if n == 0 {
        return None;
    }
    let (mut k, mut first) = (rng.below(n), BTreeSet::new());
    let take = |e: &mut E| std::mem::replace(e, E::Lit(String::new()));
    for step in &mut steps {
        let done = visit(step, &mut |block, i| {
            if !fits(&block[i], &mut first) {
                return false;
            }
            if k > 0 {
                k -= 1;
                return false;
            }
            let int = |c: E| E::Tern(Box::new(c), Box::new(E::Lit("1".into())), Box::new(E::Lit("0".into())));
            match &mut block[i] {
                S::Assign(id, e) if kind == 0 => {
                    let old = take(e);
                    *e = retyped(&bindings[*id].ty, old).expect("a variant is never broken");
                }
                S::Assign(id, e) => {
                    let old = Box::new(take(e));
                    *e = match bindings[*id].ty {
                        DataType::Bool => E::Bin(BinOp::Sub, Box::new(E::Lit("0".into())), old),
                        _ => E::Un(UnOp::Not, old),
                    };
                }
                S::If(c, ..) => *c = int(take(c)),
                S::Let(..) => {
                    let again = block[i].clone();
                    block.insert(i + 1, again);
                }
                _ => unreachable!("no break fits a loop, a switch, a function or a try"),
            }
            true
        });
        if done {
            break;
        }
    }
    Some(Program {
        bindings: p.bindings.clone(),
        steps,
        forms: p.forms.clone(),
    })
}

// -- running ------------------------------------------------------------------

/// `steps` as a pipeline with `TABLE` among its lookup tables and `SHARED_PATTERN` among its grok patterns.
fn with_table(steps: Vec<Step>) -> PipelineConfig {
    let mut p = pipeline(steps);
    let table = LookupSource::Inline {
        headers: vec!["code".into(), "message".into()],
        rows: TABLE
            .1
            .iter()
            .map(|(c, m)| vec![c.to_string(), m.to_string()])
            .collect(),
    };
    p.lookup_tables.insert(TABLE.0.into(), Ok(table));
    p.grok_patterns.insert(SHARED_PATTERN.0.into(), SHARED_PATTERN.1.into());
    p
}

/// `steps` as a failure shows them, one a line.
fn show(steps: &[Step]) -> String {
    fn one(s: &Step, out: &mut Vec<String>) {
        match s {
            Step::Function(FunctionConfig {
                kind: FunctionKind::Code { body },
                ..
            }) => out.push(body.clone()),
            Step::Function(f) => out.push(match &f.filter {
                Some(c) => format!("{:?} where {c}", f.kind),
                None => format!("{:?}", f.kind),
            }),
            Step::If { cond, then, else_, .. } => {
                out.push(format!("if ({cond}) {{"));
                then.iter().for_each(|s| one(s, out));
                out.push("} else {".into());
                else_.iter().for_each(|s| one(s, out));
                out.push("}".into());
            }
        }
    }
    let mut out = Vec::new();
    steps.iter().for_each(|s| one(s, &mut out));
    out.join("\n  ")
}

fn records(plan: &Plan, docs: &[&str]) -> Vec<Value> {
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = raw_batch(plan, docs, &mut pool);
    let out = {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        run_to_json(plan, &mut batch, &mut ctx)
            .iter()
            .map(|r| serde_json::from_slice(r).unwrap())
            .collect()
    };
    batch.reclaim(&mut pool);
    out
}

/// Whether a `while` in a run of `records` stopped at its budget, which is time spent per batch: such a run is not
/// repeatable, so no other run is held to it.
fn timed_out(records: &[Value]) -> bool {
    records.iter().any(|r| r.to_string().contains("LoopBudgetExceeded"))
}

/// What `seed`'s program covers where it compiles, `None` where it is refused or a run is not repeatable, or why its
/// runs disagree.
fn check(seed: u64) -> Result<Option<BTreeSet<String>>, String> {
    let program = Gen::new(seed).program();
    let write = |style: Style| {
        Writer {
            p: &program,
            style,
            temps: 0,
            trying: false,
        }
        .steps()
    };
    let source = write(Style::default());
    let Ok(plan) = compile(&with_table(source.clone()), &SourceMeta::KAFKA) else {
        return Ok(None);
    };
    let together = records(&plan, DOCS);
    if timed_out(&together) {
        return Ok(None);
    }
    // A `drop()` or an `unroll` changes how many rows a record gives, so the records' rows alone are laid end to end.
    let mut alone = Vec::new();
    for doc in DOCS {
        let rows = records(&plan, &[doc]);
        if timed_out(&rows) {
            return Ok(None);
        }
        alone.extend(rows);
    }
    if alone != together {
        let i = (0..)
            .find(|&i| alone.get(i) != together.get(i))
            .expect("the rows differ");
        let row = |rows: &[Value]| rows.get(i).map_or("nothing".to_string(), Value::to_string);
        return Err(format!(
            "row {i} alone gives {} and in a batch {}\n  {}",
            row(&alone),
            row(&together),
            show(&source)
        ));
    }
    let one = |f: fn(&mut Style)| {
        let mut s = Style::default();
        f(&mut s);
        s
    };
    let rewrites = [
        ("locals renamed", one(|s| s.rename_locals = true)),
        ("writes through a let", one(|s| s.through_let = true)),
        ("dead lets", one(|s| s.dead_lets = true)),
        ("steps split", one(|s| s.split_steps = true)),
        ("calls written as methods", one(|s| s.methods = true)),
        ("assigning operators", one(|s| s.compound = true)),
        ("steps written as code", one(|s| s.as_code = true)),
        (
            "all of them",
            Style {
                rename_locals: true,
                through_let: true,
                dead_lets: true,
                split_steps: true,
                methods: true,
                compound: true,
                as_code: true,
            },
        ),
    ];
    for (what, style) in rewrites {
        let rewritten = write(style);
        let plan = compile(&with_table(rewritten.clone()), &SourceMeta::KAFKA).map_err(|e| {
            let e: Vec<String> = e.iter().map(|x| x.to_string()).collect();
            format!(
                "with {what} it is refused: {}\n  {}\nwas\n  {}",
                e.join("; "),
                show(&rewritten),
                show(&source)
            )
        })?;
        let got = records(&plan, DOCS);
        if timed_out(&got) {
            return Ok(None);
        }
        if got != together {
            let i = (0..got.len()).find(|&i| got[i] != together[i]).unwrap_or(0);
            return Err(format!(
                "with {what} record {i} is {} where it was {}\n  {}\nwas\n  {}",
                got[i],
                together[i],
                show(&rewritten),
                show(&source)
            ));
        }
    }
    let mut rng = Rng(seed.wrapping_mul(0xD1B54A32D192ED03) | 1);
    for (kind, what) in BREAKS.iter().enumerate() {
        let Some(broken) = break_at(&program, kind, &mut rng) else {
            continue;
        };
        let broken = Writer {
            p: &broken,
            style: Style::default(),
            temps: 0,
            trying: false,
        }
        .steps();
        let refused = catch_unwind(AssertUnwindSafe(|| {
            compile(&with_table(broken.clone()), &SourceMeta::KAFKA).is_err()
        }));
        match refused {
            Ok(true) => {}
            Ok(false) => {
                return Err(format!(
                    "with {what} it compiles\n  {}\nwas\n  {}",
                    show(&broken),
                    show(&source)
                ));
            }
            Err(_) => return Err(format!("with {what} it panics\n  {}", show(&broken))),
        }
    }
    Ok(Some(covers(&program)))
}

#[test]
fn generated_pipelines_compile_or_refuse_and_mean_the_same_however_written() {
    let seeds = fuzz::seeds(0..SEEDS);
    let mut compiled = 0;
    let mut coverage = Coverage::new(expected());
    fuzz::run(0..SEEDS, |seed| match catch_unwind(AssertUnwindSafe(|| check(seed))) {
        Ok(Ok(Some(c))) => {
            compiled += 1;
            c.into_iter().for_each(|name| coverage.reach(name));
            Ok(())
        }
        Ok(Ok(None)) => Ok(()),
        Ok(Err(why)) => Err(why),
        Err(_) => Err(format!(
            "it panics\n  {}",
            show(
                &Writer {
                    p: &Gen::new(seed).program(),
                    style: Style::default(),
                    temps: 0,
                    trying: false,
                }
                .steps()
            )
        )),
    });
    if fuzz::replaying() {
        return;
    }
    let seeds = seeds.end - seeds.start;
    // A generator whose programs mostly fail to compile tests nothing past the type checker.
    assert!(compiled * 2 > seeds, "only {compiled} of {seeds} programs compiled");
    let missed: Vec<String> = coverage.missed().into_iter().map(show_covered).collect();
    assert!(
        missed.is_empty(),
        "no program of {seeds} that compiled writes {}",
        missed.join(", ")
    );
}
