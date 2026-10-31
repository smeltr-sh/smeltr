//! The parser: Pratt (top-down operator precedence). Each infix operator has a `(left_bp, right_bp)` pair; `left <
//! right` is left-associative, `left > right` right-associative.

use crate::core::column::DataType;
use crate::expr::ast::{Catch, Expr, ExprKind, Function, PatternSrc, Region, Site, Span, Stmt, Value};
use crate::expr::lex::{Keyword, Lexer, Punct, Spanned, TemplatePart, Token};
use crate::ops::{BinOp, UnOp};
use crate::plan::tree;

#[derive(Clone, PartialEq, Debug)]
pub struct ParseError {
    /// Covers the token at fault, so an editor can underline it.
    pub span: Span,
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Infix binding powers, loosest to tightest.
///
/// The bitwise operators bind *tighter* than comparison, unlike C and JS, so `flags & 3 == 3` is `(flags & 3) == 3`
/// -- the reading a mask test wants. C's order, where `==` binds first and leaves a `Bool` beside the mask, is a
/// documented footgun this language does not carry.
fn infix_bp(op: BinOp) -> (u8, u8) {
    use BinOp::*;
    match op {
        // JS forbids mixing `??` with `&&`/`||` unparenthesized; here it binds below both.
        Coalesce => (1, 2),
        Or => (3, 4),
        And => (5, 6),
        Eq | Ne => (7, 8),
        Lt | Le | Gt | Ge => (9, 10),
        BitOr => (13, 14),
        BitXor => (15, 16),
        BitAnd => (17, 18),
        Shl | Shr => (19, 20),
        Add | Sub => (21, 22),
        Mul | Div | Mod => (23, 24),
        Pow => (28, 27),
    }
}

/// `e as type`, as TypeScript binds it: looser than every arithmetic and bitwise operator, tighter than comparison,
/// logic, `??` and the ternary. `a + b as str` is `(a + b) as str`; `x as i64 == 1` is `(x as i64) == 1`. The type
/// after it is not an expression, so what follows the type always applies to the coerced value: `n as f64 * 2` is
/// `(n as f64) * 2`.
const AS_BP: u8 = 11;
/// Between `*` and `**`: `-a * b` is `(-a) * b`, `-a ** b` is `-(a ** b)`.
const UNARY_BP: u8 = 25;
/// Right-associative, looser than every binary operator.
const TERNARY_BP: u8 = 2;

/// How deeply expressions and statements may nest. A stack overflow aborts rather than unwinding, and this text comes
/// from outside the process, so the bound is on the resulting tree and matches `format::json_scan::MAX_DEPTH`. The tree,
/// not parser recursion: a postfix chain (`[i]`, `.m()`, calls) folds in a loop and never deepens the parser's stack,
/// but each wrap deepens the tree that type checking, lowering, evaluation and `Drop` all walk, so `postfix` counts
/// its wraps against the same budget, and so do the folds of a left-associative chain (`a + b + c + ...`) and the
/// pieces of a template, which fold the same way.
const MAX_DEPTH: u32 = 128;

/// The head of the name of the temporary `move(from, to)` desugars through: `let #move1 = from; delete(from);
/// to = #move1;`. Code cannot spell it.
pub const MOVE_TEMP: &str = "#move";

/// The calls that are statements where they stand alone.
pub const STATEMENT_CALLS: [&str; 4] = ["delete", "drop", "unroll", "move"];

/// Where an assignment lands: a bare field, or one index into a bare field. `Stmt::Assign` and
/// `Stmt::IndexAssign` are the two statements it becomes, and every assigning form -- `=`, `op=`, `++`/`--` --
/// reaches them through here, so the accepted shapes cannot drift apart between them.
enum AssignTarget {
    Field(String),
    Index { target: String, index: Expr },
}

impl AssignTarget {
    /// The expression reading the target's current value: what `x op= v` and `x++` put on the left of the operator.
    ///
    /// An indexed target repeats its index expression here, so `xs[i()] += 1` evaluates `i()` twice. The expression
    /// language is pure, so this costs work rather than changing meaning.
    fn read(&self, span: Span) -> Expr {
        match self {
            AssignTarget::Field(name) => Expr {
                kind: ExprKind::Field(name.clone()),
                span,
            },
            AssignTarget::Index { target, index } => Expr {
                kind: ExprKind::Index {
                    base: Box::new(Expr {
                        kind: ExprKind::Field(target.clone()),
                        span,
                    }),
                    index: Box::new(index.clone()),
                },
                span,
            },
        }
    }

    fn assign(self, value: Expr) -> Stmt {
        match self {
            AssignTarget::Field(target) => Stmt::Assign {
                target,
                value,
                typed: false,
            },
            AssignTarget::Index { target, index } => Stmt::IndexAssign { target, index, value },
        }
    }
}

/// What the parser learned of the text's layout that its tokens alone do not say, for the formatter
/// (`expr::pretty`). Every place is a byte offset into the text parsed.
#[derive(Default, Debug)]
pub struct Shape {
    /// Each block's `{` and `}`, in the order they close.
    pub blocks: Vec<(u32, u32)>,
    /// Where each statement ends, and whether a `;` belongs there: not after a block's `}`, and not after an
    /// expression a `}` or `else` follows, which may be a function's value.
    pub statements: Vec<(u32, bool)>,
    /// Each type a declaration, a parameter or `as` spells.
    pub types: Vec<Span>,
    /// Each ternary's `:`.
    pub ternary_colons: Vec<u32>,
    /// Each `switch` label's `case` or `default`.
    pub case_labels: Vec<u32>,
    /// The label of each case with statements of its own that runs on into the next case: no `break`, `return`,
    /// `throw`, `continue` or `drop()` ends it.
    pub fall_throughs: Vec<Span>,
    /// What ends a block before statements that therefore never run: `drop()`, `return`, `throw`, `break` or
    /// `continue`.
    pub unreachable: Vec<&'static str>,
}

impl Shape {
    fn mark(&self) -> [usize; 7] {
        [
            self.blocks.len(),
            self.statements.len(),
            self.types.len(),
            self.ternary_colons.len(),
            self.case_labels.len(),
            self.fall_throughs.len(),
            self.unreachable.len(),
        ]
    }

    /// Forgets what a parse that was rolled back recorded.
    fn truncate(&mut self, [b, s, t, c, l, f, u]: [usize; 7]) {
        self.blocks.truncate(b);
        self.statements.truncate(s);
        self.types.truncate(t);
        self.ternary_colons.truncate(c);
        self.case_labels.truncate(l);
        self.fall_throughs.truncate(f);
        self.unreachable.truncate(u);
    }

    /// Notes a statement read after the one that ends its block, the statements before it `body[..from]`.
    fn after(&mut self, body: &[Stmt], from: usize) {
        let ender = match body[..from].last() {
            Some(Stmt::Drop(_)) => "drop()",
            Some(Stmt::Return(_)) => "return",
            Some(Stmt::Throw(_)) => "throw",
            Some(Stmt::Leave { to: Region::Loop, .. }) => "break",
            Some(Stmt::Leave { to: Region::Trip, .. }) => "continue",
            _ => return,
        };
        if from < body.len() {
            self.unreachable.push(ender);
        }
    }
}

/// Whether no row runs past the end of `body`: it ends in `return`, `throw`, `drop()` or `continue`, or in an
/// `if` whose branches both end so.
fn ends_the_row(body: &[Stmt]) -> bool {
    match body.last() {
        Some(Stmt::Return(_) | Stmt::Throw(_) | Stmt::Drop(_) | Stmt::Leave { to: Region::Trip, .. }) => true,
        Some(Stmt::If { then, else_, .. }) => ends_the_row(then) && ends_the_row(else_),
        _ => false,
    }
}

pub struct Parser<'s> {
    /// The text the tokens came from, so an error can quote what it refuses.
    src: &'s str,
    tokens: Vec<Spanned>,
    pos: usize,
    /// Current nesting depth, bounded by `MAX_DEPTH`.
    depth: u32,
    /// How many temporaries this parser has made: the `n` of the next one's name, `#move{n}` or `#switch{n}`.
    temps: usize,
    /// Expression statements that end a block or a branch, with a `}` or an `else` straight after them: the value of a
    /// function where `tail_returns` finds one, a statement anywhere else. Each holds the error it is where no function
    /// takes it, if it does nothing as a statement.
    open_tails: Vec<(Span, Option<String>)>,
    /// The names the `catch` blocks around the statement being read bind: what a `throw` may name.
    catches: Vec<String>,
    shape: Shape,
    /// Where `close_angle` last took a `>` off a glued token: the rest starts there.
    split_at: Option<u32>,
    /// How many types are being read, one inside another: only the outermost is recorded.
    in_type: u32,
}

/// The one message for a `break` a `switch` case cannot take: anywhere but the last statement of the case.
const BREAK_ENDS_A_CASE: &str = "`break` ends a case; nothing may follow it in the same case";

/// Move a span from a template hole's fragment back onto the original input.
fn shift_span(span: Span, offset: usize) -> Span {
    Span::new(span.start as usize + offset, span.end as usize + offset)
}

fn shift_expr_spans(e: &mut Expr, offset: usize) {
    e.span = shift_span(e.span, offset);
    for c in e.children_mut() {
        shift_expr_spans(c, offset);
    }
}

/// Parse one expression; the whole input must be consumed.
pub fn parse_expr(src: &str) -> Result<Expr, ParseError> {
    parse_expr_within(src, 0)
}

/// `parse_expr`, with the text's [`Shape`].
pub fn parse_expr_shaped(src: &str) -> Result<(Expr, Shape), ParseError> {
    let mut p = Parser::new(src)?;
    let e = p.expr_bp(0)?;
    p.expect_eof()?;
    Ok((e, p.shape))
}

/// What a template hole costs of the `MAX_DEPTH` budget. A hole is parsed by a lexer and parser of its own, whose
/// frames are many times an expression's: a hole per level overflows the stack at the depth a parenthesis per level
/// fits in. Sixteen holds a hole to the stack a parenthesis would take, and leaves templates eight deep.
const HOLE_DEPTH: u32 = 16;

/// `parse_expr` for a fragment already `depth` levels into another expression: a template hole. The hole's tree
/// hangs off the template's, so its nesting counts against the same `MAX_DEPTH`; a parser starting at 0 would let
/// templates nested in templates grow the tree, and the stack under it, without bound.
fn parse_expr_within(src: &str, depth: u32) -> Result<Expr, ParseError> {
    let mut p = Parser::new(src)?;
    p.depth = depth;
    let e = p.expr_bp(0)?;
    p.expect_eof()?;
    Ok(e)
}

/// Parse one type as a declaration spells it (`DataType::spell`); the whole input must be consumed.
pub fn parse_type(src: &str) -> Result<DataType, ParseError> {
    let mut p = Parser::new(src)?;
    let ty = p.type_expr()?;
    p.expect_eof()?;
    Ok(ty)
}

/// `move(from, to)`: `let #move{n} = from; delete(from); to = #move{n};`, `n` unique in its block.
pub fn move_stmts(from: &str, to: &str, n: usize, span: Span) -> [Stmt; 3] {
    let tmp = format!("{MOVE_TEMP}{n}");
    [
        Stmt::Let {
            name: tmp.clone(),
            value: Expr::new(span, ExprKind::Field(from.to_string())),
        },
        Stmt::Delete {
            names: vec![from.to_string()],
            clears: true,
            span,
        },
        Stmt::Assign {
            target: to.to_string(),
            value: Expr::new(span, ExprKind::Field(tmp)),
            typed: false,
        },
    ]
}

/// `value` declared as `ty` for `name` where there is an exception: checked, and converted only from a variant.
pub fn declared(name: &str, ty: Option<DataType>, value: Expr) -> Expr {
    match ty {
        Some(ty) => Expr {
            span: value.span,
            kind: ExprKind::Check {
                ty,
                value: Box::new(value),
                site: Site::Declaration(name.to_string()),
            },
        },
        None => value,
    }
}

/// Parse a `Code` body: a statement list with no enclosing braces.
pub fn parse_block(src: &str) -> Result<Vec<Stmt>, ParseError> {
    parse_block_shaped(src).map(|(stmts, _)| stmts)
}

/// `parse_block`, with the text's [`Shape`].
pub fn parse_block_shaped(src: &str) -> Result<(Vec<Stmt>, Shape), ParseError> {
    let mut p = Parser::new(src)?;
    let mut out = Vec::new();
    while !p.at_eof() {
        p.stmt(&mut out)?;
        while p.eat_punct(Punct::Semi) {}
    }
    if let Some((span, Some(message))) = p.open_tails.iter().find(|(_, refusal)| refusal.is_some()) {
        return Err(ParseError {
            span: *span,
            message: message.clone(),
        });
    }
    Ok((out, p.shape))
}

impl<'s> Parser<'s> {
    fn new(src: &'s str) -> Result<Parser<'s>, ParseError> {
        let tokens = Lexer::tokenize(src).map_err(|e| ParseError {
            span: e.span,
            message: e.message,
        })?;
        Ok(Parser {
            src,
            tokens,
            pos: 0,
            depth: 0,
            temps: 0,
            open_tails: Vec::new(),
            catches: Vec::new(),
            shape: Shape::default(),
            split_at: None,
            in_type: 0,
        })
    }

    // -- token helpers -------------------------------------------------------

    fn peek(&self) -> &Token {
        &self.tokens[self.pos].token
    }

    /// The token after `peek`'s; the end of input at the end.
    fn peek_next(&self) -> &Token {
        &self.tokens[(self.pos + 1).min(self.tokens.len() - 1)].token
    }

    /// Whether the next token is an identifier spelled `word`: a contextual keyword, which stays a name elsewhere.
    fn at_word(&self, word: &str) -> bool {
        matches!(self.peek(), Token::Ident(w) if w == word)
    }

    fn peek_span(&self) -> Span {
        self.tokens[self.pos].span
    }

    /// The span of the token read last.
    fn last_span(&self) -> Span {
        self.tokens[self.pos - 1].span
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    fn advance(&mut self) -> Spanned {
        let t = self.tokens[self.pos].clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn eat_punct(&mut self, p: Punct) -> bool {
        if self.peek() == &Token::Punct(p) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn eat_assign_op(&mut self) -> Option<BinOp> {
        if let Token::Punct(Punct::AssignOp(op)) = self.peek() {
            let op = *op;
            self.advance();
            Some(op)
        } else {
            None
        }
    }

    fn eat_incdec(&mut self) -> Option<BinOp> {
        let op = match self.peek() {
            Token::Punct(Punct::PlusPlus) => BinOp::Add,
            Token::Punct(Punct::MinusMinus) => BinOp::Sub,
            _ => return None,
        };
        self.advance();
        Some(op)
    }

    fn expect_punct(&mut self, p: Punct, what: &str) -> Result<Span, ParseError> {
        if self.peek() == &Token::Punct(p) {
            Ok(self.advance().span)
        } else {
            Err(self.error(format!("expected {what}, found {}", self.describe())))
        }
    }

    /// The items of a list up to `close`, which it takes: `item` reads each, a comma comes between two, and one may
    /// follow the last. `what` names the closer for the error when neither follows an item. Returns the closer's span.
    fn list<T>(
        &mut self,
        close: Punct,
        what: &str,
        mut item: impl FnMut(&mut Self) -> Result<T, ParseError>,
    ) -> Result<(Vec<T>, Span), ParseError> {
        let mut items = Vec::new();
        loop {
            if self.peek() == &Token::Punct(close) {
                return Ok((items, self.advance().span));
            }
            items.push(item(self)?);
            if !self.eat_punct(Punct::Comma) {
                return Ok((items, self.expect_punct(close, what)?));
            }
        }
    }

    fn expect_eof(&self) -> Result<(), ParseError> {
        if self.at_eof() {
            Ok(())
        } else {
            Err(self.error(format!("unexpected trailing {}", self.describe())))
        }
    }

    fn describe(&self) -> String {
        match self.peek() {
            Token::Eof => "end of input".into(),
            Token::Ident(s) => format!("identifier `{s}`"),
            Token::Int(v) => format!("`{v}`"),
            Token::Float(v) => format!("`{v}`"),
            Token::Str(_) => "string literal".into(),
            Token::Regex { .. } => "regex literal".into(),
            Token::Keyword(k) => format!("keyword `{k:?}`"),
            Token::Punct(p) => format!("`{p:?}`"),
            Token::Template(_) => "template literal".into(),
        }
    }

    /// `` `a ${x} b` `` is `"a " + toString(x) + " b"`. A hole's spans are shifted back onto the original input.
    fn template(&mut self, parts: Vec<TemplatePart>, sp: Span) -> Result<Expr, ParseError> {
        if parts.is_empty() {
            return Ok(Expr::new(sp, ExprKind::Literal(Value::Str(String::new()))));
        }
        // The pieces fold into a left-nested `+` chain, one level per piece.
        if self.depth + parts.len() as u32 > MAX_DEPTH {
            return Err(self.too_deep());
        }

        let mut acc: Option<Expr> = None;
        for part in parts {
            let piece = match part {
                TemplatePart::Lit(text) => Expr::new(sp, ExprKind::Literal(Value::Str(text))),
                TemplatePart::Hole { src, offset } => {
                    if self.depth + HOLE_DEPTH > MAX_DEPTH {
                        return Err(self.too_deep());
                    }
                    let mut e = parse_expr_within(&src, self.depth + HOLE_DEPTH).map_err(|mut e| {
                        e.span = shift_span(e.span, offset);
                        e
                    })?;
                    shift_expr_spans(&mut e, offset);
                    Expr::new(
                        e.span,
                        ExprKind::Call {
                            name: "toString".into(),
                            args: vec![e],
                        },
                    )
                }
            };
            acc = Some(match acc {
                None => piece,
                Some(lhs) => Expr::new(
                    sp,
                    ExprKind::Binary {
                        op: BinOp::Add,
                        lhs: Box::new(lhs),
                        rhs: Box::new(piece),
                    },
                ),
            });
        }
        Ok(acc.expect("parts is non-empty"))
    }

    /// The just-parsed left-hand expression as a place an assignment may land. On refusal `pos` rolls back to
    /// `start`, so the error points at the statement rather than at whatever the expression had consumed.
    ///
    /// One rule for `=`, `op=` and `++`/`--`: all three compile to the same two statements, so accepting an indexed
    /// target for one and not the others was a difference in the parser alone.
    fn assign_target(&mut self, e: Expr, start: usize, what: &str) -> Result<AssignTarget, ParseError> {
        let place = match e.kind {
            ExprKind::Field(target) => Some(AssignTarget::Field(target)),
            ExprKind::Index { base, index } => match base.kind {
                ExprKind::Field(target) => Some(AssignTarget::Index { target, index: *index }),
                _ => None,
            },
            _ => None,
        };
        match place {
            Some(place) => Ok(place),
            None => {
                self.pos = start;
                Err(self.error(format!("left side of {what} must be a field name or `field[expr]`")))
            }
        }
    }

    fn error(&self, message: impl Into<String>) -> ParseError {
        ParseError {
            span: self.peek_span(),
            message: message.into(),
        }
    }

    // -- statements ----------------------------------------------------------

    /// One statement, pushed onto `out`. Most statements are one `Stmt`; `move()` and `switch` desugar to several,
    /// or to none.
    fn stmt(&mut self, out: &mut Vec<Stmt>) -> Result<(), ParseError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            self.depth -= 1;
            return Err(self.too_deep());
        }
        let tails = self.open_tails.len();
        let from = out.len();
        let r = self.stmt_inner(out);
        self.depth -= 1;
        if r.is_ok() {
            self.shape.after(out, from);
            let last = self.last_span();
            let after_block = self.shape.blocks.last().is_some_and(|b| b.1 == last.start);
            let open_tail = self.open_tails.len() > tails
                && matches!(out.last(), Some(Stmt::Expr(e)) if self.open_tails.last().is_some_and(|t| t.0 == e.span));
            self.shape.statements.push((last.end, !after_block && !open_tail));
        }
        r
    }

    fn too_deep(&self) -> ParseError {
        ParseError {
            span: self.peek_span(),
            message: format!("expression nests more than {MAX_DEPTH} levels deep"),
        }
    }

    /// `try` and `switch` are contextual keywords: a name spelled so starts the statement only where the token after
    /// it can follow no name, so a field named `try` or `switch` still parses.
    fn stmt_inner(&mut self, out: &mut Vec<Stmt>) -> Result<(), ParseError> {
        let stmt = match self.peek() {
            Token::Keyword(Keyword::Let) => self.let_stmt()?,
            Token::Keyword(Keyword::If) => self.if_stmt()?,
            Token::Keyword(Keyword::For) => self.for_stmt()?,
            Token::Keyword(Keyword::While) => self.while_stmt()?,
            Token::Keyword(Keyword::Break) => Stmt::Leave {
                to: Region::Loop,
                span: self.advance().span,
            },
            Token::Keyword(Keyword::Continue) => Stmt::Leave {
                to: Region::Trip,
                span: self.advance().span,
            },
            Token::Keyword(Keyword::Function) => self.function_decl()?,
            Token::Keyword(Keyword::Return) => {
                self.advance();
                Stmt::Return(self.expr_bp(0)?)
            }
            Token::Ident(w) if w == "try" && self.peek_next() == &Token::Punct(Punct::LBrace) => self.try_stmt()?,
            Token::Ident(w) if w == "switch" && self.peek_next() == &Token::Punct(Punct::LParen) => {
                return self.switch_stmt(out);
            }
            Token::Ident(w) if w == "throw" && matches!(self.peek_next(), Token::Ident(_)) => self.throw_stmt()?,
            // The block's own brace was taken for an object's: `if (f) {...e}`.
            Token::Punct(Punct::Ellipsis) => {
                return Err(self.error("`...e` is not a statement; write `Object.assign($, e)` to write the record"));
            }
            _ => return self.expr_stmt(out),
        };
        out.push(stmt);
        Ok(())
    }

    fn let_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.advance(); // `let`
        let name = match self.advance() {
            Spanned {
                token: Token::Ident(n), ..
            } => n,
            s => {
                return Err(ParseError {
                    span: s.span,
                    message: "expected a name after `let`".into(),
                });
            }
        };
        if name.contains('.') {
            return Err(self.error(format!("a local's name cannot be dotted: `{name}`")));
        }
        let ty = if self.eat_punct(Punct::Colon) {
            Some(self.type_expr()?)
        } else {
            None
        };
        self.expect_punct(Punct::Assign, "`=` after `let` binding")?;
        let value = self.expr_bp(0)?;
        let value = declared(&name, ty, value);
        Ok(Stmt::Let { name, value })
    }

    /// A statement that starts with an expression. Where no assigning form follows it, a call to `delete`, `drop`,
    /// `unroll` or `move` is that statement.
    fn expr_stmt(&mut self, out: &mut Vec<Stmt>) -> Result<(), ParseError> {
        let start = self.pos;
        let e = self.expr_bp(0)?;
        if let ExprKind::Call { name, .. } = &e.kind
            && STATEMENT_CALLS.contains(&name.as_str())
            && !self.at_assigning()
        {
            return self.call_stmt(e, out);
        }
        let stmt = self.assignment(e, start)?;
        out.push(stmt);
        Ok(())
    }

    /// Whether the next token makes the expression before it a place written to: `:`, `=`, `op=`, `++` or `--`.
    fn at_assigning(&self) -> bool {
        matches!(
            self.peek(),
            Token::Punct(Punct::Colon | Punct::Assign | Punct::AssignOp(_) | Punct::PlusPlus | Punct::MinusMinus)
        )
    }

    /// The statement `e` begins, `start` the token it began at: a declaration, an assignment, or `e` itself.
    fn assignment(&mut self, e: Expr, start: usize) -> Result<Stmt, ParseError> {
        // `name: type;` declares a field; `name: type = expr;` declares it and assigns, coercing the value.
        if let ExprKind::Field(_) = &e.kind
            && self.eat_punct(Punct::Colon)
        {
            let ExprKind::Field(name) = e.kind else { unreachable!() };
            let ty = self.type_expr()?;
            if self.eat_punct(Punct::Assign) {
                let value = declared(&name, Some(ty), self.expr_bp(0)?);
                return Ok(Stmt::Assign {
                    target: name,
                    value,
                    typed: true,
                });
            }
            return Ok(Stmt::Declare { name, ty });
        }

        // `x++;` is `x = x + 1`. Statement position only: assignment is not an expression here.
        if let Some(op) = self.eat_incdec() {
            let span = e.span;
            let target = self.assign_target(e, start, "`++`/`--`")?;
            let value = Expr {
                kind: ExprKind::Binary {
                    op,
                    lhs: Box::new(target.read(span)),
                    rhs: Box::new(Expr {
                        kind: ExprKind::Literal(Value::I64(1)),
                        span,
                    }),
                },
                span,
            };
            return Ok(target.assign(value));
        }

        // `x op= v` is `x = x op v`.
        if let Some(op) = self.eat_assign_op() {
            let span = e.span;
            let target = self.assign_target(e, start, "a compound assignment")?;
            let rhs = self.expr_bp(0)?;
            let value = Expr {
                kind: ExprKind::Binary {
                    op,
                    lhs: Box::new(target.read(span)),
                    rhs: Box::new(rhs),
                },
                span,
            };
            return Ok(target.assign(value));
        }

        if self.eat_punct(Punct::Assign) {
            let target = self.assign_target(e, start, "`=`")?;
            let value = self.expr_bp(0)?;
            return Ok(target.assign(value));
        }
        // `Object.assign(p, a, b)` is a write to `p`; the compiler gives it `p`'s type, which only it knows.
        if let ExprKind::Call { name, args } = &e.kind
            && name == "Object.assign"
        {
            let (
                Some(Expr {
                    kind: ExprKind::Field(target),
                    ..
                }),
                true,
            ) = (args.first(), args.len() > 1)
            else {
                return Err(ParseError {
                    span: e.span,
                    message: "`Object.assign` takes a name to assign into, then one or more sources".into(),
                });
            };
            return Ok(Stmt::Assign {
                target: target.clone(),
                value: e.clone(),
                typed: false,
            });
        }
        // Two expressions do nothing as statements, so only a function may take them, as its value. A bare name is a
        // read with no reader: `delete x;` lexes as the name `delete` and then the name `x`. `{...e}` alone reads as a
        // write to the record. Every form that gives a name something to do has been taken by now.
        let refusal = match &e.kind {
            ExprKind::Field(name) => Some(format!("`{name}` on its own is not a statement")),
            ExprKind::Object(entries) if matches!(entries.as_slice(), [(None, _)]) => {
                Some("`{...e}` is not a statement; write `Object.assign($, e)` to write the record".to_string())
            }
            _ => None,
        };
        if matches!(self.peek(), Token::Punct(Punct::RBrace) | Token::Keyword(Keyword::Else)) {
            self.open_tails.push((e.span, refusal));
        } else if let Some(message) = refusal {
            return Err(ParseError { span: e.span, message });
        }
        Ok(Stmt::Expr(e))
    }

    /// `delete(a, b.c)`, `drop()`, `unroll(field, keep)` and `move(from, to)`: statements spelled as calls, whose
    /// arguments are names, not values. `move` is `let t = from; delete(from); to = t;`, `t` a local no code can
    /// spell.
    fn call_stmt(&mut self, e: Expr, out: &mut Vec<Stmt>) -> Result<(), ParseError> {
        let span = e.span;
        let ExprKind::Call { name, args } = e.kind else {
            unreachable!("called on a call")
        };
        match name.as_str() {
            "delete" => {
                if args.is_empty() {
                    return Err(ParseError {
                        span,
                        message: "`delete` takes one or more names: `delete(a, b.c)`".into(),
                    });
                }
                let names = args
                    .iter()
                    .map(|a| self.name_arg(a, "delete"))
                    .collect::<Result<_, _>>()?;
                out.push(Stmt::Delete {
                    names,
                    clears: false,
                    span,
                });
            }
            "drop" => {
                if let Some(a) = args.first() {
                    return Err(ParseError {
                        span: a.span,
                        message: "`drop()` takes no arguments; write `if (cond) drop()`".into(),
                    });
                }
                out.push(Stmt::Drop(span));
            }
            "unroll" => {
                let (field, keep) = match args.as_slice() {
                    [field] => (field, false),
                    [
                        field,
                        Expr {
                            kind: ExprKind::Literal(Value::Bool(keep)),
                            ..
                        },
                    ] => (field, *keep),
                    [_, keep] => {
                        return Err(ParseError {
                            span: keep.span,
                            message: format!(
                                "`unroll`'s second argument is `true` or `false`, found `{}`",
                                keep.span.slice(self.src)
                            ),
                        });
                    }
                    _ => {
                        return Err(ParseError {
                            span,
                            message: "`unroll` takes a name, then optionally `true` or `false`: `unroll(field)`, \
                                      `unroll(field, true)`"
                                .into(),
                        });
                    }
                };
                let field = self.name_arg(field, "unroll")?;
                out.push(Stmt::Unroll { field, keep, span });
            }
            "move" => {
                let [from, to] = args.as_slice() else {
                    return Err(ParseError {
                        span,
                        message: "`move` takes two names: `move(from, to)`".into(),
                    });
                };
                let (from_name, to_name) = (self.name_arg(from, "move")?, self.name_arg(to, "move")?);
                self.temps += 1;
                out.extend(move_stmts(&from_name, &to_name, self.temps, span));
            }
            _ => unreachable!("`expr_stmt` passes only these four"),
        }
        Ok(())
    }

    /// The name an argument of the statement `what` spells; anything else is an error quoting it.
    fn name_arg(&self, arg: &Expr, what: &str) -> Result<String, ParseError> {
        match &arg.kind {
            ExprKind::Field(name) => Ok(name.clone()),
            _ => Err(ParseError {
                span: arg.span,
                message: format!(
                    "`{what}` takes names; `{}` is not one (write `a.b` or `$[\"a b\"]`)",
                    arg.span.slice(self.src)
                ),
            }),
        }
    }

    /// `try { ... } catch (e) { ... } finally { ... }`. `catch`es, a `finally` or both follow the body; `(e)` is
    /// optional and `e` is a plain name. Only the last `catch` may name no kind, and no kind is named twice.
    fn try_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.advance(); // `try`
        let body = self.stmt_block()?;
        let mut catches: Vec<Catch> = Vec::new();
        while self.at_word("catch") {
            if catches.last().is_some_and(|c| c.1.is_empty()) {
                return Err(self.error("a `catch` that names no kind takes every kind, so no `catch` follows it"));
            }
            let catch = self.catch_clause()?;
            let names = |k: &usize| catches.iter().any(|c| c.1.contains(k));
            if let Some(&k) = catch.1.iter().find(|k| names(k)) {
                let k = crate::core::error::ExceptionKind::KINDS[k].0;
                return Err(self.error(format!("`{k}` is named by two `catch`es; one `catch` takes a kind")));
            }
            catches.push(catch);
        }
        let finally = match self.at_word("finally") {
            true => {
                self.advance();
                Some(self.stmt_block()?)
            }
            false => None,
        };
        if catches.is_empty() && finally.is_none() {
            return Err(self.error(format!(
                "expected `catch` or `finally` after the `try` block, found {}",
                self.describe()
            )));
        }
        Ok(Stmt::Try {
            body,
            catches,
            finally: finally.unwrap_or_default(),
        })
    }

    /// `catch (e) { ... }`, `catch { ... }`, or `catch (e: K | L) { ... }`, which takes only the kinds it names: their
    /// positions in `ExceptionKind::KINDS`.
    fn catch_clause(&mut self) -> Result<Catch, ParseError> {
        self.advance(); // `catch`
        if !self.eat_punct(Punct::LParen) {
            return Ok((None, Vec::new(), self.stmt_block()?));
        }
        let var = match self.advance() {
            Spanned {
                token: Token::Ident(n), ..
            } if !n.contains('.') && n != "$" => n,
            Spanned {
                token: Token::Ident(n),
                span,
            } => {
                return Err(ParseError {
                    span,
                    message: format!("`catch` binds a plain name, found `{n}`"),
                });
            }
            s => {
                return Err(ParseError {
                    span: s.span,
                    message: "expected a name in `catch (name)`".into(),
                });
            }
        };
        let mut kinds = Vec::new();
        if self.eat_punct(Punct::Colon) {
            loop {
                kinds.push(self.catch_kind()?);
                if !self.eat_punct(Punct::Pipe) {
                    break;
                }
            }
        }
        self.expect_punct(Punct::RParen, "`)` after the `catch` name")?;
        self.catches.push(var.clone());
        let body = self.stmt_block();
        self.catches.pop();
        Ok((Some(var), kinds, body?))
    }

    /// The position in `ExceptionKind::KINDS` of one kind a typed `catch` names.
    fn catch_kind(&mut self) -> Result<usize, ParseError> {
        use crate::core::error::ExceptionKind;
        let s = self.advance();
        let message = match &s.token {
            Token::Ident(k) => match ExceptionKind::KINDS.iter().position(|(n, _)| n == k) {
                Some(i) if !ExceptionKind::KINDS[i].1.caught() => format!(
                    "`{k}` is an exception a `try` does not catch: encoding exceptions keep their value, and delivery \
                     exceptions happen outside every pipeline"
                ),
                Some(i) => return Ok(i),
                None => format!("`{k}` is no exception kind"),
            },
            _ => "expected an exception kind".into(),
        };
        let kinds: Vec<String> = (ExceptionKind::KINDS.iter())
            .filter(|(_, c)| c.caught())
            .map(|(n, _)| format!("`{n}`"))
            .collect();
        Err(ParseError {
            span: s.span,
            message: format!("{message}; a `catch` takes {}", kinds.join(", ")),
        })
    }

    /// `throw e`: `e` must be a name a `catch` around it binds.
    fn throw_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.advance(); // `throw`
        let s = self.advance();
        match s.token {
            Token::Ident(n) if self.catches.contains(&n) => Ok(Stmt::Throw(Expr::new(s.span, ExprKind::Field(n)))),
            Token::Ident(n) => Err(ParseError {
                span: s.span,
                message: format!(
                    "`throw {n}`: `throw` takes only the name a `catch` around it binds, and raises that exception \
                     again; `{n}` is not one"
                ),
            }),
            _ => unreachable!("`stmt_inner` reads `throw` before a name"),
        }
    }

    /// `switch (e) { case v: ... default: ... }`, read as TypeScript reads it and desugared to an `if`/`else if`/`else`
    /// chain over `e == v`, `e` repeated in each test. A case whose statements do not end in `break` falls through: its
    /// branch is its own statements and then the next case's, up to the first case that ends in `break`. Labels with
    /// no statements between them share the next case's branch. `default`, at most one and last, is the final `else`.
    /// The `break`s are spent here: one left in the output would end an enclosing loop.
    fn switch_stmt(&mut self, out: &mut Vec<Stmt>) -> Result<(), ParseError> {
        struct Case {
            /// The labels whose rows take this case's branch; `None` for `default`.
            labels: Option<Vec<Expr>>,
            body: Vec<Stmt>,
            breaks: bool,
        }

        self.advance(); // `switch`
        self.expect_punct(Punct::LParen, "`(` after `switch`")?;
        let mut subject = self.expr_bp(0)?;
        self.expect_punct(Punct::RParen, "`)` after the `switch` subject")?;
        // Each case compares against the subject, so one that computes is computed once, into a temporary.
        if !matches!(subject.kind, ExprKind::Field(_) | ExprKind::Literal(_)) {
            self.temps += 1;
            let tmp = format!("#switch{}", self.temps);
            let span = subject.span;
            out.push(Stmt::Let {
                name: tmp.clone(),
                value: std::mem::replace(&mut subject, Expr::new(span, ExprKind::Field(tmp))),
            });
        }
        let open = self.expect_punct(Punct::LBrace, "`{` to open the `switch` body")?;

        let mut cases: Vec<Case> = Vec::new();
        // Labels with no statements of their own, waiting for the case they share.
        let mut pending: Vec<Expr> = Vec::new();
        let mut seen_default = false;
        // The label of the case before, where it runs on into the one being read.
        let mut running_on: Option<Span> = None;
        while !self.eat_punct(Punct::RBrace) {
            if self.at_eof() {
                return Err(self.error("unterminated `switch`, expected `}`"));
            }
            let label_span = self.peek_span();
            self.shape.case_labels.push(label_span.start);
            self.shape.fall_throughs.extend(running_on.take());
            if seen_default {
                return Err(ParseError {
                    span: label_span,
                    message: "`default` must be the last case of a `switch`".into(),
                });
            }
            let label = if self.at_case_label() {
                self.advance(); // `case`
                Some(self.expr_bp(0)?)
            } else if self.at_default_label() {
                self.advance(); // `default`
                seen_default = true;
                None
            } else {
                return Err(self.error(format!(
                    "expected `case` or `default` in a `switch`, found {}",
                    self.describe()
                )));
            };
            let colon = self.expect_punct(Punct::Colon, "`:` after the case label")?;
            // A label ends like a statement does, without a `;`.
            self.shape.statements.push((colon.end, false));
            let (body, breaks) = self.case_body()?;
            if !body.is_empty() && !breaks && !ends_the_row(&body) {
                running_on = Some(Span::new(label_span.start as usize, colon.start as usize));
            }
            match label {
                // `default` takes every row no case before it took, a waiting label's among them.
                None => {
                    pending.clear();
                    cases.push(Case {
                        labels: None,
                        body,
                        breaks,
                    });
                }
                Some(label) if body.is_empty() && !breaks => pending.push(label),
                Some(label) => {
                    pending.push(label);
                    cases.push(Case {
                        labels: Some(std::mem::take(&mut pending)),
                        body,
                        breaks,
                    });
                }
            }
        }
        self.shape.blocks.push((open.start, self.last_span().start));
        // Labels still waiting at the end fall through to nothing, as a row no case takes does.

        let branches: Vec<Vec<Stmt>> = (0..cases.len())
            .map(|i| {
                let mut branch = Vec::new();
                for case in &cases[i..] {
                    branch.extend(case.body.iter().cloned());
                    if case.breaks {
                        break;
                    }
                }
                branch
            })
            .collect();
        let mut chain: Vec<Stmt> = Vec::new();
        for (case, branch) in cases.into_iter().zip(branches).rev() {
            let Some(labels) = case.labels else {
                chain = branch;
                continue;
            };
            let cond = labels
                .into_iter()
                .map(|label| {
                    Expr::new(
                        label.span,
                        ExprKind::Binary {
                            op: BinOp::Eq,
                            lhs: Box::new(subject.clone()),
                            rhs: Box::new(label),
                        },
                    )
                })
                .reduce(|lhs, rhs| {
                    Expr::new(
                        lhs.span.join(rhs.span),
                        ExprKind::Binary {
                            op: BinOp::Or,
                            lhs: Box::new(lhs),
                            rhs: Box::new(rhs),
                        },
                    )
                })
                .expect("a case has its own label");
            chain = vec![Stmt::If {
                cond,
                then: branch,
                else_: chain,
            }];
        }
        out.extend(chain);
        Ok(())
    }

    /// `case` starting a label: the name, unless what follows it writes to a field named `case`.
    fn at_case_label(&self) -> bool {
        self.at_word("case")
            && !matches!(
                self.peek_next(),
                Token::Punct(Punct::Colon | Punct::Assign | Punct::AssignOp(_) | Punct::PlusPlus | Punct::MinusMinus)
            )
    }

    /// `default:`. Inside a `switch` this is the label, never the declaration of a field named `default`.
    fn at_default_label(&self) -> bool {
        self.at_word("default") && self.peek_next() == &Token::Punct(Punct::Colon)
    }

    /// A case's statements, up to the next label or the `switch`'s `}`, and whether they end in `break`, which is
    /// consumed. A `break` anywhere else in them that no loop inside the case takes is an error.
    fn case_body(&mut self) -> Result<(Vec<Stmt>, bool), ParseError> {
        let at_end = |p: &Self| {
            p.at_eof() || p.peek() == &Token::Punct(Punct::RBrace) || p.at_case_label() || p.at_default_label()
        };
        let mut body = Vec::new();
        while !at_end(self) {
            if self.peek() == &Token::Keyword(Keyword::Break) {
                let end = self.advance().span.end;
                self.shape.statements.push((end, true));
                while self.eat_punct(Punct::Semi) {}
                if !at_end(self) {
                    return Err(self.error(BREAK_ENDS_A_CASE));
                }
                return Ok((body, true));
            }
            let from = body.len();
            self.stmt(&mut body)?;
            let not_a_loop = |s: &Stmt| !matches!(s, Stmt::For { .. } | Stmt::While { .. } | Stmt::Function { .. });
            if let Some(span) = Stmt::find_map(&body[from..], &not_a_loop, &mut |s| match s {
                Stmt::Leave { to: Region::Loop, span } => Some(*span),
                _ => None,
            }) {
                return Err(ParseError {
                    span,
                    message: BREAK_ENDS_A_CASE.into(),
                });
            }
            while self.eat_punct(Punct::Semi) {}
        }
        Ok((body, false))
    }

    /// `function name(a, b): type { ... }`. A definition, never a value. The return type is required: a call is
    /// that type, and every `return` coerces to it.
    fn function_decl(&mut self) -> Result<Stmt, ParseError> {
        self.advance(); // `function`
        let name = match self.advance() {
            Spanned {
                token: Token::Ident(n), ..
            } => n,
            s => {
                return Err(ParseError {
                    span: s.span,
                    message: "expected a name after `function`".into(),
                });
            }
        };
        if name.contains('.') {
            return Err(self.error("a function name cannot be dotted"));
        }
        self.expect_punct(Punct::LParen, "`(` after the function name")?;
        let (params, param_types) = self.params()?;
        let ret = self.return_type()?;
        let body = self.function_body()?;
        Ok(Stmt::Function {
            name,
            function: Function {
                params,
                param_types,
                ret,
                body,
            },
        })
    }

    /// A parameter list, after its `(`: plain names, each with an optional `: type`.
    fn params(&mut self) -> Result<(Vec<String>, Vec<Option<DataType>>), ParseError> {
        let mut params: Vec<String> = Vec::new();
        let mut types = Vec::new();
        self.list(Punct::RParen, "`)` to close the parameter list", |p| {
            let (param, span) = match p.advance() {
                Spanned {
                    token: Token::Ident(param),
                    span,
                } => (param, span),
                s => {
                    return Err(ParseError {
                        span: s.span,
                        message: "expected a parameter name".into(),
                    });
                }
            };
            if param.contains('.') || param == tree::ROOT {
                return Err(ParseError {
                    span,
                    message: format!("a parameter is a plain name, found `{param}`"),
                });
            }
            if params.contains(&param) {
                return Err(ParseError {
                    span,
                    message: format!("duplicate parameter `{param}`"),
                });
            }
            types.push(if p.eat_punct(Punct::Colon) {
                Some(p.type_expr()?)
            } else {
                None
            });
            params.push(param);
            Ok(())
        })?;
        Ok((params, types))
    }

    /// The `: type` after a parameter list, where one is written.
    fn return_type(&mut self) -> Result<Option<DataType>, ParseError> {
        if !self.eat_punct(Punct::Colon) {
            return Ok(None);
        }
        Ok(Some(self.type_expr()?))
    }

    /// A function's `{ ... }`, its tail made its `return`.
    fn function_body(&mut self) -> Result<Vec<Stmt>, ParseError> {
        let mut body = self.stmt_block()?;
        self.tail_returns(&mut body);
        Ok(body)
    }

    /// A body's value written Rust's way: an expression ending the body with no `;` after it is `return` of it, and
    /// so is one ending each branch of a final `if`/`else`.
    fn tail_returns(&mut self, body: &mut [Stmt]) {
        let Some(last) = body.last_mut() else { return };
        match last {
            Stmt::Expr(e) => {
                // A `switch` copies a case's body into each case that falls into it, so one tail can be met twice.
                if let Some(tail) = self.open_tails.iter_mut().find(|(span, _)| *span == e.span) {
                    tail.1 = None;
                    *last = Stmt::Return(e.clone());
                }
            }
            Stmt::If { then, else_, .. } if !else_.is_empty() => {
                self.tail_returns(then);
                self.tail_returns(else_);
            }
            _ => {}
        }
    }

    /// A type exception: one of the engine's types by name, `array<T>`, or `{name: T}`, open where it ends in `...`.
    fn type_expr(&mut self) -> Result<DataType, ParseError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            self.depth -= 1;
            return Err(self.error(format!("a type nests more than {MAX_DEPTH} levels deep")));
        }
        let start = self.peek_span().start;
        self.in_type += 1;
        let r = self.type_expr_inner();
        self.in_type -= 1;
        self.depth -= 1;
        if r.is_ok() && self.in_type == 0 {
            // A `>` `close_angle` took off a glued token ends the type inside that token.
            let last = self.last_span().end;
            let end = match self.split_at {
                Some(at) if at >= last => at,
                _ => last,
            };
            self.shape.types.push(Span { start, end });
        }
        r
    }

    /// The `>` closing a type argument. The lexer glues a `>` to what follows it (`>>`, `>=`, `>>=`), so this takes
    /// one `>` off such a token and leaves the rest for the parse after it: `array<array<i64>>` closes twice.
    fn close_angle(&mut self) -> Result<(), ParseError> {
        let rest = match self.peek() {
            Token::Punct(Punct::Gt) => {
                self.advance();
                return Ok(());
            }
            Token::Punct(Punct::GtGt) => Punct::Gt,
            Token::Punct(Punct::Ge) => Punct::Assign,
            Token::Punct(Punct::AssignOp(crate::ops::BinOp::Shr)) => Punct::Ge,
            _ => {
                return Err(self.error(format!(
                    "expected `>` after the element type, found {}",
                    self.describe()
                )));
            }
        };
        let t = &mut self.tokens[self.pos];
        t.token = Token::Punct(rest);
        t.span = Span::new(t.span.start as usize + 1, t.span.end as usize);
        self.split_at = Some(t.span.start);
        Ok(())
    }

    fn type_expr_inner(&mut self) -> Result<DataType, ParseError> {
        if self.eat_punct(Punct::LBrace) {
            let mut fields: Vec<(String, DataType)> = Vec::new();
            let mut open = false;
            self.list(Punct::RBrace, "`}` to close the object type", |p| {
                if open {
                    return Err(p.error("`...` is an object type's last entry"));
                }
                if p.eat_punct(Punct::Ellipsis) {
                    open = true;
                    return Ok(());
                }
                // A field's name is one key: a plain name, or a string for any other.
                let name = match p.advance() {
                    Spanned {
                        token: Token::Ident(n), ..
                    } if tree::is_plain(&n) => n,
                    Spanned {
                        token: Token::Str(n), ..
                    } => n,
                    s => {
                        return Err(ParseError {
                            span: s.span,
                            message: "expected a field name in an object type: a name, or a string for a key that \
                                      holds a dot"
                                .into(),
                        });
                    }
                };
                p.expect_punct(Punct::Colon, "`:` after the field name")?;
                let ty = p.type_expr()?;
                if fields.iter().any(|(n, _)| *n == name) {
                    return Err(p.error(format!("object type names `{name}` twice")));
                }
                fields.push((name, ty));
                Ok(())
            })?;
            return Ok(DataType::Struct(fields, open));
        }
        let (name, span) = match self.peek() {
            Token::Ident(_) => match self.advance() {
                Spanned {
                    token: Token::Ident(n),
                    span,
                } => (n, span),
                _ => unreachable!("peeked an identifier"),
            },
            _ => return Err(self.error(format!("expected a type, found {}", self.describe()))),
        };
        Ok(match name.as_str() {
            "bool" => DataType::Bool,
            "i64" => DataType::I64,
            "f64" => DataType::F64,
            "datetime" => DataType::DateTime,
            "duration" => DataType::Duration,
            "str" => DataType::Str,
            "bytes" => DataType::Bytes,
            "array" => {
                self.expect_punct(Punct::Lt, "`<` after `array`")?;
                let elem = self.type_expr()?;
                self.close_angle()?;
                DataType::Array(Box::new(elem))
            }
            "variant" => DataType::Variant,
            other => {
                return Err(ParseError {
                    span,
                    message: format!(
                        "unknown type `{other}` (expected bool, i64, f64, datetime, duration, str, bytes, variant, \
                         array<T> or {{name: T, ...}})"
                    ),
                });
            }
        })
    }

    /// `else if` parses as a nested `If` in `else_`.
    fn if_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.advance(); // `if`
        self.expect_punct(Punct::LParen, "`(` after `if`")?;
        let cond = self.expr_bp(0)?;
        self.expect_punct(Punct::RParen, "`)` after `if` condition")?;
        let then = self.branch()?;
        // The `;` ending a braceless branch before `else`.
        if self.peek() == &Token::Punct(Punct::Semi) && self.peek_next() == &Token::Keyword(Keyword::Else) {
            self.advance();
        }
        let else_ = if self.peek() == &Token::Keyword(Keyword::Else) {
            self.advance();
            // Not through `branch`: an `else if` chain is a list, and only its tree nests.
            if self.peek() == &Token::Keyword(Keyword::If) {
                vec![self.if_stmt()?]
            } else {
                self.branch()?
            }
        } else {
            Vec::new()
        };
        Ok(Stmt::If { cond, then, else_ })
    }

    /// A branch of an `if`: a block, or one statement without braces, as TypeScript takes it. A `{` here always opens
    /// a block.
    fn branch(&mut self) -> Result<Vec<Stmt>, ParseError> {
        if self.peek() == &Token::Punct(Punct::LBrace) {
            return self.stmt_block();
        }
        let mut out = Vec::new();
        self.stmt(&mut out)?;
        Ok(out)
    }

    fn while_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.advance(); // `while`
        self.expect_punct(Punct::LParen, "`(` after `while`")?;
        let cond = self.expr_bp(0)?;
        self.expect_punct(Punct::RParen, "`)` after `while` condition")?;
        let body = self.stmt_block()?;
        Ok(Stmt::While {
            cond,
            body,
            next: Vec::new(),
        })
    }

    /// `for (let var of iterable) { body }`. `of` is a contextual keyword: an identifier spelled `of`, so a field named
    /// `of` still parses.
    fn for_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.advance(); // `for`
        self.expect_punct(Punct::LParen, "`(` after `for`")?;
        if self.peek() != &Token::Keyword(Keyword::Let) {
            return Err(self.error("expected `let` after `for (`"));
        }
        self.advance();
        let var = match self.advance() {
            Spanned {
                token: Token::Ident(n), ..
            } => n,
            s => {
                return Err(ParseError {
                    span: s.span,
                    message: "expected a loop variable name after `let`".into(),
                });
            }
        };
        if var.contains('.') {
            return Err(self.error(format!("a loop variable's name cannot be dotted: `{var}`")));
        }
        match self.advance() {
            Spanned {
                token: Token::Ident(s), ..
            } if s == "of" => {}
            s => {
                return Err(ParseError {
                    span: s.span,
                    message: "expected `of` in `for (let var of iterable)`".into(),
                });
            }
        }
        let iterable = self.expr_bp(0)?;
        self.expect_punct(Punct::RParen, "`)` after `for...of` iterable")?;
        let body = self.stmt_block()?;
        Ok(Stmt::For { var, iterable, body })
    }

    /// `{ stmt; ... }`: a loop's, a function's and a `try`'s block always has braces.
    fn stmt_block(&mut self) -> Result<Vec<Stmt>, ParseError> {
        let open = self.expect_punct(Punct::LBrace, "`{`")?;
        let mut out = Vec::new();
        while self.peek() != &Token::Punct(Punct::RBrace) {
            if self.at_eof() {
                return Err(self.error("unterminated block, expected `}`"));
            }
            self.stmt(&mut out)?;
            while self.eat_punct(Punct::Semi) {}
        }
        let close = self.advance().span; // `}`
        self.shape.blocks.push((open.start, close.start));
        Ok(out)
    }

    // -- expressions ---------------------------------------------------------

    /// The Pratt loop: parse an expression, stopping at an operator that binds looser than `min_bp`. Every expression
    /// recursion funnels through here, so the depth guard covers all of them; left- associative chains fold in the loop
    /// and instead of recursing.
    fn expr_bp(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            self.depth -= 1;
            return Err(self.too_deep());
        }
        let r = self.expr_bp_inner(min_bp);
        self.depth -= 1;
        r
    }

    fn expr_bp_inner(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.prefix()?;

        let mut folds: u32 = 0;
        loop {
            if AS_BP >= min_bp && self.peek() == &Token::Keyword(Keyword::As) {
                folds += 1;
                if self.depth + folds > MAX_DEPTH {
                    return Err(self.too_deep());
                }
                self.advance();
                let ty = self.type_expr()?;
                let span = lhs.span.join(self.last_span());
                lhs = Expr::new(
                    span,
                    ExprKind::Coerce {
                        ty,
                        value: Box::new(lhs),
                    },
                );
                continue;
            }
            let infix = if TERNARY_BP >= min_bp && self.peek() == &Token::Punct(Punct::Question) {
                None
            } else {
                match self.peek_binop() {
                    Some(op) if infix_bp(op).0 >= min_bp => Some(op),
                    _ => break,
                }
            };
            folds += 1;
            if self.depth + folds > MAX_DEPTH {
                return Err(self.too_deep());
            }
            self.advance();
            lhs = match infix {
                Some(op) => {
                    let rhs = self.expr_bp(infix_bp(op).1)?;
                    let span = lhs.span.join(rhs.span);
                    Expr::new(
                        span,
                        ExprKind::Binary {
                            op,
                            lhs: Box::new(lhs),
                            rhs: Box::new(rhs),
                        },
                    )
                }
                None => {
                    let then = self.expr_bp(0)?;
                    let colon = self.expect_punct(Punct::Colon, "`:` in ternary")?;
                    self.shape.ternary_colons.push(colon.start);
                    let else_ = self.expr_bp(TERNARY_BP)?;
                    let span = lhs.span.join(else_.span);
                    Expr::new(
                        span,
                        ExprKind::Ternary {
                            cond: Box::new(lhs),
                            then: Box::new(then),
                            else_: Box::new(else_),
                        },
                    )
                }
            }
        }

        Ok(lhs)
    }

    fn peek_binop(&self) -> Option<BinOp> {
        let Token::Punct(p) = self.peek() else {
            return None;
        };
        Some(match p {
            Punct::Plus => BinOp::Add,
            Punct::Minus => BinOp::Sub,
            Punct::Star => BinOp::Mul,
            Punct::Slash => BinOp::Div,
            Punct::Percent => BinOp::Mod,
            Punct::StarStar => BinOp::Pow,
            Punct::EqEq => BinOp::Eq,
            Punct::BangEq => BinOp::Ne,
            Punct::Lt => BinOp::Lt,
            Punct::Le => BinOp::Le,
            Punct::Gt => BinOp::Gt,
            Punct::Ge => BinOp::Ge,
            Punct::AmpAmp => BinOp::And,
            Punct::PipePipe => BinOp::Or,
            Punct::Amp => BinOp::BitAnd,
            Punct::Pipe => BinOp::BitOr,
            Punct::Caret => BinOp::BitXor,
            Punct::LtLt => BinOp::Shl,
            Punct::GtGt => BinOp::Shr,
            Punct::QuestionQuestion => BinOp::Coalesce,
            _ => return None,
        })
    }

    fn prefix(&mut self) -> Result<Expr, ParseError> {
        let start = self.peek_span();

        if let Token::Punct(p) = self.peek() {
            let unop = match p {
                Punct::Minus => Some(UnOp::Neg),
                Punct::Bang => Some(UnOp::Not),
                Punct::Tilde => Some(UnOp::BitNot),
                _ => None,
            };
            if let Some(op) = unop {
                self.advance();
                let operand = self.expr_bp(UNARY_BP)?;
                let span = start.join(operand.span);
                return Ok(Expr::new(
                    span,
                    ExprKind::Unary {
                        op,
                        operand: Box::new(operand),
                    },
                ));
            }
        }

        let atom = self.atom()?;
        self.postfix(atom)
    }

    /// Call, index and method call: tighter than any operator, chained left to right. Each wrap deepens the tree that
    /// later passes walk, so wraps count against `MAX_DEPTH` even though this loop does not recurse.
    fn postfix(&mut self, mut e: Expr) -> Result<Expr, ParseError> {
        let mut wraps: u32 = 0;
        loop {
            if matches!(
                self.peek(),
                Token::Punct(Punct::Dot) | Token::Punct(Punct::LParen) | Token::Punct(Punct::LBracket)
            ) {
                wraps += 1;
                if self.depth + wraps > MAX_DEPTH {
                    return Err(self.too_deep());
                }
            }
            // A dot here follows an expression, never a bare identifier chain (the lexer collapsed those).
            if self.peek() == &Token::Punct(Punct::Dot) {
                self.advance();
                let (name, name_span) = match self.advance() {
                    Spanned {
                        token: Token::Ident(n),
                        span,
                    } => (n, span),
                    s => {
                        return Err(ParseError {
                            span: s.span,
                            message: "expected a field or method name after `.`".into(),
                        });
                    }
                };
                // Member access desugars to the bracket form, one level per segment: `tags[0].geo.city` is
                // `tags[0]["geo"]["city"]`.
                if self.peek() != &Token::Punct(Punct::LParen) {
                    let span = e.span.join(name_span);
                    for segment in name.split('.') {
                        let index = Expr::new(name_span, ExprKind::Literal(Value::Str(segment.to_string())));
                        e = index_of(e, index, span);
                    }
                    continue;
                }
                self.expect_punct(Punct::LParen, "`(` after a method name")?;
                let recv_span = e.span;
                let (rest, end) = self.list(Punct::RParen, "`)` to close the argument list", |p| p.expr_bp(0))?;
                let args = std::iter::once(e).chain(rest).collect();
                e = Expr::new(recv_span.join(end), ExprKind::Call { name, args });
                continue;
            }
            if self.peek() == &Token::Punct(Punct::LParen) {
                self.advance();
                let ExprKind::Field(name) = &e.kind else {
                    return Err(self.error("only a named function can be called"));
                };
                let name = name.clone();
                let (args, end) = self.list(Punct::RParen, "`)` to close the argument list", |p| p.expr_bp(0))?;
                e = Expr::new(e.span.join(end), ExprKind::Call { name, args });
                continue;
            }
            if self.peek() == &Token::Punct(Punct::LBracket) {
                self.advance();
                let index = self.expr_bp(0)?;
                let end = self.expect_punct(Punct::RBracket, "`]`")?;
                let span = e.span.join(end);
                e = index_of(e, index, span);
                continue;
            }
            return Ok(e);
        }
    }

    /// `$["..."]`, after the `$`: the field whose name is the string, for a name the bare form cannot spell, such as
    /// `@timestamp` or `user-agent`. The same `Field` a bare name parses to, so everything after the parser treats the
    /// two alike.
    fn quoted_name(&mut self, dollar: Span) -> Result<Expr, ParseError> {
        self.advance(); // `[`
        let Token::Str(name) = self.advance().token else {
            unreachable!("`primary` sees a string before it calls this")
        };
        let mut end = self.expect_punct(Punct::RBracket, "`]` to close `$[...]`")?;
        let mut name = tree::spell(&name).into_owned();
        // A dot after it extends the name, as the lexer joins `a.b`: `$["x-y"].z` is the one field `x-y.z`. The
        // lexer joined what follows the dot too, so `.z.length(` arrives as one `z.length` before a `(`: the segments
        // before the last extend the name, and the last stays for `postfix` to call as the method.
        if let [
            Spanned {
                token: Token::Punct(Punct::Dot),
                ..
            },
            Spanned {
                token: Token::Ident(segments),
                span,
            },
            after,
            ..,
        ] = &self.tokens[self.pos..]
        {
            let is_call = after.token == Token::Punct(Punct::LParen);
            match (is_call, segments.rsplit_once('.')) {
                (false, _) => {
                    name = tree::join(&name, segments);
                    end = *span;
                    self.pos += 2;
                }
                (true, Some((lead, method))) => {
                    name = tree::join(&name, lead);
                    let method = method.to_string();
                    self.tokens[self.pos + 1].token = Token::Ident(method);
                }
                (true, None) => {}
            }
        }
        Ok(Expr::new(dollar.join(end), ExprKind::Field(name)))
    }

    /// An arrow from its `=>`, its parameters read: `{ ... }` is a function body, and anything else is the one
    /// expression it returns.
    fn arrow(&mut self, mut function: Function, start: Span) -> Result<Expr, ParseError> {
        self.advance(); // `=>`
        function.body = if self.peek() == &Token::Punct(Punct::LBrace) {
            self.function_body()?
        } else {
            vec![Stmt::Return(self.expr_bp(0)?)]
        };
        Ok(Expr::new(start.join(self.last_span()), ExprKind::Arrow(function)))
    }

    /// `(a, b: T): R` with `=>` after it, read from its `(` as a function with no body yet, or `None` with nothing
    /// read: `(a)` alone is a parenthesized expression.
    fn arrow_head(&mut self) -> Option<Function> {
        let at = self.pos;
        let mark = self.shape.mark();
        self.advance(); // `(`
        let head = match (self.params(), self.return_type()) {
            (Ok((params, param_types)), Ok(ret)) if self.peek() == &Token::Punct(Punct::FatArrow) => Some(Function {
                params,
                param_types,
                ret,
                body: Vec::new(),
            }),
            _ => None,
        };
        if head.is_none() {
            self.pos = at;
            self.shape.truncate(mark);
        }
        head
    }

    fn atom(&mut self) -> Result<Expr, ParseError> {
        let sp = self.peek_span();
        match self.peek().clone() {
            Token::Int(v) => {
                self.advance();
                Ok(Expr::new(sp, ExprKind::Literal(Value::I64(v))))
            }
            Token::Float(v) => {
                self.advance();
                Ok(Expr::new(sp, ExprKind::Literal(Value::F64(v))))
            }
            Token::Str(s) => {
                self.advance();
                Ok(Expr::new(sp, ExprKind::Literal(Value::Str(s))))
            }
            Token::Template(parts) => {
                self.advance();
                self.template(parts, sp)
            }
            Token::Regex { source, flags } => {
                self.advance();
                Ok(Expr::new(sp, ExprKind::Regex(PatternSrc { source, flags })))
            }
            Token::Ident(name) => {
                self.advance();
                // `$["k"]` is the name `k`. `$[k]` is a key only the row knows, read as any object's is.
                if name == "$"
                    && let [
                        _,
                        Spanned {
                            token: Token::Str(_), ..
                        },
                        Spanned {
                            token: Token::Punct(Punct::RBracket),
                            ..
                        },
                        ..,
                    ] = &self.tokens[self.pos..]
                    && self.peek() == &Token::Punct(Punct::LBracket)
                {
                    return self.quoted_name(sp);
                }
                if self.peek() == &Token::Punct(Punct::FatArrow) {
                    if name.contains('.') || name == tree::ROOT {
                        return Err(ParseError {
                            span: sp,
                            message: format!("a parameter is a plain name, found `{name}`"),
                        });
                    }
                    return self.arrow(
                        Function {
                            params: vec![name],
                            param_types: vec![None],
                            ret: None,
                            body: Vec::new(),
                        },
                        sp,
                    );
                }
                Ok(Expr::new(sp, ExprKind::Field(name)))
            }
            Token::Keyword(Keyword::True) => {
                self.advance();
                Ok(Expr::new(sp, ExprKind::Literal(Value::Bool(true))))
            }
            Token::Keyword(Keyword::False) => {
                self.advance();
                Ok(Expr::new(sp, ExprKind::Literal(Value::Bool(false))))
            }
            Token::Keyword(Keyword::Null) => {
                self.advance();
                Ok(Expr::new(sp, ExprKind::Literal(Value::Null)))
            }
            Token::Punct(Punct::LParen) => {
                if let Some(head) = self.arrow_head() {
                    return self.arrow(head, sp);
                }
                self.advance();
                let inner = self.expr_bp(0)?;
                self.expect_punct(Punct::RParen, "`)`")?;
                Ok(inner)
            }
            Token::Punct(Punct::LBracket) => {
                self.advance();
                let (items, end) = self.list(Punct::RBracket, "`]` to close the array", |p| p.expr_bp(0))?;
                Ok(Expr::new(sp.join(end), ExprKind::Array(items)))
            }
            Token::Punct(Punct::LBrace) => {
                self.advance();
                let (fields, end) = self.list(Punct::RBrace, "`}` to close the object", |p| {
                    if p.eat_punct(Punct::Ellipsis) {
                        return Ok((None, p.expr_bp(0)?));
                    }
                    let (key, shorthand) = match p.advance() {
                        Spanned {
                            token: Token::Ident(n),
                            span,
                        } => (n.clone(), Some(Expr::new(span, ExprKind::Field(n)))),
                        Spanned {
                            token: Token::Str(s), ..
                        } => (tree::spell(&s).into_owned(), None),
                        s => {
                            return Err(ParseError {
                                span: s.span,
                                message: "object keys must be names or strings".into(),
                            });
                        }
                    };
                    // `{level}` is `{level: level}`, for a plain name only, as in TypeScript: not `a.b`, nor `$.a`,
                    // which the lexer spells `a`.
                    if let Some(value) = shorthand
                        && tree::is_plain(&key)
                        && value.span.slice(p.src) == key
                        && matches!(p.peek(), Token::Punct(Punct::Comma | Punct::RBrace))
                    {
                        return Ok((Some(key), value));
                    }
                    p.expect_punct(Punct::Colon, "`:` after an object key")?;
                    Ok((Some(key), p.expr_bp(0)?))
                })?;
                Ok(Expr::new(sp.join(end), ExprKind::Object(fields)))
            }
            _ => Err(self.error(format!("expected an expression, found {}", self.describe()))),
        }
    }
}

/// `base[index]`. A string key applied to a name is the name of that key, `o["a.b"]` the field `o["a.b"]`, so a read, a
/// write and a declaration of it are one name, as `$["a.b"]` is.
pub fn index_of(base: Expr, index: Expr, span: Span) -> Expr {
    if let (ExprKind::Field(name), ExprKind::Literal(Value::Str(key))) = (&base.kind, &index.kind) {
        return Expr::new(span, ExprKind::Field(tree::child(name, key)));
    }
    let kind = ExprKind::Index {
        base: Box::new(base),
        index: Box::new(index),
    };
    Expr::new(span, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A type reads back as it is spelled, whatever it nests.
    #[test]
    fn a_spelled_type_parses_back() {
        let types = [
            DataType::I64,
            DataType::DateTime,
            DataType::Variant,
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::F64)))),
            DataType::Struct(
                vec![
                    ("ip".into(), DataType::Str),
                    ("a.b".into(), DataType::Array(Box::new(DataType::Bool))),
                ],
                true,
            ),
        ];
        for ty in types {
            assert_eq!(parse_type(&ty.spell()).unwrap(), ty, "{}", ty.spell());
        }
        assert!(parse_type("i64 extra").is_err());
    }

    /// Fully parenthesized rendering.
    fn sexp(e: &Expr) -> String {
        match &e.kind {
            ExprKind::Literal(Value::I64(v)) => v.to_string(),
            ExprKind::Literal(Value::F64(v)) => v.to_string(),
            ExprKind::Literal(Value::Str(s)) => format!("{s:?}"),
            ExprKind::Literal(Value::Bool(b)) => b.to_string(),
            ExprKind::Literal(Value::Null) => "null".into(),
            ExprKind::Literal(_) => "<lit>".into(),
            ExprKind::Field(n) => n.clone(),
            ExprKind::Binary { op, lhs, rhs } => {
                format!("({} {} {})", sexp(lhs), op.symbol(), sexp(rhs))
            }
            ExprKind::Unary { op, operand } => {
                let s = match op {
                    UnOp::Neg => "-",
                    UnOp::Not => "!",
                    UnOp::BitNot => "~",
                };
                format!("({}{})", s, sexp(operand))
            }
            ExprKind::Ternary { cond, then, else_ } => {
                format!("({} ? {} : {})", sexp(cond), sexp(then), sexp(else_))
            }
            ExprKind::Call { name, args } => {
                let a: Vec<String> = args.iter().map(sexp).collect();
                format!("{}({})", name, a.join(", "))
            }
            ExprKind::Index { base, index } => format!("{}[{}]", sexp(base), sexp(index)),
            ExprKind::SetElement { array, index, value } => {
                format!("({}[{}] = {})", sexp(array), sexp(index), sexp(value))
            }
            ExprKind::Array(items) => {
                let a: Vec<String> = items.iter().map(sexp).collect();
                format!("[{}]", a.join(", "))
            }
            ExprKind::Object(f) => {
                let a: Vec<String> = f
                    .iter()
                    .map(|(k, v)| match k {
                        Some(k) => format!("{k}: {}", sexp(v)),
                        None => format!("...{}", sexp(v)),
                    })
                    .collect();
                format!("{{{}}}", a.join(", "))
            }
            ExprKind::Regex(p) => format!("/{}/", p.source),
            ExprKind::Arrow(f) => match f.body.as_slice() {
                [Stmt::Return(e)] => format!("({} => {})", f.params.join(", "), sexp(e)),
                body => format!("({} => {body:?})", f.params.join(", ")),
            },
            ExprKind::Coerce { ty, value } => format!("({} as {ty})", sexp(value)),
            ExprKind::Check { ty, value, .. } => format!("({} is {ty})", sexp(value)),
        }
    }

    fn p(src: &str) -> String {
        sexp(&parse_expr(src).unwrap_or_else(|e| panic!("{src}: {}", e.message)))
    }

    /// `$["..."]` is the field whose key is the string, the same node a bare name is, and a dot after it extends the
    /// name the way the lexer joins `a.b`. A method call after it is a call on that field. A key that is not plain is
    /// spelled in brackets.
    #[test]
    fn a_quoted_name_is_a_field() {
        assert_eq!(p(r#"$["@timestamp"]"#), "@timestamp");
        assert_eq!(p(r#"$["a.b"].c"#), r#"["a.b"].c"#);
        assert_eq!(p(r#"$["user-agent"].length()"#), "length(user-agent)");
        assert_eq!(p(r#"$["x-y"].z"#), "x-y.z");
        assert_eq!(p(r#"$["a"] + 1"#), "(a + 1)");
        assert_eq!(p(r#"$["tags"][0]"#), p("tags[0]"));
        assert_eq!(p(r#"$["x-y"].z.length()"#), "length(x-y.z)");
        assert_eq!(p("(a).b"), p(r#"a["b"]"#), "a dot after a value still indexes it");
    }

    #[test]
    fn a_quoted_name_is_a_string_and_any_other_key_indexes_the_root() {
        assert_eq!(p(r#"$["x"]"#), "x");
        assert_eq!(
            p("$[name]"),
            p("o[name]").replace('o', "$"),
            "a computed key reads the root as any object"
        );
        assert_eq!(p(r#"$["a" + b]"#), p(r#"o["a" + b]"#).replacen('o', "$", 1));
        assert_eq!(p("$"), "$", "a bare `$` is the root object");
    }

    #[test]
    fn precedence_multiplication_over_addition() {
        assert_eq!(p("a + b * 2"), "(a + (b * 2))");
        assert_eq!(p("a * b + 2"), "((a * b) + 2)");
    }

    #[test]
    fn parentheses_override_precedence_then_vanish() {
        assert_eq!(p("(a + b) * 2"), "((a + b) * 2)");
    }

    #[test]
    fn subtraction_is_left_associative() {
        assert_eq!(p("a - b - c"), "((a - b) - c)");
        assert_eq!(p("a / b / c"), "((a / b) / c)");
    }

    #[test]
    fn exponentiation_is_right_associative() {
        assert_eq!(p("a ** b ** c"), "(a ** (b ** c))");
    }

    #[test]
    fn comparison_binds_looser_than_arithmetic() {
        assert_eq!(p("a + 1 < b * 2"), "((a + 1) < (b * 2))");
    }

    #[test]
    fn logical_and_binds_tighter_than_or() {
        assert_eq!(p("a || b && c"), "(a || (b && c))");
        assert_eq!(p("a && b || c"), "((a && b) || c)");
    }

    #[test]
    fn equality_binds_looser_than_comparison() {
        assert_eq!(p("a < b == c < d"), "((a < b) == (c < d))");
    }

    #[test]
    fn bitwise_precedence_chain() {
        assert_eq!(p("a | b ^ c & d"), "(a | (b ^ (c & d)))");
    }

    #[test]
    fn bitwise_binds_tighter_than_comparison() {
        // Unlike C and JS: `flags & 3 == 3` is `(flags & 3) == 3`, not `flags & (3 == 3)`.
        assert_eq!(p("flags & 3 == 3"), "((flags & 3) == 3)");
        assert_eq!(p("a | b != c"), "((a | b) != c)");
    }

    #[test]
    fn shifts_bind_tighter_than_comparison_looser_than_addition() {
        assert_eq!(p("a + b << c"), "((a + b) << c)");
        assert_eq!(p("a << b < c"), "((a << b) < c)");
    }

    #[test]
    fn coalesce_binds_loosest() {
        assert_eq!(p("a ?? b + 1"), "(a ?? (b + 1))");
        assert_eq!(p("a ?? b ?? c"), "((a ?? b) ?? c)");
    }

    #[test]
    fn unary_binds_tighter_than_binary() {
        assert_eq!(p("-a + b"), "((-a) + b)");
        assert_eq!(p("-a * b"), "((-a) * b)");
        assert_eq!(p("!a && b"), "((!a) && b)");
        assert_eq!(p("- -a"), "(-(-a))");
    }

    #[test]
    fn unary_binds_looser_than_exponentiation() {
        assert_eq!(p("-a ** b"), "(-(a ** b))");
    }

    /// `as` binds as in TypeScript: looser than arithmetic and bitwise, tighter than comparison, logic, `??` and
    /// the ternary. The type is not an expression, so an operator after it always applies to the coerced value.
    #[test]
    fn as_binds_between_arithmetic_and_comparison() {
        assert_eq!(p("a + b as str"), "((a + b) as str)");
        assert_eq!(p("x as i64 == 1"), "((x as i64) == 1)");
        assert_eq!(p("1 == x as i64"), "(1 == (x as i64))");
        assert_eq!(p("n as f64 * 2"), "((n as f64) * 2)");
        assert_eq!(p("flags & 3 as i64"), "((flags & 3) as i64)");
        assert_eq!(p("a as i64 as str"), "((a as i64) as str)");
        assert_eq!(p("-a as i64"), "((-a) as i64)");
        assert_eq!(p("a ?? b as str"), "(a ?? (b as str))");
        assert_eq!(p("c ? a : b as str"), "(c ? a : (b as str))");
        assert_eq!(p("x as array<i64>"), "(x as array(i64))");
        assert_eq!(p("x as i64 > 1"), "((x as i64) > 1)");
        assert_eq!(p("f(x) as str"), "(f(x) as str)");
        assert_eq!(p("(a as i64).toString()"), "toString((a as i64))");
        let e = parse_expr("x as 1").unwrap_err();
        assert!(e.message.contains("type"), "{}", e.message);
        let e = parse_expr("x as").unwrap_err();
        assert!(e.message.contains("type"), "{}", e.message);
    }

    #[test]
    fn ternary_is_right_associative() {
        assert_eq!(p("a ? b : c ? d : e"), "(a ? b : (c ? d : e))");
    }

    #[test]
    fn ternary_binds_looser_than_comparison() {
        assert_eq!(p("a < b ? c : d"), "((a < b) ? c : d)");
    }

    #[test]
    fn ternary_branches_can_hold_full_expressions() {
        assert_eq!(p("a ? b + 1 : c * 2"), "(a ? (b + 1) : (c * 2))");
    }

    #[test]
    fn dotted_fields_are_atoms() {
        assert_eq!(p("source.ip == dest.ip"), "(source.ip == dest.ip)");
    }

    #[test]
    fn calls_and_nested_calls() {
        assert_eq!(p("parseInt(x)"), "parseInt(x)");
        assert_eq!(p("f(a, b + 1)"), "f(a, (b + 1))");
        assert_eq!(p("f(g(x))"), "f(g(x))");
        assert_eq!(p("f()"), "f()");
    }

    #[test]
    fn call_binds_tighter_than_any_operator() {
        assert_eq!(p("f(x) + 1"), "(f(x) + 1)");
        assert_eq!(p("-f(x)"), "(-f(x))");
    }

    #[test]
    fn indexing_chains() {
        assert_eq!(p("a[0]"), "a[0]");
        assert_eq!(p("a[0][1]"), "a[0][1]");
        assert_eq!(p("a[i + 1]"), "a[(i + 1)]");
    }

    #[test]
    fn literals() {
        assert_eq!(p("true"), "true");
        assert_eq!(p("null"), "null");
        assert_eq!(p(r#""hi""#), "\"hi\"");
        assert_eq!(p("1.5"), "1.5");
    }

    #[test]
    fn arrays_and_objects() {
        assert_eq!(p("[1, x, y + 2]"), "[1, x, (y + 2)]");
        assert_eq!(p("[]"), "[]");
        assert_eq!(p("{a: 1, b: x}"), "{a: 1, b: x}");
    }

    #[test]
    fn every_list_takes_a_trailing_comma() {
        assert_eq!(p("[1, 2,]"), "[1, 2]");
        assert_eq!(p("{a: 1, ...b,}"), p("{a: 1, ...b}"));
        assert_eq!(p("f(a, b,)"), "f(a, b)");
        assert_eq!(p("s.trim(a,)"), p("s.trim(a)"));
        assert_eq!(
            block("function f(a: i64,): i64 { return a; }"),
            block("function f(a: i64 ): i64 { return a; }")
        );
        assert_eq!(block("x: {a: i64, ...,};"), block("x: {a: i64, ...};"));
        for src in ["[,]", "f(,)", "{,}", "[1,,]", "f(a b)"] {
            assert!(parse_expr(src).is_err(), "{src}");
        }
    }

    #[test]
    fn a_function_body_ending_without_a_semicolon_returns_its_last_expression() {
        // Spans differ between the two spellings, so they are left out of the comparison.
        let spans = regex::Regex::new(r"span: Span \{ start: \d+, end: \d+ \}").unwrap();
        let f = |body: &str| {
            let b = block(&format!("function f(x: i64): i64 {{ {body} }}"));
            spans.replace_all(&b, "").into_owned()
        };
        assert_eq!(f("let y = x; y + 1"), f("let y = x; return y + 1;"));
        assert_eq!(f("x"), f("return x;"));
        assert_eq!(
            f("if (x > 0) { 1 } else if (x < 0) { -1 } else { 0 }"),
            f("if (x > 0) { return 1; } else if (x < 0) { return -1; } else { return 0; }")
        );
        assert_eq!(
            f("if (x > 0) 1 else if (x < 0) -1 else 0"),
            f("if (x > 0) { return 1; } else if (x < 0) { return -1; } else { return 0; }")
        );
        assert_eq!(f("{...o}"), f("return {...o};"));
        // With a `;`, or anywhere but the end, it stays a statement.
        assert!(!f("x + 1;").contains("Return"));
        assert_eq!(f("if (x > 0) { x + 1 } return 0;").matches("Return").count(), 1);
        // A bare name or a lone spread that no function takes is still no statement.
        for src in [
            "if (a) { x }",
            "switch (a) { case 1: x }",
            "while (c) { if (a) x }",
            "if (a) x else { f(y); }",
            "{...a}",
            "if (c) { {...a} }",
        ] {
            assert!(parse_block(src).is_err(), "{src}");
        }
        assert!(parse_block("function f(x: i64): i64 { if (x > 0) { x } return 0; }").is_err());
    }

    #[test]
    fn a_plain_name_alone_is_a_key_and_its_value() {
        assert_eq!(p("{level}"), p("{level: level}"));
        assert_eq!(p("{a, b: 1, ...c, d,}"), p("{a: a, b: 1, ...c, d: d}"));
        assert_eq!(p("xs.map(x => ({x}))"), p("xs.map(x => ({x: x}))"));
        for src in ["{a.b}", "{$.a}", "{\"a\"}", "{a b}", "xs.map(x => {a: 1})"] {
            assert!(parse_expr(src).is_err(), "{src}");
        }
    }

    #[test]
    fn regex_literal_in_an_expression() {
        assert_eq!(p("x == /abc/i"), "(x == /abc/)");
    }

    #[test]
    fn realistic_pipeline_expression() {
        assert_eq!(
            p("status >= 400 && status < 500 ? \"client\" : \"other\""),
            "(((status >= 400) && (status < 500)) ? \"client\" : \"other\")"
        );
    }

    // --- errors ---

    #[test]
    fn missing_operand_is_an_error() {
        assert!(parse_expr("a +").is_err());
        assert!(parse_expr("* b").is_err());
    }

    #[test]
    fn unbalanced_parens_are_an_error() {
        assert!(parse_expr("(a + b").is_err());
        assert!(parse_expr("a + b)").is_err());
    }

    #[test]
    fn ternary_without_colon_is_an_error() {
        let e = parse_expr("a ? b").unwrap_err();
        assert!(e.message.contains(':'), "{}", e.message);
    }

    #[test]
    fn trailing_tokens_are_rejected() {
        assert!(parse_expr("a b").is_err());
    }

    #[test]
    fn an_error_span_covers_the_token_at_fault() {
        let src = "a + + ";
        let e = parse_expr(src).unwrap_err();
        assert_eq!((e.span.start, e.span.slice(src)), (4, "+"), "the second `+`");
    }

    // --- statements ---

    #[test]
    fn assignment_statement() {
        let s = parse_block("source.ip = a + b").unwrap();
        match &s[0] {
            Stmt::Assign { target, value, .. } => {
                assert_eq!(target, "source.ip");
                assert_eq!(sexp(value), "(a + b)");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn let_binding() {
        let s = parse_block("let x = 1 + 2").unwrap();
        match &s[0] {
            Stmt::Let { name, value } => {
                assert_eq!(name, "x");
                assert_eq!(sexp(value), "(1 + 2)");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn multiple_statements_with_semicolons() {
        let s = parse_block("let x = 1; y = x * 2; z = y").unwrap();
        assert_eq!(s.len(), 3);
    }

    #[test]
    fn only_a_field_or_an_index_into_one_can_be_assigned_to() {
        for src in ["f(x) = 1", "1 = 2", "f(x) += 1", "f(x)++", "a[0][1] = 2"] {
            assert!(parse_block(src).is_err(), "{src} should be refused");
        }
    }

    /// `=` accepted an indexed target and the compound forms did not, though all three
    /// reach `Stmt::IndexAssign`.
    #[test]
    fn every_assigning_form_takes_an_indexed_target() {
        match &parse_block("counts[k] = 1").unwrap()[0] {
            Stmt::IndexAssign { target, index, value } => {
                assert_eq!(target, "counts");
                assert_eq!(sexp(index), "k");
                assert_eq!(sexp(value), "1");
            }
            other => panic!("got {other:?}"),
        }

        match &parse_block("counts[k] += 1").unwrap()[0] {
            Stmt::IndexAssign { target, index, value } => {
                assert_eq!(target, "counts");
                assert_eq!(sexp(index), "k");
                assert_eq!(sexp(value), "(counts[k] + 1)", "reads the element it writes");
            }
            other => panic!("got {other:?}"),
        }

        match &parse_block("counts[k]++").unwrap()[0] {
            Stmt::IndexAssign { target, index, value } => {
                assert_eq!(target, "counts");
                assert_eq!(sexp(index), "k");
                assert_eq!(sexp(value), "(counts[k] + 1)");
            }
            other => panic!("got {other:?}"),
        }
    }

    /// A name on its own does nothing, so it is not a statement. `delete x` is the shape that would otherwise
    /// pass as two of them and compile to nothing.
    #[test]
    fn a_bare_name_is_not_a_statement() {
        for (src, name) in [("delete x;", "delete"), ("x", "x"), ("a = 1; source.ip", "source.ip")] {
            let e = parse_block(src).unwrap_err();
            assert_eq!(e.message, format!("`{name}` on its own is not a statement"), "{src}");
            assert_eq!(&src[e.span.start as usize..e.span.end as usize], name, "{src}");
        }
        assert!(matches!(parse_block("drop();").unwrap()[0], Stmt::Drop(_)));
        assert!(matches!(parse_block("unroll(f)").unwrap()[0], Stmt::Unroll { .. }));
        assert!(matches!(parse_block("x: i64").unwrap()[0], Stmt::Declare { .. }));
        assert!(matches!(parse_block("f(x)").unwrap()[0], Stmt::Expr(_)));
    }

    /// One line per statement, nested blocks indented, so a test can compare a whole parse.
    fn render(stmts: &[Stmt]) -> String {
        fn go(stmts: &[Stmt], indent: usize, out: &mut String) {
            let pad = "  ".repeat(indent);
            for s in stmts {
                let line = match s {
                    Stmt::Assign { target, value, .. } => format!("{target} = {}", sexp(value)),
                    Stmt::Let { name, value } => format!("let {name} = {}", sexp(value)),
                    Stmt::Expr(e) => sexp(e),
                    Stmt::Delete { names, .. } => format!("delete({})", names.join(", ")),
                    Stmt::Drop(_) => "drop()".into(),
                    Stmt::Unroll { field, keep, .. } => format!("unroll({field}, {keep})"),
                    Stmt::Throw(e) => format!("throw {}", sexp(e)),
                    Stmt::Leave { to: Region::Loop, .. } => "break".into(),
                    Stmt::If { cond, then, else_ } => {
                        out.push_str(&format!("{pad}if {}\n", sexp(cond)));
                        go(then, indent + 1, out);
                        if !else_.is_empty() {
                            out.push_str(&format!("{pad}else\n"));
                            go(else_, indent + 1, out);
                        }
                        continue;
                    }
                    Stmt::While { cond, body, .. } => {
                        out.push_str(&format!("{pad}while {}\n", sexp(cond)));
                        go(body, indent + 1, out);
                        continue;
                    }
                    Stmt::Try { body, catches, finally } => {
                        out.push_str(&format!("{pad}try\n"));
                        go(body, indent + 1, out);
                        for (var, _, catch) in catches {
                            out.push_str(&format!("{pad}catch {}\n", var.as_deref().unwrap_or("_")));
                            go(catch, indent + 1, out);
                        }
                        if !finally.is_empty() {
                            out.push_str(&format!("{pad}finally\n"));
                            go(finally, indent + 1, out);
                        }
                        continue;
                    }
                    other => format!("{other:?}"),
                };
                out.push_str(&format!("{pad}{line}\n"));
            }
        }
        let mut out = String::new();
        go(stmts, 0, &mut out);
        out
    }

    fn block(src: &str) -> String {
        render(&parse_block(src).unwrap_or_else(|e| panic!("{src}: {}", e.message)))
    }

    fn block_err(src: &str) -> String {
        parse_block(src).expect_err(src).message
    }

    /// An `if` or `else` branch takes one statement without braces, as TypeScript does; a dangling `else` is the
    /// nearer `if`'s. A loop's body keeps its braces.
    #[test]
    fn an_if_branch_takes_one_statement_without_braces() {
        assert_eq!(
            block("if (c) x = 1; else x = 2; y = 3;"),
            "if c\n  x = 1\nelse\n  x = 2\ny = 3\n"
        );
        assert_eq!(block("if (c) x = 1; y = 2;"), "if c\n  x = 1\ny = 2\n");
        assert_eq!(block("if (c) x = 1 else { x = 2; }"), "if c\n  x = 1\nelse\n  x = 2\n");
        assert_eq!(
            block("if (a) x = 1; else if (b) x = 2; else x = 3;"),
            "if a\n  x = 1\nelse\n  if b\n    x = 2\n  else\n    x = 3\n"
        );
        assert_eq!(
            block("if (a) if (b) x = 1; else x = 2;"),
            "if a\n  if b\n    x = 1\n  else\n    x = 2\n"
        );
        assert_eq!(block("if (c) drop()"), "if c\n  drop()\n");
        assert!(parse_block("while (c) x = 1;").is_err());
        assert!(parse_block("for (let i of a) x = i;").is_err());
    }

    #[test]
    fn delete_takes_one_or_more_names() {
        assert_eq!(block(r#"delete(a, b.c, $["x.y"]);"#), "delete(a, b.c, [\"x.y\"])\n");
        let src = "delete(a, f(b))";
        let e = parse_block(src).unwrap_err();
        assert!(e.message.contains("`f(b)` is not one"), "{}", e.message);
        assert_eq!(e.span.slice(src), "f(b)");
        assert!(block_err("delete()").contains("one or more names"));
        assert!(block_err("delete(a[0])").contains("`a[0]` is not one"));
        let e = block_err("delete(a) = 1");
        assert!(e.contains("left side of `=`"), "{e}");
    }

    #[test]
    fn drop_takes_no_arguments() {
        assert_eq!(block("drop()"), "drop()\n");
        assert_eq!(
            block_err("drop(x)"),
            "`drop()` takes no arguments; write `if (cond) drop()`"
        );
    }

    #[test]
    fn unroll_takes_a_name_and_a_literal_keep() {
        assert_eq!(block("unroll(tags)"), "unroll(tags, false)\n");
        assert_eq!(block("unroll(a.tags, true)"), "unroll(a.tags, true)\n");
        assert!(block_err("unroll(tags, k)").contains("`true` or `false`, found `k`"));
        assert!(block_err("unroll(tags[0])").contains("is not one"));
        assert!(block_err("unroll()").contains("takes a name"));
        assert!(block_err("unroll(a, true, 1)").contains("takes a name"));
    }

    /// `move(from, to)` is three statements, wherever a statement goes; each `move` has its own temporary.
    #[test]
    fn move_is_a_let_a_delete_and_a_write() {
        assert_eq!(
            block("move(a.b, a.d); move(x, y)"),
            "let #move1 = a.b\ndelete(a.b)\na.d = #move1\nlet #move2 = x\ndelete(x)\ny = #move2\n"
        );
        assert_eq!(
            block("if (c) move(a, b); else x = 1;"),
            "if c\n  let #move1 = a\n  delete(a)\n  b = #move1\nelse\n  x = 1\n"
        );
        assert!(block_err("move(a)").contains("two names"));
        assert!(block_err("move(a, b, c)").contains("two names"));
        assert!(block_err("move(a, \"b\")").contains("`\"b\"` is not one"));
    }

    /// A spread is no statement: `Object.assign($, e)` writes the record.
    #[test]
    fn a_lone_spread_is_no_statement() {
        assert!(block_err("{...json(_raw)}").contains("write `Object.assign($, e)`"));
        assert!(block_err("if (c) {...a}").contains("write `Object.assign($, e)`"));
        assert_eq!(
            block("Object.assign($, json(_raw)); x = 1;"),
            "$ = Object.assign($, json(_raw))\nx = 1\n"
        );
        assert!(matches!(parse_block("{a: 1}").unwrap()[0], Stmt::Expr(_)));
        assert!(matches!(parse_block("{...a, b: 1}").unwrap()[0], Stmt::Expr(_)));
    }

    /// `try` and `catch` are names everywhere a statement does not start with `try {`.
    #[test]
    fn try_takes_a_catch_with_or_without_a_name() {
        assert_eq!(
            block("try { x = parseInt(s); } catch { x = 0; }"),
            "try\n  x = parseInt(s)\ncatch _\n  x = 0\n"
        );
        assert_eq!(
            block("try { x = 1; y = 2; } catch (e) { err = e.message; }"),
            "try\n  x = 1\n  y = 2\ncatch e\n  err = e.message\n"
        );
        assert!(block_err("try { x = 1; }").contains("expected `catch`"));
        assert!(block_err("try { x = 1; } catch (e.f) { }").contains("plain name"));
        assert_eq!(
            block("try = 1; catch = try; default = 2"),
            "try = 1\ncatch = try\ndefault = 2\n"
        );
    }

    /// A `switch` is an `if` chain over `e == v`. A case falls through into the next one until a `break`, labels with
    /// no statements share the next case's, and `default` is the final `else`. No `break` is left behind.
    #[test]
    fn a_switch_is_an_if_chain_with_fallthrough() {
        assert_eq!(
            block(r#"switch (k) { case "a": x = 1; case "b": y = 2; break; default: z = 3; }"#),
            "if (k == \"a\")\n  x = 1\n  y = 2\nelse\n  if (k == \"b\")\n    y = 2\n  else\n    z = 3\n"
        );
        assert_eq!(
            block(r#"switch (k) { case "a": case "b": x = 1; break; case "c": break; }"#),
            "if ((k == \"a\") || (k == \"b\"))\n  x = 1\nelse\n  if (k == \"c\")\n"
        );
        // The last case falls through into `default`; a case with no `break` at the end runs to the end.
        assert_eq!(
            block(r#"switch (k) { case 1: x = 1; default: z = 3; }"#),
            "if (k == 1)\n  x = 1\n  z = 3\nelse\n  z = 3\n"
        );
        assert_eq!(block("switch (k) { default: z = 3; break; }"), "z = 3\n");
        assert_eq!(block("switch (k) { }"), "");
        assert_eq!(block("switch (k) { case 1: }"), "");
        // A `break` in a loop inside a case is the loop's.
        assert_eq!(
            block("switch (k) { case 1: while (c) { c = false; break; } }"),
            "if (k == 1)\n  while c\n    c = false\n    break\n"
        );
        assert!(
            !format!(
                "{:?}",
                parse_block("for (let i of a) { switch (i) { case 1: x = 1; break; } }").unwrap()
            )
            .contains("Leave")
        );
    }

    #[test]
    fn a_switch_refuses_a_misplaced_break_or_default() {
        for src in [
            "switch (k) { case 1: break; x = 1; }",
            "switch (k) { case 1: if (c) { break; } x = 1; }",
            "switch (k) { case 1: if (c) break; }",
        ] {
            assert_eq!(block_err(src), BREAK_ENDS_A_CASE, "{src}");
        }
        assert_eq!(
            block_err("switch (k) { default: x = 1; case 1: y = 2; }"),
            "`default` must be the last case of a `switch`"
        );
        assert_eq!(
            block_err("switch (k) { default: x = 1; default: y = 2; }"),
            "`default` must be the last case of a `switch`"
        );
        assert!(block_err("switch (k) { x = 1; }").contains("expected `case` or `default`"));
        assert!(block_err("switch (k) { case 1: x = 1;").contains("unterminated"));
    }

    #[test]
    fn a_bare_field_still_takes_every_assigning_form() {
        for (src, want) in [("n = 1", "1"), ("n += 2", "(n + 2)"), ("n++", "(n + 1)")] {
            match &parse_block(src).unwrap()[0] {
                Stmt::Assign { target, value, .. } => {
                    assert_eq!(target, "n", "{src}");
                    assert_eq!(sexp(value), want, "{src}");
                }
                other => panic!("{src}: got {other:?}"),
            }
        }
    }

    #[test]
    fn an_if_statement_with_no_else() {
        let s = parse_block("if (a > b) { x = 1; }").unwrap();
        match &s[0] {
            Stmt::If { cond, then, else_ } => {
                assert_eq!(sexp(cond), "(a > b)");
                assert_eq!(then.len(), 1);
                assert!(else_.is_empty());
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn an_if_else_statement() {
        let s = parse_block("if (a) { x = 1; } else { x = 2; }").unwrap();
        match &s[0] {
            Stmt::If { then, else_, .. } => {
                assert_eq!(then.len(), 1);
                assert_eq!(else_.len(), 1);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn else_if_chains_without_needing_a_wrapping_block() {
        let s = parse_block("if (a) { x = 1; } else if (b) { x = 2; } else { x = 3; }").unwrap();
        match &s[0] {
            Stmt::If { else_, .. } => {
                assert_eq!(else_.len(), 1);
                match &else_[0] {
                    Stmt::If { then, else_, .. } => {
                        assert_eq!(then.len(), 1);
                        assert_eq!(else_.len(), 1);
                    }
                    other => panic!("got {other:?}"),
                }
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn an_if_condition_must_be_parenthesized() {
        assert!(parse_block("if a { x = 1; }").is_err());
    }

    #[test]
    fn a_for_of_loop_over_a_literal_array() {
        let s = parse_block("for (let x of [1, 2, 3]) { y = x; }").unwrap();
        match &s[0] {
            Stmt::For { var, iterable, body } => {
                assert_eq!(var, "x");
                assert!(matches!(iterable.kind, ExprKind::Array(_)));
                assert_eq!(body.len(), 1);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn for_requires_let_and_of() {
        assert!(parse_block("for (x of [1]) { y = x; }").is_err());
        assert!(parse_block("for (let x in [1]) { y = x; }").is_err());
    }

    #[test]
    fn a_field_actually_named_of_still_parses_outside_a_for_loop() {
        let s = parse_block("y = of").unwrap();
        match &s[0] {
            Stmt::Assign { target, value, .. } => {
                assert_eq!(target, "y");
                assert_eq!(sexp(value), "of");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn nested_if_inside_a_for_loop_parses() {
        let s = parse_block("for (let x of [1, 2]) { if (x > 1) { y = x; } else { y = 0; } }").unwrap();
        match &s[0] {
            Stmt::For { body, .. } => {
                assert_eq!(body.len(), 1);
                assert!(matches!(body[0], Stmt::If { .. }));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn an_unterminated_block_is_a_parse_error() {
        assert!(parse_block("if (a) { x = 1;").is_err());
    }

    // -- method-call syntax ---------------------------------------------------

    #[test]
    fn method_call_on_an_array_literal_desugars_to_a_call_with_the_receiver_first() {
        assert_eq!(p(r#"["x"].length()"#), r#"length(["x"])"#);
    }

    #[test]
    fn method_call_on_a_string_literal() {
        assert_eq!(p(r#""hi".trim()"#), r#"trim("hi")"#);
    }

    #[test]
    fn method_call_with_extra_arguments_after_the_receiver() {
        assert_eq!(p(r#""hi".slice(1, 2)"#), r#"slice("hi", 1, 2)"#);
    }

    #[test]
    fn method_call_on_a_parenthesized_expression() {
        assert_eq!(p("(a + b).toString()"), "toString((a + b))");
    }

    #[test]
    fn method_call_on_an_index_result() {
        assert_eq!(p("a[0].trim()"), "trim(a[0])");
    }

    #[test]
    fn method_call_on_a_call_result() {
        assert_eq!(p("f(x).trim()"), "trim(f(x))");
    }

    #[test]
    fn chained_method_calls_nest_left_to_right() {
        assert_eq!(p(r#""hi".trim().toUpperCase()"#), r#"toUpperCase(trim("hi"))"#);
    }

    #[test]
    fn method_call_binds_tighter_than_any_operator() {
        assert_eq!(p(r#""hi".length() + 1"#), r#"(length("hi") + 1)"#);
    }

    #[test]
    fn a_bare_identifier_chain_stays_one_name_through_the_parser() {
        assert_eq!(p("x.length()"), "x.length()");
    }

    #[test]
    fn method_name_must_follow_the_dot() {
        assert!(parse_expr(r#""hi".()"#).is_err());
    }

    /// Dot without parens is member access, the same shape as the bracket form; the type checker rejects it on a `Str`.
    #[test]
    fn a_method_call_missing_its_parens_parses_as_member_access() {
        for src in [r#""hi".trim"#, r#""hi"["trim"]"#] {
            let e = parse_expr(src).unwrap_or_else(|e| panic!("{src}: {}", e.message));
            let ExprKind::Index { base, index } = &e.kind else {
                panic!("{src} should be member access, got {:?}", e.kind);
            };
            assert!(matches!(&base.kind, ExprKind::Literal(Value::Str(s)) if s == "hi"));
            assert!(matches!(&index.kind, ExprKind::Literal(Value::Str(s)) if s == "trim"));
        }
    }

    /// An arrow's body is one expression, so it ends at the `,` or `)` after it; `(x) =>` is the same arrow.
    #[test]
    fn an_arrow_binds_a_parameter_over_one_expression() {
        assert_eq!(p("xs.map(x => x * 2)"), "xs.map((x => (x * 2)))");
        assert_eq!(p("xs.map((x) => x * 2)"), "xs.map((x => (x * 2)))");
        assert_eq!(p("map(xs, x => x > 1 ? x : 0)"), "map(xs, (x => ((x > 1) ? x : 0)))");
        assert_eq!(
            p("xs.filter(x => x > 1).map(x => x.n)"),
            "map(xs.filter((x => (x > 1))), (x => x.n))"
        );
        assert!(parse_expr("xs.map(a.b => 1)").is_err(), "a dotted parameter");
    }

    /// Nesting past `MAX_DEPTH` is a `ParseError`, not a stack overflow, whichever construct nests: prefix forms,
    /// right-associative tails, postfix chains (which do not recurse in the parser), a mix of both, and statement
    /// blocks.
    #[test]
    fn deep_nesting_is_rejected_not_fatal() {
        let n = 20_000;
        let exprs = [
            format!("{}1{}", "(".repeat(n), ")".repeat(n)),
            format!("{}1", "- ".repeat(n)),
            format!("{}true", "!".repeat(n)),
            format!("{}1{}", "[".repeat(n), "]".repeat(n)),
            format!("{}1{}", "{\"a\":".repeat(n), "}".repeat(n)),
            (0..n).map(|_| "2**").collect::<String>() + "2",
            (0..n).map(|_| "a?b:").collect::<String>() + "c",
            format!("a{}", "[0]".repeat(n)),
            format!("a{}", ".m()".repeat(n)),
            (0..n).map(|_| "x => ").collect::<String>() + "1",
            format!("{}a{}{}", "-(".repeat(n), "[0]".repeat(n), ")".repeat(n)),
            format!("1{}", " + 1".repeat(n)),
            format!("a{}", " || b".repeat(n)),
            // A template hole is parsed by a parser of its own, which starts at the template's depth.
            format!("{}1{}", "`${".repeat(n), "}`".repeat(n)),
        ];
        for src in &exprs {
            let err = parse_expr(src).expect_err("should be rejected");
            assert!(err.message.contains("nests more than"), "{:?}", err.message);
        }
        for src in [
            format!("{}x=1;{}", "if (true) {".repeat(n), "}".repeat(n)),
            format!("{}x=1;{}", "for (let i of a) {".repeat(n), "}".repeat(n)),
        ] {
            let err = parse_block(&src).expect_err("should be rejected");
            assert!(err.message.contains("nests more than"), "{:?}", err.message);
        }
    }

    #[test]
    fn a_left_associative_chain_folds_left_and_is_bounded() {
        let at = format!("1{}", " + 1".repeat(MAX_DEPTH as usize - 1));
        let e = parse_expr(&at).expect("a chain at the bound should parse");

        // Left-nested: every fold put the new operand on the right.
        let mut node = &e;
        let mut folds = 0;
        while let ExprKind::Binary { lhs, rhs, .. } = &node.kind {
            assert!(matches!(&rhs.kind, ExprKind::Literal(Value::I64(1))));
            node = lhs;
            folds += 1;
        }
        assert_eq!(folds, MAX_DEPTH - 1);

        let over = format!("1{}", " + 1".repeat(MAX_DEPTH as usize + 1));
        assert!(parse_expr(&over).is_err());
    }

    /// Exactly at the bound parses; one past it does not.
    #[test]
    fn the_bound_is_off_by_none() {
        let at = format!(
            "{}1{}",
            "(".repeat(MAX_DEPTH as usize - 1),
            ")".repeat(MAX_DEPTH as usize - 1)
        );
        assert!(parse_expr(&at).is_ok(), "{} levels should parse", MAX_DEPTH - 1);
        let over = format!(
            "{}1{}",
            "(".repeat(MAX_DEPTH as usize + 1),
            ")".repeat(MAX_DEPTH as usize + 1)
        );
        assert!(
            parse_expr(&over).is_err(),
            "{} levels should be rejected",
            MAX_DEPTH + 1
        );
    }

    /// `name: type;` declares, `name: type = expr;` declares and coerces, and a local, a parameter and a return take
    /// an exception. The type grammar is the engine's names, with `array<T>` and `{name: T, ...}`.
    #[test]
    fn exceptions_parse() {
        let s =
            parse_block("status: i64; source: {ip: str, port: i64}; tags: array<str> = split(raw, \",\");").unwrap();
        assert!(matches!(&s[0], Stmt::Declare { name, ty } if name == "status" && *ty == DataType::I64));
        assert!(matches!(
            &s[1],
            Stmt::Declare { name, ty }
                if name == "source"
                    && *ty == DataType::Struct(vec![("ip".into(), DataType::Str), ("port".into(), DataType::I64)], false)
        ));
        match &s[2] {
            Stmt::Assign { target, value, .. } => {
                assert_eq!(target, "tags");
                assert_eq!(sexp(value), "(split(raw, \",\") is array(str))");
            }
            other => panic!("{other:?}"),
        }

        let s = parse_block("let n: f64 = 0; let m = 1; $[\"@ts\"]: datetime;").unwrap();
        assert!(matches!(&s[0], Stmt::Let { name, value } if name == "n" && sexp(value) == "(0 is f64)"));
        assert!(matches!(&s[1], Stmt::Let { name, value } if name == "m" && sexp(value) == "1"));
        assert!(matches!(&s[2], Stmt::Declare { name, ty } if name == "@ts" && *ty == DataType::DateTime));

        let s = parse_block("function host(u: str, n: i64): str { return u; }").unwrap();
        match &s[0] {
            Stmt::Function { function, .. } => {
                assert_eq!(function.params, ["u", "n"]);
                assert_eq!(function.param_types, [Some(DataType::Str), Some(DataType::I64)]);
                assert_eq!(function.ret, Some(DataType::Str));
            }
            other => panic!("{other:?}"),
        }

        for src in ["x: int;", "x: array<unknown>;", "x: {a};", "x: array;"] {
            assert!(parse_block(src).is_err(), "{src} should be refused");
        }

        // An object type is open where it ends in `...`.
        let open = |src: &str| match &parse_block(src).unwrap()[0] {
            Stmt::Declare { ty, .. } => ty.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            open("x: {a: i64, ...};"),
            DataType::Struct(vec![("a".into(), DataType::I64)], true)
        );
        assert_eq!(open("x: {...};"), DataType::Struct(vec![], true));
        assert_eq!(open("x: {a: i64, ...,};").spell(), "{a: i64, ...}");
        for src in ["x: {..., a: str};", "x: {...: str};"] {
            assert!(parse_block(src).is_err(), "{src} should be refused");
        }
        // A `>` the lexer glued to the next token still closes a type argument.
        let nested = |depth: usize| {
            let mut t = DataType::I64;
            for _ in 0..depth {
                t = DataType::Array(Box::new(t));
            }
            t
        };
        for (src, depth) in [
            ("x: array<array<i64>>;", 2),
            ("x: array<array<array<i64>>>;", 3),
            ("x: array<i64>= [1];", 1),
            ("x: array<array<i64>>= [[1]];", 2),
        ] {
            let s = parse_block(src).unwrap_or_else(|e| panic!("{src}: {}", e.message));
            let ty = match &s[0] {
                Stmt::Declare { ty, .. } => ty.clone(),
                Stmt::Assign { value, .. } => match &value.kind {
                    ExprKind::Check { ty, .. } => ty.clone(),
                    other => panic!("{src}: {other:?}"),
                },
                other => panic!("{src}: {other:?}"),
            };
            assert_eq!(ty, nested(depth), "{src}");
        }
        // A type deeper than the parser's bound is an error, not a stack overflow.
        let deep = format!(
            "x: {}i64{};",
            "array<".repeat(MAX_DEPTH as usize + 1),
            "> ".repeat(MAX_DEPTH as usize + 1)
        );
        let e = parse_block(&deep).unwrap_err();
        assert!(e.message.contains("a type nests more than"), "{}", e.message);
        // `variant` is a type a declaration names.
        let s = parse_block("o: variant; let x: variant = 1;").unwrap();
        assert!(matches!(&s[0], Stmt::Declare { name, ty } if name == "o" && *ty == DataType::Variant));
        assert!(matches!(&s[1], Stmt::Let { name, value } if name == "x" && sexp(value) == "(1 is variant)"));
        // A ternary's colon is the ternary's.
        assert_eq!(p("a ? b : c"), "(a ? b : c)");
    }
}
