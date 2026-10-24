//! The formatter: lays pipeline code out in one style, changing only the space between its tokens and the `;` that
//! ends a statement. The text's tokens and comments are kept as written. A layout whose tree differs from the text's
//! own is never returned, so formatting cannot change what code does.
//!
//! The style is the prelude's: two spaces an indent, a block's `{` on its statement's line and its `}` on a line of
//! its own, one space around a binary operator, after `,` and `:` and after a keyword, none inside brackets. An
//! object literal and a type stay on one line. Each statement and each `case` starts a line, the writer's other line
//! breaks stay, a run of blank lines becomes one, and a statement ends in `;` except after a block's `}` or where it
//! may be a function's value.

use std::collections::HashMap;

use crate::expr::ast::Span;
use crate::expr::lex::{Keyword, Lexer, Punct, Token};
use crate::expr::parse::{self, Shape};

/// What a text is: one expression (a condition, an assignment's value), or a statement list (a `Code` body).
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Kind {
    Expr,
    Block,
}

/// `src` laid out in the style, or `None` where it does not parse. Formatting formatted text gives it back unchanged.
pub fn format(src: &str, kind: Kind) -> Option<String> {
    let (tree, shape) = parsed(src, kind)?;
    let out = layout(src, &shape)?;
    // The layout keeps the tokens, so this holds unless the layout itself has a bug; the text is left as written then.
    let (again, _) = parsed(&out, kind)?;
    (again == tree).then_some(out)
}

/// The text's tree, positions left out, so that two layouts of one program compare equal.
fn parsed(src: &str, kind: Kind) -> Option<(String, Shape)> {
    let (tree, shape) = match kind {
        Kind::Expr => parse::parse_expr_shaped(src).map(|(e, shape)| (format!("{e:?}"), shape)),
        Kind::Block => parse::parse_block_shaped(src).map(|(b, shape)| (format!("{b:?}"), shape)),
    }
    .ok()?;
    static SPANS: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"Span \{ start: \d+, end: \d+ \}").expect("a valid pattern"));
    let spans = &*SPANS;
    Some((spans.replace_all(&tree, "Span").into_owned(), shape))
}

/// One token of the text, as the formatter places it.
struct Piece<'a> {
    token: Token,
    text: &'a str,
    span: Span,
    /// The whitespace and comments between the piece before and this one.
    gap: &'a str,
}

/// The text's tokens, a glued token the parser split for a type (`array<str>=`) split the same way.
fn pieces<'a>(src: &'a str, shape: &Shape) -> Option<Vec<Piece<'a>>> {
    let mut cuts: Vec<u32> = shape.types.iter().map(|t| t.end).collect();
    cuts.sort_unstable();
    let mut out = Vec::new();
    let mut end = 0;
    for t in Lexer::tokenize(src).ok()? {
        let mut start = t.span.start;
        let inside = cuts.iter().copied().filter(|&c| c > start && c < t.span.end);
        let bounds: Vec<u32> = inside.chain([t.span.end]).collect();
        for (i, &stop) in bounds.iter().enumerate() {
            let span = Span { start, end: stop };
            let text = span.slice(src);
            let token = if bounds.len() == 1 {
                t.token.clone()
            } else {
                // Each part of a split token is a punctuation token of its own.
                Lexer::tokenize(text).ok()?.into_iter().next()?.token
            };
            let gap = if i == 0 { &src[end as usize..start as usize] } else { "" };
            out.push(Piece { token, text, span, gap });
            start = stop;
        }
        end = t.span.end;
    }
    Some(out)
}

/// A comment, and how many line breaks came before it.
struct Comment<'a> {
    text: &'a str,
    breaks: usize,
}

/// The comments in a gap, and the line breaks after the last of them. `None` where the gap holds anything else.
fn comments(gap: &str) -> Option<(Vec<Comment<'_>>, usize)> {
    let mut out = Vec::new();
    let mut breaks = 0;
    let mut rest = gap;
    while let Some(c) = rest.chars().next() {
        if c == '\n' {
            breaks += 1;
            rest = &rest[1..];
        } else if c.is_ascii_whitespace() {
            rest = &rest[1..];
        } else if rest.starts_with("//") {
            let len = rest.find('\n').unwrap_or(rest.len());
            out.push(Comment {
                text: rest[..len].trim_end(),
                breaks,
            });
            breaks = 0;
            rest = &rest[len..];
        } else if rest.starts_with("/*") {
            let len = rest[2..].find("*/").map_or(rest.len(), |i| i + 4);
            out.push(Comment {
                text: &rest[..len],
                breaks,
            });
            breaks = 0;
            rest = &rest[len..];
        } else {
            return None;
        }
    }
    Some((out, breaks))
}

/// An open bracket.
struct Frame {
    block: bool,
    /// The indent of the line it opened on: its own closer's line takes it, and the lines inside take one more.
    indent: usize,
    /// A `switch` body past its first label: the lines inside a case take one more again.
    in_case: bool,
}

struct Writer {
    out: String,
    frames: Vec<Frame>,
    /// The indent of the line being written.
    indent: usize,
}

impl Writer {
    /// Ends the line, with a blank line after it where `breaks` asks for one, and indents the next `indent` deep.
    fn newline(&mut self, breaks: usize, indent: usize) {
        while self.out.ends_with(' ') {
            self.out.pop();
        }
        for _ in 0..breaks.clamp(1, 2) {
            self.out.push('\n');
        }
        self.indent = indent;
        self.out.push_str(&"  ".repeat(indent));
    }
}

fn layout(src: &str, shape: &Shape) -> Option<String> {
    let pieces = pieces(src, shape)?;
    let opens: HashMap<u32, u32> = shape.blocks.iter().copied().collect();
    let closes: HashMap<u32, u32> = shape.blocks.iter().map(|&(o, c)| (c, o)).collect();
    let mut ends: HashMap<u32, bool> = HashMap::new();
    for &(at, semi) in &shape.statements {
        *ends.entry(at).or_insert(true) &= semi;
    }
    let in_type = |s: Span| shape.types.iter().any(|t| t.start <= s.start && s.end <= t.end);
    let ternary = |s: Span| shape.ternary_colons.contains(&s.start);
    let is = |p: &Piece, t: Punct| p.token == Token::Punct(t);
    let block_open = |p: &Piece| is(p, Punct::LBrace) && opens.contains_key(&p.span.start);
    let block_close = |p: &Piece| is(p, Punct::RBrace) && closes.contains_key(&p.span.start);
    let label = |p: &Piece| shape.case_labels.contains(&p.span.start);
    let closer = |p: &Piece| matches!(p.token, Token::Punct(Punct::RParen | Punct::RBracket | Punct::RBrace));
    // What may follow a block's `}` on its line: `} else {`, `xs.map(x => {...})`.
    let after_block = |t: &Piece| {
        matches!(
            &t.token,
            Token::Keyword(Keyword::Else)
                | Token::Punct(Punct::RParen | Punct::Comma | Punct::Semi | Punct::RBracket | Punct::Dot)
        ) || matches!(&t.token, Token::Ident(w) if w == "catch" || w == "finally")
    };

    let mut w = Writer {
        out: String::new(),
        frames: Vec::new(),
        indent: 0,
    };
    // The last token written, whether it ends an expression, and whether the token before it does.
    let mut prev: Option<(usize, bool, bool)> = None;
    for (i, piece) in pieces.iter().enumerate() {
        let eof = piece.token == Token::Eof;
        let (comments, mut breaks) = comments(piece.gap)?;
        let p = prev.map(|(j, _, _)| &pieces[j]);
        let prev_ends = prev.is_some_and(|(_, e, _)| e);
        let before_prev_ends = prev.is_some_and(|(_, _, e)| e);

        // A doubled `;` is one.
        if is(piece, Punct::Semi) && p.is_some_and(|p| is(p, Punct::Semi)) && comments.is_empty() {
            continue;
        }

        // How deep a new line here is indented: past the innermost open bracket, past a case's label, and once more
        // inside a statement that has not ended.
        let indent_at = |w: &Writer, t: Option<&Piece>| -> usize {
            if let Some(t) = t
                && closer(t)
            {
                return w.frames.last().map_or(0, |f| f.indent);
            }
            let Some(inner) = w.frames.last() else {
                return usize::from(continued(p, &ends, &is, &block_open, &block_close));
            };
            let case = inner.in_case && !t.is_some_and(label);
            let continues = inner.block && continued(p, &ends, &is, &block_open, &block_close);
            inner.indent + 1 + usize::from(case) + usize::from(continues)
        };

        let mut forced = 0;
        let mut joined = false;
        // A statement starts a line, which a comment on a line of its own before it has started already.
        let mut starts = false;
        if let Some(p) = p {
            if block_open(p) {
                joined =
                    comments.is_empty() && block_close(piece) && closes.get(&piece.span.start) == Some(&p.span.start);
                forced = usize::from(!joined);
            }
            if (block_close(piece) && !joined) || label(piece) {
                forced = 1;
            }
            // Each statement starts a line; a braceless branch's `else` stays on its `if`'s.
            let ended = is(p, Punct::Semi) || ends.contains_key(&p.span.end);
            starts = ended && !is(piece, Punct::Semi) && piece.token != Token::Keyword(Keyword::Else);
            if block_close(p) {
                if after_block(piece) {
                    joined = comments.is_empty();
                } else {
                    forced = 1;
                }
            }
        }
        if joined || eof {
            breaks = 0;
            forced = 0;
            starts = false;
        }

        // A `//` comment ends its line.
        let mut line_comment = false;
        for c in &comments {
            if w.out.is_empty() {
                // The comment opens the text.
            } else if c.breaks > 0 || line_comment {
                w.newline(c.breaks, indent_at(&w, None));
                starts = false;
            } else {
                w.out.push(' ');
            }
            w.out.push_str(c.text);
            line_comment = c.text.starts_with("//");
        }
        if line_comment {
            breaks = breaks.max(1);
        }
        if eof {
            break;
        }
        let lines = breaks.max(forced).max(usize::from(starts));
        if lines > 0 && !w.out.is_empty() {
            w.newline(lines, indent_at(&w, Some(piece)));
        } else if let Some(p) = p
            && (!comments.is_empty() || spaced(p, piece, [prev_ends, before_prev_ends], &in_type, &ternary))
        {
            w.out.push(' ');
        }
        w.out.push_str(piece.text);

        match &piece.token {
            Token::Punct(Punct::LParen | Punct::LBracket | Punct::LBrace) => w.frames.push(Frame {
                block: block_open(piece),
                indent: w.indent,
                in_case: false,
            }),
            Token::Punct(Punct::RParen | Punct::RBracket | Punct::RBrace) => {
                w.frames.pop();
            }
            _ if label(piece) => {
                if let Some(f) = w.frames.last_mut() {
                    f.in_case = true;
                }
            }
            _ => {}
        }
        prev = Some((i, ends_expr(&piece.token, prev_ends) && !block_close(piece), prev_ends));

        // The `;` a statement ends in, where the writer left it out.
        let next_is_semi = pieces.get(i + 1).is_some_and(|n| is(n, Punct::Semi));
        if ends.get(&piece.span.end) == Some(&true) && !next_is_semi && !is(piece, Punct::Semi) {
            w.out.push(';');
        }
    }
    Some(w.out.trim_end().to_string())
}

/// Whether a line after `p` continues a statement that has not ended.
fn continued(
    p: Option<&Piece>,
    ends: &HashMap<u32, bool>,
    is: &impl Fn(&Piece, Punct) -> bool,
    block_open: &impl Fn(&Piece) -> bool,
    block_close: &impl Fn(&Piece) -> bool,
) -> bool {
    p.is_some_and(|p| !ends.contains_key(&p.span.end) && !is(p, Punct::Semi) && !block_open(p) && !block_close(p))
}

/// Whether `t` can end an expression, `prev_ends` saying whether the token before it can: the lexer's rule for `/`.
fn ends_expr(t: &Token, prev_ends: bool) -> bool {
    match t {
        Token::Ident(_)
        | Token::Int(_)
        | Token::Float(_)
        | Token::Str(_)
        | Token::Regex { .. }
        | Token::Template(_) => true,
        Token::Keyword(k) => matches!(k, Keyword::True | Keyword::False | Keyword::Null),
        Token::Punct(Punct::RParen | Punct::RBracket | Punct::RBrace) => true,
        Token::Punct(Punct::PlusPlus | Punct::MinusMinus) => prev_ends,
        _ => false,
    }
}

/// Whether one space goes between `p` and `t` on a line. `p_ends` and `before_p_ends` say whether `p`, and the token
/// before it, end an expression.
fn spaced(
    p: &Piece,
    t: &Piece,
    [p_ends, before_p_ends]: [bool; 2],
    in_type: &impl Fn(Span) -> bool,
    ternary: &impl Fn(Span) -> bool,
) -> bool {
    use Punct::*;
    let punct = |x: &Piece| match x.token {
        Token::Punct(p) => Some(p),
        _ => None,
    };
    let (pp, tp) = (punct(p), punct(t));
    // A binary operator; `+` and `-` are unary where nothing that ends an expression comes before them.
    let binary = |x: Option<Punct>, before_ends: bool| match x {
        Some(Plus | Minus) => before_ends,
        Some(Colon) => false,
        Some(
            Star | Slash | Percent | StarStar | EqEq | BangEq | Lt | Le | Gt | Ge | AmpAmp | PipePipe | Amp | Pipe
            | Caret | LtLt | GtGt | QuestionQuestion | Question | Assign | AssignOp(_) | FatArrow,
        ) => true,
        _ => false,
    };

    // A type is written close: `array<str>`, `{a: str, ...}`.
    if in_type(p.span) && in_type(t.span) {
        return matches!(pp, Some(Comma | Colon));
    }
    if matches!(tp, Some(Colon)) {
        return ternary(t.span);
    }
    if matches!(pp, Some(Colon)) {
        return true;
    }
    // The operator's own side: `t` an operator, `p` what came before it.
    if binary(tp, p_ends) {
        return true;
    }
    if binary(pp, before_p_ends) {
        return true;
    }
    if matches!(tp, Some(Comma | Semi | RParen | RBracket)) || matches!(pp, Some(LParen | LBracket | Ellipsis)) {
        return false;
    }
    if matches!(pp, Some(Comma | Semi)) {
        return true;
    }
    if matches!(tp, Some(Dot)) || matches!(pp, Some(Dot)) {
        // `x .y` and `x. y` stay apart: closed up they would lex as the one name `x.y`.
        let names = matches!(p.token, Token::Ident(_)) || matches!(t.token, Token::Ident(_));
        return names && !t.gap.is_empty();
    }
    if matches!(pp, Some(Bang | Tilde)) {
        return false;
    }
    if matches!(tp, Some(PlusPlus | MinusMinus)) && p_ends {
        return false;
    }
    if matches!(pp, Some(PlusPlus | MinusMinus)) && !matches!(t.token, Token::Punct(_)) {
        // A prefix `++x`; a postfix `x++` is followed by an operator or a closer.
        return false;
    }
    match tp {
        // A call or an index, after a name or a closer; a keyword's `(` is spaced: `if (`.
        Some(LParen | LBracket) => match &p.token {
            Token::Keyword(k) => *k != Keyword::Function,
            Token::Ident(w) if tp == Some(LParen) && (w == "switch" || w == "catch") => true,
            _ => !p_ends && !matches!(pp, Some(Plus | Minus)),
        },
        // An object literal's braces hold their entries close: `{a: 1}`. A block's are on lines of their own.
        Some(RBrace) => false,
        // A unary sign holds its operand close, but not a second sign: `- -a`, never `--a`.
        _ => {
            !matches!(pp, Some(LBrace) | Some(Plus | Minus)) || matches!(tp, Some(Plus | Minus | PlusPlus | MinusMinus))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(src: &str) -> String {
        format(src, Kind::Block).unwrap_or_else(|| panic!("formats: {src}"))
    }

    fn expr(src: &str) -> String {
        format(src, Kind::Expr).unwrap_or_else(|| panic!("formats: {src}"))
    }

    #[test]
    fn the_prelude_is_in_the_style() {
        let prelude = include_str!("prelude.smeltr").trim_end();
        assert_eq!(block(prelude), prelude);
    }

    #[test]
    fn blocks_break_and_indent() {
        assert_eq!(
            block(r#"if(status>=500){level="error"}else{level="info"}"#),
            "if (status >= 500) {\n  level = \"error\";\n} else {\n  level = \"info\";\n}"
        );
        assert_eq!(block("if (x) {}"), "if (x) {}");
        assert_eq!(
            block(r#"switch (c) { case "a": x = 1; break; default: x = 0; }"#),
            "switch (c) {\n  case \"a\":\n    x = 1;\n    break;\n  default:\n    x = 0;\n}"
        );
        assert_eq!(
            block("for (let x of xs) { if (x > 0) { n += 1 } }"),
            "for (let x of xs) {\n  if (x > 0) {\n    n += 1;\n  }\n}"
        );
    }

    #[test]
    fn statements_end_in_one_semicolon() {
        assert_eq!(block("a = 1 b = 2"), "a = 1;\nb = 2;");
        assert_eq!(block("a = 1;;\nb = 2"), "a = 1;\nb = 2;");
        assert_eq!(block("if (x) drop()"), "if (x) drop();");
        assert_eq!(block("if (x) a = 1 else a = 2"), "if (x) a = 1; else a = 2;");
        assert_eq!(block("ts: datetime; n: i64;"), "ts: datetime;\nn: i64;");
        assert_eq!(block("a = 1; /* c */ b = 2"), "a = 1; /* c */\nb = 2;");
    }

    #[test]
    fn a_function_value_takes_no_semicolon() {
        assert_eq!(
            block("function f(x) { x + 1 }\ny = f(1)"),
            "function f(x) {\n  x + 1\n}\ny = f(1);"
        );
        assert_eq!(
            block("function f(x) { if (x) { 1 } else { 2 } }"),
            "function f(x) {\n  if (x) {\n    1\n  } else {\n    2\n  }\n}"
        );
        assert_eq!(block("ys = xs.map(x => { x * 2 })"), "ys = xs.map(x => {\n  x * 2\n});");
    }

    #[test]
    fn comments_stay_where_they_were() {
        assert_eq!(
            block("// top\na=1 // why\n\n\n/* b */ b=2"),
            "// top\na = 1; // why\n\n/* b */ b = 2;"
        );
        assert_eq!(block("if (x) {\n// inside\n}"), "if (x) {\n  // inside\n}");
    }

    #[test]
    fn expressions_space_their_operators() {
        assert_eq!(expr("a&&!b||c==-1"), "a && !b || c == -1");
        assert_eq!(expr("x?y:z"), "x ? y : z");
        assert_eq!(expr("f( a ,b )[ 0 ]"), "f(a, b)[0]");
        assert_eq!(expr("{a:1,...b}"), "{a: 1, ...b}");
        assert_eq!(expr("[ 1,2 ]"), "[1, 2]");
        assert_eq!(expr("$[\"@timestamp\"] ?? 0"), "$[\"@timestamp\"] ?? 0");
        assert_eq!(expr("a - -b"), "a - -b");
        assert_eq!(expr("- -a + -(b) - -[1][0]"), "- -a + -(b) - -[1][0]");
        assert_eq!(expr("x/2 + /a b/.test(y)"), "x / 2 + /a b/.test(y)");
    }

    #[test]
    fn types_are_written_close() {
        assert_eq!(expr("x as array < str >"), "x as array<str>");
        assert_eq!(block("let x : { a : str , ... } = y"), "let x: {a: str, ...} = y;");
        assert_eq!(block("x: array<array<i64>>= y"), "x: array<array<i64>> = y;");
    }

    #[test]
    fn continued_lines_indent_once() {
        assert_eq!(
            block("x = a &&\nb\ny = f(\n1,\n2\n)"),
            "x = a &&\n  b;\ny = f(\n  1,\n  2\n);"
        );
        assert_eq!(
            block("switch (x) {\ncase 1:\na = 1\nbreak\ndefault:\na = 2\n}"),
            "switch (x) {\n  case 1:\n    a = 1;\n    break;\n  default:\n    a = 2;\n}"
        );
    }

    #[test]
    fn text_that_does_not_parse_is_left_alone() {
        assert_eq!(format("a = (", Kind::Block), None);
        assert_eq!(format("a = 1", Kind::Expr), None);
    }

    #[test]
    fn formatting_twice_changes_nothing() {
        for src in [
            "if(a){b=1}else if(c){d=2}else{e=3}",
            "let t = `a ${x+1} b`\ntry { x = 1 } catch (e) { y = 2 } finally { z = 3 }",
            "x = {a: [1, 2], b: f(x => x * 2)}",
        ] {
            let once = block(src);
            assert_eq!(block(&once), once);
        }
    }

    /// Every pipeline the docs show that parses formats, and formatting it again changes nothing.
    #[test]
    fn the_docs_examples_format() {
        let docs = [
            include_str!("../../docs/examples.md"),
            include_str!("../../docs/architecture/language.md"),
        ];
        for doc in docs {
            for fenced in doc.split("```ts\n").skip(1) {
                let src = fenced.split("\n```").next().expect("a fence's text");
                if parse::parse_block(src).is_err() {
                    continue;
                }
                let once = block(src);
                assert_eq!(block(&once), once);
            }
        }
    }
}
