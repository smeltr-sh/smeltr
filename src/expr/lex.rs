//! The lexer.
//!
//! `/` is division after a token that can end an expression (identifier, literal, `)`, `]`, `}`) and opens a regex
//! otherwise, as in JS. A dotted identifier chain (`source.ip.addr`) is one `Ident` token: a flat column name, not
//! member access.

use crate::expr::ast::{RegexFlags, Span};
use crate::ops::BinOp;

#[derive(Clone, PartialEq, Debug)]
pub enum Token {
    /// A bare or dotted name: `x`, `source.ip`
    Ident(String),
    Int(i64),
    Float(f64),
    /// String literal, already unescaped.
    Str(String),
    Regex {
        source: String,
        flags: RegexFlags,
    },
    Keyword(Keyword),
    Punct(Punct),
    /// `` `a ${x} b` ``: literal text and `${...}` holes. A hole is kept as source and parsed through the ordinary
    /// entry point.
    Template(Vec<TemplatePart>),
    Eof,
}

#[derive(Clone, PartialEq, Debug)]
pub enum TemplatePart {
    /// Literal text between holes, escapes already resolved.
    Lit(String),
    /// One `${...}`; `offset` is where `src` begins in the original input.
    Hole { src: String, offset: usize },
}

/// Every reserved word and the keyword it lexes as.
pub const KEYWORDS: [(&str, Keyword); 13] = [
    ("true", Keyword::True),
    ("false", Keyword::False),
    ("null", Keyword::Null),
    ("let", Keyword::Let),
    ("if", Keyword::If),
    ("else", Keyword::Else),
    ("for", Keyword::For),
    ("return", Keyword::Return),
    ("function", Keyword::Function),
    ("while", Keyword::While),
    ("break", Keyword::Break),
    ("continue", Keyword::Continue),
    ("as", Keyword::As),
];

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Keyword {
    True,
    False,
    Null,
    Let,
    If,
    Else,
    For,
    Return,
    Function,
    While,
    Break,
    Continue,
    /// `e as type`: the postfix coercion.
    As,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Punct {
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    StarStar,
    EqEq,
    BangEq,
    Lt,
    Le,
    Gt,
    Ge,
    AmpAmp,
    PipePipe,
    Bang,
    Amp,
    Pipe,
    Caret,
    Tilde,
    LtLt,
    GtGt,
    QuestionQuestion,
    Question,
    Colon,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Dot,
    Assign,
    Semi,
    /// `x op= v`, carrying the operator it compounds.
    AssignOp(BinOp),
    PlusPlus,
    MinusMinus,
    /// `=>`, between an arrow's parameter and its body.
    FatArrow,
    /// `...`, before a spread object literal entry.
    Ellipsis,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Spanned {
    pub token: Token,
    pub span: Span,
}

#[derive(Clone, PartialEq, Debug)]
pub struct LexError {
    pub span: Span,
    pub message: String,
}

/// The ASCII bytes a token can begin or end with, which is where a would-be name stops when the lexer names it in an
/// error. Everything else, a non-ASCII byte included, is taken as part of the name.
const NAME_BREAKERS: &[u8] = b"+-*/%<>=!&|?:,;()[]{}\"'`";

pub struct Lexer<'a> {
    src: &'a [u8],
    text: &'a str,
    pos: usize,
    /// Whether the previous token could end an expression; decides `/`.
    prev_ends_expr: bool,
}

impl<'a> Lexer<'a> {
    pub fn new(text: &'a str) -> Lexer<'a> {
        Lexer {
            src: text.as_bytes(),
            text,
            pos: 0,
            prev_ends_expr: false,
        }
    }

    pub fn tokenize(text: &'a str) -> Result<Vec<Spanned>, LexError> {
        let mut lx = Lexer::new(text);
        let mut out = Vec::new();
        loop {
            let t = lx.next_token()?;
            let eof = t.token == Token::Eof;
            out.push(t);
            if eof {
                return Ok(out);
            }
        }
    }

    /// A character no token begins with. Most often it is part of a field name the bare form cannot spell --
    /// `@timestamp`, `naïve` -- so the message shows the name the run around it makes and the `$["..."]` that reads it.
    fn unexpected_character(&mut self, start: usize) -> LexError {
        let c = self.text[start..].chars().next().expect("a character at the error");
        self.pos = start + c.len_utf8();
        let is_word = |b: u8| !(b.is_ascii_whitespace() || NAME_BREAKERS.contains(&b));
        let mut lo = start;
        while lo > 0 && is_word(self.src[lo - 1]) {
            lo -= 1;
        }
        let mut hi = self.pos;
        while hi < self.src.len() && is_word(self.src[hi]) {
            hi += 1;
        }
        let name = &self.text[lo..hi];
        // A `"` is a breaker and never lands in the run; a `\` does, and has to be escaped to sit in the string.
        let quoted = name.replace('\\', "\\\\");
        self.err(
            start,
            format!(
                "unexpected character '{c}' in `{name}` -- a name with characters outside letters, digits, `_` and `$` \
                 is written `$[\"{quoted}\"]`"
            ),
        )
    }

    fn err(&self, start: usize, msg: impl Into<String>) -> LexError {
        LexError {
            span: Span::new(start, self.pos),
            message: msg.into(),
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn peek_at(&self, n: usize) -> Option<u8> {
        self.src.get(self.pos + n).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn skip_trivia(&mut self) {
        loop {
            while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
                self.pos += 1;
            }
            // Line comment
            if self.peek() == Some(b'/') && self.peek_at(1) == Some(b'/') {
                while self.peek().is_some_and(|c| c != b'\n') {
                    self.pos += 1;
                }
                continue;
            }
            // Block comment
            if self.peek() == Some(b'/') && self.peek_at(1) == Some(b'*') {
                self.pos += 2;
                while self.pos < self.src.len() {
                    if self.peek() == Some(b'*') && self.peek_at(1) == Some(b'/') {
                        self.pos += 2;
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }
            return;
        }
    }

    pub fn next_token(&mut self) -> Result<Spanned, LexError> {
        self.skip_trivia();
        let start = self.pos;

        let Some(c) = self.peek() else {
            return Ok(self.emit(Token::Eof, start, false));
        };

        // Identifiers and keywords
        if c.is_ascii_alphabetic() || c == b'_' || c == b'$' {
            return self.lex_ident(start);
        }
        if c.is_ascii_digit() {
            return self.lex_number(start);
        }
        if c == b'"' || c == b'\'' {
            return self.lex_string(start, c);
        }
        if c == b'`' {
            return self.lex_template(start);
        }
        if c == b'/' && !self.prev_ends_expr {
            return self.lex_regex(start);
        }

        self.lex_punct(start)
    }

    fn emit(&mut self, token: Token, start: usize, ends_expr: bool) -> Spanned {
        self.prev_ends_expr = ends_expr;
        Spanned {
            token,
            span: Span::new(start, self.pos),
        }
    }

    fn lex_ident(&mut self, start: usize) -> Result<Spanned, LexError> {
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'$')
        {
            self.pos += 1;
        }

        // The chain continues only when an identifier follows the dot.
        while self.peek() == Some(b'.')
            && self
                .peek_at(1)
                .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_' || c == b'$')
        {
            self.pos += 1; // the dot
            while self
                .peek()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'$')
            {
                self.pos += 1;
            }
        }

        let text = &self.text[start..self.pos];
        let kw = KEYWORDS.iter().find(|(word, _)| *word == text).map(|&(_, k)| k);
        Ok(match kw {
            Some(k) => {
                let ends = matches!(k, Keyword::True | Keyword::False | Keyword::Null);
                self.emit(Token::Keyword(k), start, ends)
            }
            None => self.emit(
                Token::Ident(crate::plan::tree::under_record(text).to_string()),
                start,
                true,
            ),
        })
    }

    fn lex_number(&mut self, start: usize) -> Result<Spanned, LexError> {
        if self.peek() == Some(b'0')
            && let Some(base_char) = self.peek_at(1)
        {
            let radix = match base_char {
                b'x' | b'X' => Some(16),
                b'o' | b'O' => Some(8),
                b'b' | b'B' => Some(2),
                _ => None,
            };
            if let Some(radix) = radix {
                self.pos += 2;
                let ds = self.pos;
                while self.peek().is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_') {
                    self.pos += 1;
                }
                let digits = self.text[ds..self.pos].replace('_', "");
                return match i64::from_str_radix(&digits, radix) {
                    Ok(v) => Ok(self.emit(Token::Int(v), start, true)),
                    Err(e) => Err(self.err(start, format!("bad integer literal: {e}"))),
                };
            }
        }

        let mut is_float = false;
        while self.peek().is_some_and(|c| c.is_ascii_digit() || c == b'_') {
            self.pos += 1;
        }
        // A dot is a decimal point only when a digit follows.
        if self.peek() == Some(b'.') && self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) {
            is_float = true;
            self.pos += 1;
            while self.peek().is_some_and(|c| c.is_ascii_digit() || c == b'_') {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            let save = self.pos;
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if self.peek().is_some_and(|c| c.is_ascii_digit()) {
                is_float = true;
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    self.pos += 1;
                }
            } else {
                self.pos = save;
            }
        }

        let text = self.text[start..self.pos].replace('_', "");
        if is_float {
            match text.parse::<f64>() {
                Ok(v) => Ok(self.emit(Token::Float(v), start, true)),
                Err(e) => Err(self.err(start, format!("bad float literal: {e}"))),
            }
        } else {
            match text.parse::<i64>() {
                Ok(v) => Ok(self.emit(Token::Int(v), start, true)),
                // An integer too large for i64 becomes a float.
                Err(_) => match text.parse::<f64>() {
                    Ok(v) => Ok(self.emit(Token::Float(v), start, true)),
                    Err(e) => Err(self.err(start, format!("bad number: {e}"))),
                },
            }
        }
    }

    /// A hole ends at its matching `}`: brace depth is tracked and string literals are skipped.
    fn lex_template(&mut self, start: usize) -> Result<Spanned, LexError> {
        self.pos += 1; // opening backtick
        let mut parts = Vec::new();
        let mut lit = String::new();

        loop {
            let Some(c) = self.peek() else {
                return Err(self.err(start, "unterminated template literal"));
            };
            if c == b'`' {
                self.pos += 1;
                break;
            }
            if c == b'$' && self.peek_at(1) == Some(b'{') {
                if !lit.is_empty() {
                    parts.push(TemplatePart::Lit(std::mem::take(&mut lit)));
                }
                self.pos += 2; // `${`
                let hole_start = self.pos;
                let mut depth = 1usize;
                loop {
                    let Some(h) = self.peek() else {
                        return Err(self.err(start, "unterminated `${` in template literal"));
                    };
                    match h {
                        b'{' => depth += 1,
                        b'}' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        b'"' | b'\'' => {
                            let quote = h;
                            self.pos += 1;
                            loop {
                                match self.peek() {
                                    None => {
                                        return Err(self.err(start, "unterminated string in template hole"));
                                    }
                                    Some(b'\\') => self.pos += 1,
                                    Some(q) if q == quote => break,
                                    _ => {}
                                }
                                self.pos += 1;
                            }
                        }
                        _ => {}
                    }
                    self.pos += 1;
                }
                parts.push(TemplatePart::Hole {
                    src: self.text[hole_start..self.pos].to_string(),
                    offset: hole_start,
                });
                self.pos += 1; // `}`
                continue;
            }
            if c == b'\\' {
                self.pos += 1;
                let Some(esc) = self.bump() else {
                    return Err(self.err(start, "unterminated escape in template literal"));
                };
                match esc {
                    b'n' => lit.push('\n'),
                    b't' => lit.push('\t'),
                    b'r' => lit.push('\r'),
                    b'`' => lit.push('`'),
                    b'$' => lit.push('$'),
                    b'\\' => lit.push('\\'),
                    other => {
                        return Err(self.err(
                            start,
                            format!("unknown escape '\\{}' in template literal", other as char),
                        ));
                    }
                }
                continue;
            }
            // Whole UTF-8 sequences, so multi-byte characters survive.
            let s = self.pos;
            let mut e = self.pos + 1;
            while e < self.src.len() && (self.src[e] & 0xC0) == 0x80 {
                e += 1;
            }
            lit.push_str(&self.text[s..e]);
            self.pos = e;
        }

        if !lit.is_empty() {
            parts.push(TemplatePart::Lit(lit));
        }
        Ok(self.emit(Token::Template(parts), start, true))
    }

    fn lex_string(&mut self, start: usize, quote: u8) -> Result<Spanned, LexError> {
        self.pos += 1; // opening quote
        let mut out = String::new();
        loop {
            let Some(c) = self.bump() else {
                return Err(self.err(start, "unterminated string literal"));
            };
            if c == quote {
                break;
            }
            if c != b'\\' {
                let s = self.pos - 1;
                let mut e = self.pos;
                while e < self.src.len() && (self.src[e] & 0xC0) == 0x80 {
                    e += 1;
                }
                out.push_str(&self.text[s..e]);
                self.pos = e;
                continue;
            }
            let Some(esc) = self.bump() else {
                return Err(self.err(start, "unterminated escape"));
            };
            match esc {
                b'n' => out.push('\n'),
                b't' => out.push('\t'),
                b'r' => out.push('\r'),
                b'0' => out.push('\0'),
                b'\\' => out.push('\\'),
                b'\'' => out.push('\''),
                b'"' => out.push('"'),
                b'u' => {
                    // \uXXXX and \u{...}
                    let hex_start;
                    let hex_end;
                    if self.peek() == Some(b'{') {
                        self.pos += 1;
                        hex_start = self.pos;
                        while self.peek().is_some_and(|c| c != b'}') {
                            self.pos += 1;
                        }
                        hex_end = self.pos;
                        if self.bump() != Some(b'}') {
                            return Err(self.err(start, "unterminated \\u{...}"));
                        }
                    } else {
                        // Four hex digits, so the slice below ends on a character boundary whatever follows.
                        hex_start = self.pos;
                        while self.pos - hex_start < 4 && self.peek().is_some_and(|c| c.is_ascii_hexdigit()) {
                            self.pos += 1;
                        }
                        hex_end = self.pos;
                        if hex_end - hex_start < 4 {
                            return Err(self.err(start, "bad \\u escape: four hex digits are needed"));
                        }
                    }
                    let code = u32::from_str_radix(&self.text[hex_start..hex_end], 16)
                        .map_err(|e| self.err(start, format!("bad \\u escape: {e}")))?;
                    match char::from_u32(code) {
                        Some(ch) => out.push(ch),
                        None => {
                            return Err(self.err(start, format!("invalid code point U+{code:04X}")));
                        }
                    }
                }
                other => {
                    return Err(self.err(start, format!("unknown escape \\{}", other as char)));
                }
            }
        }
        Ok(self.emit(Token::Str(out), start, true))
    }

    fn lex_regex(&mut self, start: usize) -> Result<Spanned, LexError> {
        self.pos += 1; // opening slash
        let body_start = self.pos;
        let mut in_class = false;
        loop {
            let Some(c) = self.bump() else {
                return Err(self.err(start, "unterminated regex literal"));
            };
            match c {
                b'\\' => {
                    self.pos += 1;
                }
                b'[' => in_class = true,
                b']' => in_class = false,
                b'/' if !in_class => break,
                b'\n' => return Err(self.err(start, "unterminated regex literal")),
                _ => {}
            }
        }
        let source = self.text[body_start..self.pos - 1].to_string();

        let mut flags = RegexFlags::default();
        while let Some(c) = self.peek() {
            if !c.is_ascii_alphabetic() {
                break;
            }
            match c {
                b'i' => flags.case_insensitive = true,
                b'm' => flags.multi_line = true,
                b's' => flags.dot_matches_newline = true,
                other => {
                    return Err(self.err(start, format!("unknown regex flag '{}'", other as char)));
                }
            }
            self.pos += 1;
        }
        Ok(self.emit(Token::Regex { source, flags }, start, true))
    }

    fn lex_punct(&mut self, start: usize) -> Result<Spanned, LexError> {
        use Punct::*;
        let c = self.src[self.pos];
        let c1 = self.peek_at(1);

        // Longest match first.
        let c2 = self.peek_at(2);
        let (p, len) = match (c, c1) {
            (b'.', Some(b'.')) if c2 == Some(b'.') => (Ellipsis, 3),
            (b'*', Some(b'*')) if c2 == Some(b'=') => (AssignOp(BinOp::Pow), 3),
            (b'?', Some(b'?')) if c2 == Some(b'=') => (AssignOp(BinOp::Coalesce), 3),
            (b'&', Some(b'&')) if c2 == Some(b'=') => (AssignOp(BinOp::And), 3),
            (b'|', Some(b'|')) if c2 == Some(b'=') => (AssignOp(BinOp::Or), 3),
            (b'<', Some(b'<')) if c2 == Some(b'=') => (AssignOp(BinOp::Shl), 3),
            (b'>', Some(b'>')) if c2 == Some(b'=') => (AssignOp(BinOp::Shr), 3),

            (b'*', Some(b'*')) => (StarStar, 2),
            (b'=', Some(b'=')) => (EqEq, if c2 == Some(b'=') { 3 } else { 2 }),
            (b'!', Some(b'=')) => (BangEq, if c2 == Some(b'=') { 3 } else { 2 }),
            (b'<', Some(b'=')) => (Le, 2),
            (b'>', Some(b'=')) => (Ge, 2),

            (b'+', Some(b'=')) => (AssignOp(BinOp::Add), 2),
            (b'-', Some(b'=')) => (AssignOp(BinOp::Sub), 2),
            (b'*', Some(b'=')) => (AssignOp(BinOp::Mul), 2),
            (b'/', Some(b'=')) => (AssignOp(BinOp::Div), 2),
            (b'%', Some(b'=')) => (AssignOp(BinOp::Mod), 2),
            (b'&', Some(b'=')) => (AssignOp(BinOp::BitAnd), 2),
            (b'|', Some(b'=')) => (AssignOp(BinOp::BitOr), 2),
            (b'^', Some(b'=')) => (AssignOp(BinOp::BitXor), 2),

            (b'=', Some(b'>')) => (FatArrow, 2),

            (b'+', Some(b'+')) => (PlusPlus, 2),
            (b'-', Some(b'-')) => (MinusMinus, 2),

            (b'<', Some(b'<')) => (LtLt, 2),
            (b'>', Some(b'>')) => (GtGt, 2),
            (b'&', Some(b'&')) => (AmpAmp, 2),
            (b'|', Some(b'|')) => (PipePipe, 2),
            (b'?', Some(b'?')) => (QuestionQuestion, 2),
            (b'+', _) => (Plus, 1),
            (b'-', _) => (Minus, 1),
            (b'*', _) => (Star, 1),
            (b'/', _) => (Slash, 1),
            (b'%', _) => (Percent, 1),
            (b'<', _) => (Lt, 1),
            (b'>', _) => (Gt, 1),
            (b'!', _) => (Bang, 1),
            (b'&', _) => (Amp, 1),
            (b'|', _) => (Pipe, 1),
            (b'^', _) => (Caret, 1),
            (b'~', _) => (Tilde, 1),
            (b'?', _) => (Question, 1),
            (b':', _) => (Colon, 1),
            (b'(', _) => (LParen, 1),
            (b')', _) => (RParen, 1),
            (b'[', _) => (LBracket, 1),
            (b']', _) => (RBracket, 1),
            (b'{', _) => (LBrace, 1),
            (b'}', _) => (RBrace, 1),
            (b',', _) => (Comma, 1),
            (b'.', _) => (Dot, 1),
            (b'=', _) => (Assign, 1),
            (b';', _) => (Semi, 1),
            _ => return Err(self.unexpected_character(start)),
        };
        self.pos += len;

        let ends = matches!(p, RParen | RBracket | RBrace);
        Ok(self.emit(Token::Punct(p), start, ends))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `}` inside a nested object literal must not close the hole.
    #[test]
    fn a_template_hole_tracks_brace_depth() {
        let t = toks("`${ {\"a\": 1} }`");
        let Token::Template(parts) = &t[0] else {
            panic!("expected a template, got {:?}", t[0]);
        };
        let holes: Vec<&str> = parts
            .iter()
            .filter_map(|p| match p {
                TemplatePart::Hole { src, .. } => Some(src.as_str()),
                TemplatePart::Lit(_) => None,
            })
            .collect();
        assert_eq!(
            holes,
            vec![" {\"a\": 1} "],
            "the hole must span the whole object literal, not stop at its first `}}`"
        );
    }

    #[test]
    fn a_template_hole_skips_braces_inside_strings() {
        let t = toks("`${ \"}\" }`");
        let Token::Template(parts) = &t[0] else {
            panic!("expected a template");
        };
        assert!(matches!(
            &parts[0],
            TemplatePart::Hole { src, .. } if src == " \"}\" "
        ));
    }

    fn toks(s: &str) -> Vec<Token> {
        Lexer::tokenize(s).unwrap().into_iter().map(|t| t.token).collect()
    }

    /// `\uXXXX` takes hex digits and nothing else, so what follows a short escape is never sliced through.
    #[test]
    fn a_short_unicode_escape_is_an_error_not_a_panic() {
        assert_eq!(toks(r#""\u00e9""#)[0], Token::Str("\u{e9}".into()));
        for src in [r#""\u12€""#, r#""\u12""#, r#""\u""#] {
            let err = Lexer::tokenize(src).unwrap_err();
            assert!(err.message.contains("bad \\u escape"), "{src}: {}", err.message);
        }
    }

    fn ident(s: &str) -> Token {
        Token::Ident(s.to_string())
    }

    #[test]
    fn simple_expression() {
        assert_eq!(
            toks("a + b * 2"),
            vec![
                ident("a"),
                Token::Punct(Punct::Plus),
                ident("b"),
                Token::Punct(Punct::Star),
                Token::Int(2),
                Token::Eof
            ]
        );
    }

    /// A character no token begins with is most often a field name the bare form cannot spell. The error shows
    /// the name it sits in and the `$["..."]` that reads it.
    #[test]
    fn an_unexpected_character_names_the_field_and_the_quoted_form() {
        for (src, name, c) in [
            ("@timestamp", "@timestamp", '@'),
            ("year(@timestamp)", "@timestamp", '@'),
            ("a + naïve.b", "naïve.b", 'ï'),
            ("x#1 == 2", "x#1", '#'),
        ] {
            let e = Lexer::tokenize(src).err().unwrap_or_else(|| panic!("{src} lexed"));
            assert_eq!(
                e.message,
                format!(
                    "unexpected character '{c}' in `{name}` -- a name with characters outside letters, digits, `_` \
                     and `$` is written `$[\"{name}\"]`"
                ),
                "{src}"
            );
        }
        let e = Lexer::tokenize(r"a\b@c").expect_err("lexed");
        assert!(e.message.ends_with(r#"is written `$["a\\b@c"]`"#), "{}", e.message);
    }

    #[test]
    fn dotted_identifier_is_one_token() {
        assert_eq!(toks("source.ip"), vec![ident("source.ip"), Token::Eof]);
        assert_eq!(toks("a.b.c.d"), vec![ident("a.b.c.d"), Token::Eof]);
    }

    #[test]
    fn dotted_chain_stops_before_a_non_identifier() {
        assert_eq!(
            toks("x.5"),
            vec![ident("x"), Token::Punct(Punct::Dot), Token::Int(5), Token::Eof]
        );
    }

    #[test]
    fn slash_after_an_identifier_is_division() {
        assert_eq!(
            toks("a / b"),
            vec![ident("a"), Token::Punct(Punct::Slash), ident("b"), Token::Eof]
        );
    }

    #[test]
    fn slash_at_the_start_is_a_regex() {
        match &toks("/foo/i")[0] {
            Token::Regex { source, flags } => {
                assert_eq!(source, "foo");
                assert!(flags.case_insensitive);
            }
            other => panic!("expected regex, got {other:?}"),
        }
    }

    #[test]
    fn regex_after_an_operator() {
        let t = toks("x == /abc/");
        assert!(matches!(t[2], Token::Regex { .. }), "got {:?}", t[2]);
    }

    #[test]
    fn division_after_a_closing_paren() {
        let t = toks("(a + b) / 2");
        assert_eq!(t[5], Token::Punct(Punct::Slash));
    }

    #[test]
    fn regex_after_a_comma_in_a_call() {
        let t = toks("replaceAll(x, /a/, \"b\")");
        assert!(matches!(t[4], Token::Regex { .. }), "got {:?}", t[4]);
    }

    #[test]
    fn regex_with_escaped_slash() {
        match &toks(r"/a\/b/")[0] {
            Token::Regex { source, .. } => assert_eq!(source, r"a\/b"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn regex_slash_inside_a_character_class_is_literal() {
        match &toks("/[/]/")[0] {
            Token::Regex { source, .. } => assert_eq!(source, "[/]"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn all_regex_flags_parse() {
        match &toks("/x/ims")[0] {
            Token::Regex { flags, .. } => {
                assert!(flags.case_insensitive && flags.multi_line && flags.dot_matches_newline);
            }
            other => panic!("got {other:?}"),
        }
    }

    /// Every flag the lexer takes reaches the regex engine. `g` chose an
    /// overload -- `replaceFirst`/`replaceAll` name that now -- and `d`, `u`,
    /// `v` and `y` never did anything at all.
    #[test]
    fn a_flag_that_does_nothing_is_rejected() {
        for src in ["/x/g", "/x/d", "/x/u", "/x/v", "/x/y"] {
            let e = Lexer::tokenize(src).expect_err(src);
            assert!(e.message.contains("unknown regex flag"), "{src}: {}", e.message);
        }
    }

    #[test]
    fn unterminated_regex_is_an_error() {
        assert!(Lexer::tokenize("/abc").is_err());
        assert!(Lexer::tokenize("/abc\nx/").is_err());
    }

    #[test]
    fn numbers() {
        assert_eq!(toks("42")[0], Token::Int(42));
        assert_eq!(toks("3.5")[0], Token::Float(3.5));
        assert_eq!(toks("1e3")[0], Token::Float(1000.0));
        assert_eq!(toks("1_000_000")[0], Token::Int(1_000_000));
        assert_eq!(toks("0xff")[0], Token::Int(255));
        assert_eq!(toks("0b1010")[0], Token::Int(10));
        assert_eq!(toks("0o17")[0], Token::Int(15));
    }

    #[test]
    fn oversized_integer_becomes_a_float() {
        assert!(matches!(toks("99999999999999999999")[0], Token::Float(_)));
    }

    #[test]
    fn strings_and_escapes() {
        assert_eq!(toks(r#""hi""#)[0], Token::Str("hi".into()));
        assert_eq!(toks(r#""a\nb""#)[0], Token::Str("a\nb".into()));
        assert_eq!(toks(r#""q\"q""#)[0], Token::Str("q\"q".into()));
        assert_eq!(toks(r"'single'")[0], Token::Str("single".into()));
        assert_eq!(toks(r#""\u0041""#)[0], Token::Str("A".into()));
        assert_eq!(toks(r#""\u{1F44B}""#)[0], Token::Str("\u{1F44B}".into()));
    }

    #[test]
    fn multibyte_string_content_survives() {
        assert_eq!(toks(r#""café""#)[0], Token::Str("café".into()));
        assert_eq!(toks(r#""👋""#)[0], Token::Str("👋".into()));
    }

    #[test]
    fn unterminated_string_is_an_error() {
        assert!(Lexer::tokenize(r#""abc"#).is_err());
    }

    #[test]
    fn keywords_versus_identifiers() {
        assert_eq!(toks("true")[0], Token::Keyword(Keyword::True));
        assert_eq!(toks("null")[0], Token::Keyword(Keyword::Null));
        assert_eq!(toks("nullable")[0], ident("nullable"));
        assert_eq!(toks("trueish")[0], ident("trueish"));
    }

    #[test]
    fn longest_match_wins_on_operators() {
        assert_eq!(toks("**")[0], Token::Punct(Punct::StarStar));
        assert_eq!(toks("==")[0], Token::Punct(Punct::EqEq));
        assert_eq!(toks("===")[0], Token::Punct(Punct::EqEq));
        assert_eq!(toks("??")[0], Token::Punct(Punct::QuestionQuestion));
        assert_eq!(toks("?")[0], Token::Punct(Punct::Question));
        assert_eq!(toks("&&")[0], Token::Punct(Punct::AmpAmp));
        assert_eq!(toks("&")[0], Token::Punct(Punct::Amp));
        assert_eq!(toks(">>")[0], Token::Punct(Punct::GtGt));
    }

    #[test]
    fn comments_are_skipped() {
        assert_eq!(toks("a // trailing\n+ b").len(), 4);
        assert_eq!(toks("a /* mid */ + b").len(), 4);
    }

    #[test]
    fn block_comment_is_not_mistaken_for_a_regex() {
        let t = toks("/* c */ a");
        assert_eq!(t[0], ident("a"));
    }

    #[test]
    fn spans_point_at_the_source() {
        let src = "abc + 1";
        let t = Lexer::tokenize(src).unwrap();
        assert_eq!(t[0].span.slice(src), "abc");
        assert_eq!(t[1].span.slice(src), "+");
        assert_eq!(t[2].span.slice(src), "1");
    }

    #[test]
    fn empty_input_is_just_eof() {
        assert_eq!(toks(""), vec![Token::Eof]);
        assert_eq!(toks("   \n  "), vec![Token::Eof]);
    }
}
