//! The SQL an SQL source runs: the user's query, checked, inside a statement of smeltr's own.
//!
//! ```text
//!   the user's query ──► Query::parse ──► refused, with the reason
//!                              │
//!                              ▼
//!   SELECT * FROM (                       ◄── statement(query, poll)
//!     the query, :last.<column> bound
//!   ) AS q
//!   WHERE position > the last row sent    ◄── only where a position was chosen and a row was sent
//!     AND position <= now - settle        ◄── only for a time position
//!   ORDER BY position
//!   LIMIT n
//! ```
//!
//! smeltr never parses the query's grammar. It reads it only as far as telling code from strings, quoted names and
//! comments, which is enough to refuse what would make the statement wrong and to find `:last.<column>`. Everything a
//! dialect writes its own way -- quoting, placeholders, the position test, the clock, the limit -- is a [`Dialect`]
//! decision here, so a driver supplies none of it.

use std::fmt;
use std::ops::Range;

use crate::core::column::DataType;

/// The SQL a source's database speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    /// SQL Server refuses a `WITH` inside a derived table, so its driver must lift the query's `WITH` above smeltr's
    /// `SELECT`. Until it does, a `WITH` query fails there.
    SqlServer,
    /// MySQL and MariaDB.
    MySql,
}

/// A query a source may run: a single `SELECT` or `WITH`, with no order or limit of its own.
///
/// `SELECT` and `WITH` hold the query to a read, as far as text can. PostgreSQL allows a write inside `WITH`, so a
/// read-only credential is still what keeps a source from writing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    /// The query as written, less a trailing semicolon and anything after it.
    text: String,
    dialect: Dialect,
    /// Every `:last.<column>`, by the byte range of the whole reference in `text` and the column it names.
    references: Vec<(Range<usize>, String)>,
}

/// Why a query cannot be a source's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refused {
    Empty,
    /// It begins with this word, not `SELECT` or `WITH`.
    NotARead(String),
    MoreThanOneStatement,
    /// A string, quoted name or comment that never ends; names which.
    Unterminated(&'static str),
    /// An `ORDER BY` of the query's own, outside any parenthesis.
    Ordered,
    /// A limit of the query's own: `LIMIT`, `OFFSET`, `FETCH` or `TOP`.
    Limited(String),
    /// A placeholder the statement's own would collide with.
    Placeholder(String),
    /// `:last.` followed by no column name.
    LastWithoutColumn,
    /// A `)` with no `(` before it, or a `(` never closed. Either moves smeltr's own clauses into another query.
    Unbalanced,
    /// MySQL's `/*! */`, which runs its contents as code.
    ExecutableComment,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::Empty => write!(f, "the query is empty"),
            Refused::NotARead(word) => {
                write!(
                    f,
                    "only a SELECT or a WITH runs here; this query begins with {word}. A source reads; it never writes"
                )
            }
            Refused::MoreThanOneStatement => write!(f, "this is more than one statement; a source runs one query"),
            Refused::Unterminated(what) => write!(f, "a {what} is never closed"),
            Refused::Ordered => write!(f, "leave the ORDER BY out: smeltr orders by the position you choose"),
            Refused::Limited(word) => {
                write!(
                    f,
                    "leave the {word} out: with it, every poll returns the same rows. smeltr sets the batch size"
                )
            }
            Refused::Placeholder(text) => {
                write!(
                    f,
                    "{text} is a placeholder; use :last.<column> for a value from the last row sent"
                )
            }
            Refused::LastWithoutColumn => write!(f, ":last. must be followed by the name of a column of the result"),
            Refused::Unbalanced => write!(f, "the parentheses do not pair up"),
            Refused::ExecutableComment => {
                write!(
                    f,
                    "a /*! */ comment runs as code; write its contents as the query itself"
                )
            }
        }
    }
}

impl std::error::Error for Refused {}

impl Query {
    /// Checks `text` as a query `dialect` can run as a source.
    pub fn parse(text: &str, dialect: Dialect) -> Result<Query, Refused> {
        let segments = segments(text, dialect)?;
        let tokens = tokens(text, &segments);
        let Some(first) = tokens.first() else {
            return Err(Refused::Empty);
        };
        match first {
            Token::Word(range) if is(text, range, "SELECT") || is(text, range, "WITH") => {}
            Token::Word(range) => return Err(Refused::NotARead(text[range.clone()].to_uppercase())),
            Token::Mark(at, _) => return Err(Refused::NotARead(text[*at..].chars().next().unwrap().to_string())),
        }

        // The statement ends at a final semicolon. One anywhere else starts a second statement.
        let mut end = text.len();
        for (n, token) in tokens.iter().enumerate() {
            if let Token::Mark(at, ';') = token {
                if n + 1 != tokens.len() {
                    return Err(Refused::MoreThanOneStatement);
                }
                end = *at;
            }
        }
        refuse_own_order_or_limit(text, &tokens, dialect)?;
        refuse_placeholders(text, &segments, dialect)?;
        let references = references(text, &segments, end)?;
        Ok(Query {
            text: text[..end].trim_end().to_string(),
            dialect,
            references,
        })
    }

    /// Every column the query names with `:last.`, once each, in the order first named. smeltr remembers these from
    /// the last row sent, as well as the position.
    pub fn last_columns(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        for (_, name) in &self.references {
            if !names.contains(&name.as_str()) {
                names.push(name);
            }
        }
        names
    }
}

/// What one poll reads.
#[derive(Clone, Debug)]
pub struct Poll<'a> {
    /// The columns of the result that order it: the incrementing or updated at column, then the columns that tell
    /// apart rows with the same value in it. Empty for a mode with no position, or a Run before one is chosen.
    pub position: &'a [String],
    /// Whether a row was sent, so the poll reads past the last one. Without one it reads from [`Bind::Start`] when
    /// `start` is set, else from the beginning.
    pub after: bool,
    /// Whether, before any row was sent, the poll reads the position's first column at or past the value the source
    /// starts from. At, so a row sharing it is read: a duplicate, never a loss.
    pub start: bool,
    /// How long to wait for late commits. Only for a position whose first column is a time.
    pub settle_secs: Option<u32>,
    /// The most rows the poll returns.
    pub batch: u32,
    /// The record's fields, each with its declared type: a `:last.<column>` takes its field's type.
    pub fields: &'a [(String, DataType)],
}

/// What fills one placeholder of a [`Statement`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Bind {
    /// The value of the last row sent in this column of [`Poll::position`].
    Position(usize),
    /// The value of the last row sent in this column, for a `:last.<column>`; `NULL` before the first row.
    Last(String),
    /// The value the source starts from, in the position's first column.
    Start,
}

/// A statement to run, and what fills each placeholder, in order: `binds[n]` fills the placeholder numbered `n + 1`.
/// MySQL's `?` placeholders are numbered by where they stand, so a value used twice there is bound twice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Statement {
    pub sql: String,
    pub binds: Vec<Bind>,
}

/// The statement a poll runs: `query`, unchanged but for its `:last.<column>` references, inside smeltr's own.
///
/// A `:last` filter only narrows what the query reads: the statement still reads past the last row sent and waits for
/// late commits, so a batch that ends among rows sharing a value loses none of them to a filter that keeps that value.
///
/// SQL Server has no session time zone, so a position without one is compared against the database's clock in UTC.
pub fn statement(query: &Query, poll: &Poll<'_>) -> Statement {
    let dialect = query.dialect;
    let mut binds = Binder {
        dialect,
        binds: Vec::new(),
    };
    let inner = bound(query, &mut binds, poll.fields);

    let column = |name: &str| format!("q.{}", quote(dialect, name));
    let mut sql = match dialect {
        Dialect::SqlServer => format!("SELECT TOP ({}) * FROM (\n", poll.batch),
        Dialect::Postgres | Dialect::MySql => "SELECT * FROM (\n".to_string(),
    };
    sql.push_str(&inner);
    // On a line of its own, so a comment ending the query cannot swallow it.
    sql.push_str("\n) AS q");

    let mut conditions = Vec::new();
    if !poll.position.is_empty() {
        if poll.after {
            conditions.push(past_position(dialect, poll.position, &column, &mut binds));
        } else if poll.start {
            let first = column(&poll.position[0]);
            conditions.push(format!("{first} >= {}", binds.bind(Bind::Start)));
        }
        if let Some(secs) = poll.settle_secs {
            conditions.push(settled(dialect, &column(&poll.position[0]), secs));
        }
    }
    if !conditions.is_empty() {
        sql.push_str("\nWHERE ");
        sql.push_str(&conditions.join("\n  AND "));
    }
    if !poll.position.is_empty() {
        let order: Vec<String> = poll.position.iter().map(|c| column(c)).collect();
        sql.push_str("\nORDER BY ");
        sql.push_str(&order.join(", "));
    }
    if dialect != Dialect::SqlServer {
        sql.push_str(&format!("\nLIMIT {}", poll.batch));
    }
    Statement {
        sql,
        binds: binds.binds,
    }
}

/// Rows whose time in `first` is at least `secs` behind the database's clock: those no late commit can still join.
fn settled(dialect: Dialect, first: &str, secs: u32) -> String {
    match dialect {
        Dialect::Postgres => format!("{first} <= now() - interval '{secs} seconds'"),
        Dialect::MySql => format!("{first} <= NOW(6) - INTERVAL {secs} SECOND"),
        Dialect::SqlServer => format!("{first} <= DATEADD(second, -{secs}, SYSUTCDATETIME())"),
    }
}

/// The largest value `column` holds in `query`'s result: where a source that starts now starts. A `:last.<column>`
/// in the query binds as it would before the first row. With `settle_secs`, only settled rows count, so a late commit
/// behind the newest row is still read.
pub fn maximum(query: &Query, column: &str, settle_secs: Option<u32>, fields: &[(String, DataType)]) -> Statement {
    let dialect = query.dialect;
    let mut binds = Binder {
        dialect,
        binds: Vec::new(),
    };
    let mut sql = format!("SELECT max(q.{}) FROM (\n", quote(dialect, column));
    sql.push_str(&bound(query, &mut binds, fields));
    sql.push_str("\n) AS q");
    if let Some(secs) = settle_secs {
        sql.push_str("\nWHERE ");
        sql.push_str(&settled(dialect, &format!("q.{}", quote(dialect, column)), secs));
    }
    Statement {
        sql,
        binds: binds.binds,
    }
}

/// `query` with each `:last.<column>` a placeholder. PostgreSQL cannot type a placeholder from `$1 IS NULL`, the way
/// a query asks whether a row was sent yet, so there each is cast to its field's type.
fn bound(query: &Query, binds: &mut Binder, fields: &[(String, DataType)]) -> String {
    let mut out = String::with_capacity(query.text.len());
    let mut from = 0;
    for (range, name) in &query.references {
        out.push_str(&query.text[from..range.start]);
        let placeholder = binds.bind(Bind::Last(name.clone()));
        let ty = fields
            .iter()
            .find(|(field, _)| field == name)
            .and_then(|(_, ty)| postgres_type(ty));
        match (query.dialect, ty) {
            (Dialect::Postgres, Some(ty)) => out.push_str(&format!("CAST({placeholder} AS {ty})")),
            _ => out.push_str(&placeholder),
        }
        from = range.end;
    }
    out.push_str(&query.text[from..]);
    out
}

/// The PostgreSQL type a value of `ty` is bound as; `None` for one PostgreSQL types from where it stands. A text
/// field may read a uuid, a numeric or an enum, each compared only with its own type, so text is never cast.
fn postgres_type(ty: &DataType) -> Option<&'static str> {
    match ty {
        DataType::Bool => Some("boolean"),
        DataType::I64 => Some("bigint"),
        DataType::F64 => Some("double precision"),
        DataType::DateTime => Some("timestamptz"),
        DataType::Duration => Some("interval"),
        DataType::Bytes => Some("bytea"),
        _ => None,
    }
}

/// Rows past the last one sent, in the position's order. PostgreSQL compares rows directly, which its indexes serve.
/// SQL Server has no row comparison, and MySQL's is not served by an index in every version, so both spell it out.
fn past_position(dialect: Dialect, position: &[String], column: &dyn Fn(&str) -> String, binds: &mut Binder) -> String {
    if dialect == Dialect::Postgres {
        let columns: Vec<String> = position.iter().map(|c| column(c)).collect();
        let values: Vec<String> = (0..position.len()).map(|n| binds.bind(Bind::Position(n))).collect();
        return if position.len() == 1 {
            format!("{} > {}", columns[0], values[0])
        } else {
            format!("({}) > ({})", columns.join(", "), values.join(", "))
        };
    }
    fn from(n: usize, position: &[String], column: &dyn Fn(&str) -> String, binds: &mut Binder) -> String {
        let name = column(&position[n]);
        let greater = format!("{name} > {}", binds.bind(Bind::Position(n)));
        if n + 1 == position.len() {
            return greater;
        }
        let equal = format!("{name} = {}", binds.bind(Bind::Position(n)));
        format!("({greater} OR ({equal} AND {}))", from(n + 1, position, column, binds))
    }
    from(0, position, column, binds)
}

struct Binder {
    dialect: Dialect,
    binds: Vec<Bind>,
}

impl Binder {
    /// The placeholder for `bind`. PostgreSQL and SQL Server number theirs, so a value used twice is bound once.
    fn bind(&mut self, bind: Bind) -> String {
        let number = match self.dialect {
            Dialect::MySql => None,
            Dialect::Postgres | Dialect::SqlServer => self.binds.iter().position(|b| *b == bind),
        };
        let number = number.unwrap_or_else(|| {
            self.binds.push(bind);
            self.binds.len() - 1
        }) + 1;
        match self.dialect {
            Dialect::Postgres => format!("${number}"),
            Dialect::SqlServer => format!("@P{number}"),
            Dialect::MySql => "?".to_string(),
        }
    }
}

/// `name` as a quoted name, so a column named after a keyword, or with a capital, is the column.
pub(crate) fn quote(dialect: Dialect, name: &str) -> String {
    match dialect {
        Dialect::Postgres => format!("\"{}\"", name.replace('"', "\"\"")),
        Dialect::SqlServer => format!("[{}]", name.replace(']', "]]")),
        Dialect::MySql => format!("`{}`", name.replace('`', "``")),
    }
}

/// The parts of a query, in order: code, or a string, quoted name or comment, by byte range.
fn segments(text: &str, dialect: Dialect) -> Result<Vec<(Range<usize>, bool)>, Refused> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut code_from = 0;
    let mut i = 0;
    // Each arm finds where a non-code part starts and ends at an ASCII byte, so no UTF-8 sequence is split.
    while i < b.len() {
        let next = b.get(i + 1).copied();
        let end = match b[i] {
            b'\'' => {
                let backslash = dialect == Dialect::MySql || (dialect == Dialect::Postgres && escape_string(b, i));
                Some(quoted(b, i, b'\'', backslash).ok_or(Refused::Unterminated("string"))?)
            }
            // MySQL's `"` opens a string, with a string's escapes.
            b'"' => Some(quoted(b, i, b'"', dialect == Dialect::MySql).ok_or(Refused::Unterminated("quoted name"))?),
            b'`' if dialect == Dialect::MySql => {
                Some(quoted(b, i, b'`', false).ok_or(Refused::Unterminated("quoted name"))?)
            }
            b'[' if dialect == Dialect::SqlServer => {
                Some(quoted(b, i, b']', false).ok_or(Refused::Unterminated("quoted name"))?)
            }
            // MySQL reads `--` as a comment only before white space: `a--1` is `a - -1`.
            b'-' if next == Some(b'-')
                && (dialect != Dialect::MySql
                    || b.get(i + 2)
                        .is_none_or(|c| c.is_ascii_whitespace() || c.is_ascii_control())) =>
            {
                Some(line_end(b, i))
            }
            b'#' if dialect == Dialect::MySql => Some(line_end(b, i)),
            // MySQL's `/*!` and MariaDB's `/*M!` run their contents.
            b'/' if next == Some(b'*')
                && dialect == Dialect::MySql
                && (b.get(i + 2) == Some(&b'!') || b[i..].starts_with(b"/*M!")) =>
            {
                return Err(Refused::ExecutableComment);
            }
            b'/' if next == Some(b'*') => {
                Some(block_comment_end(b, i, dialect != Dialect::MySql).ok_or(Refused::Unterminated("comment"))?)
            }
            // A `$` inside a name is part of the name: `x$y$` is not a tag.
            b'$' if dialect == Dialect::Postgres && !after_name(b, i) => match dollar_tag(b, i) {
                Some(tag) => Some(dollar_end(b, i, tag).ok_or(Refused::Unterminated("string"))?),
                None => None,
            },
            _ => None,
        };
        match end {
            Some(end) => {
                if code_from < i {
                    out.push((code_from..i, true));
                }
                out.push((i..end, false));
                i = end;
                code_from = end;
            }
            None => i += 1,
        }
    }
    if code_from < b.len() {
        out.push((code_from..b.len(), true));
    }
    Ok(out)
}

/// The end of a part opened at `start` and closed by `close`, where a doubled `close` stands for itself and, where
/// `backslash`, a backslash escapes the next byte.
fn quoted(b: &[u8], start: usize, close: u8, backslash: bool) -> Option<usize> {
    let mut j = start + 1;
    while j < b.len() {
        if backslash && b[j] == b'\\' {
            j += 2;
            continue;
        }
        if b[j] == close {
            if b.get(j + 1) == Some(&close) {
                j += 2;
                continue;
            }
            return Some(j + 1);
        }
        j += 1;
    }
    None
}

/// Whether the `'` at `start` opens PostgreSQL's `E'...'`, where a backslash escapes.
fn escape_string(b: &[u8], start: usize) -> bool {
    start > 0 && b[start - 1].eq_ignore_ascii_case(&b'e') && !after_name(b, start - 1)
}

/// A byte of a name. Every byte of a character outside ASCII is one, as the databases read them.
fn name_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80
}

/// Whether the byte at `at` continues a name, rather than starting something.
fn after_name(b: &[u8], at: usize) -> bool {
    at > 0 && (name_byte(b[at - 1]) || b[at - 1] == b'$')
}

fn line_end(b: &[u8], start: usize) -> usize {
    b[start..]
        .iter()
        .position(|&c| c == b'\n' || c == b'\r')
        .map_or(b.len(), |n| start + n)
}

/// The end of a `/* */` comment. PostgreSQL's and SQL Server's nest.
fn block_comment_end(b: &[u8], start: usize, nests: bool) -> Option<usize> {
    let mut depth = 0;
    let mut j = start;
    while j + 1 < b.len() {
        if b[j] == b'/' && b[j + 1] == b'*' && (nests || depth == 0) {
            depth += 1;
            j += 2;
        } else if b[j] == b'*' && b[j + 1] == b'/' {
            depth -= 1;
            j += 2;
            if depth == 0 {
                return Some(j);
            }
        } else {
            j += 1;
        }
    }
    None
}

/// PostgreSQL's `$tag$` or `$$` opening a dollar-quoted string at `start`, as its length. `$1` is a placeholder.
fn dollar_tag(b: &[u8], start: usize) -> Option<usize> {
    let mut j = start + 1;
    if b.get(j).is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    while j < b.len() && name_byte(b[j]) {
        j += 1;
    }
    (b.get(j) == Some(&b'$')).then_some(j + 1 - start)
}

fn dollar_end(b: &[u8], start: usize, tag_len: usize) -> Option<usize> {
    let tag = &b[start..start + tag_len];
    let body = start + tag_len;
    b[body..]
        .windows(tag_len)
        .position(|w| w == tag)
        .map(|n| body + n + tag_len)
}

#[derive(Clone, Debug)]
enum Token {
    /// A word: a keyword, a name, or a number.
    Word(Range<usize>),
    /// Any other byte of code that is not white space, at its offset. Every such byte is ASCII.
    Mark(usize, char),
}

fn tokens(text: &str, segments: &[(Range<usize>, bool)]) -> Vec<Token> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    for (range, code) in segments {
        if !code {
            continue;
        }
        let mut i = range.start;
        while i < range.end {
            let c = b[i];
            if name_byte(c) {
                let from = i;
                while i < range.end && (name_byte(b[i]) || b[i] == b'$') {
                    i += 1;
                }
                out.push(Token::Word(from..i));
            } else if c.is_ascii_whitespace() {
                i += 1;
            } else {
                out.push(Token::Mark(i, c as char));
                i += 1;
            }
        }
    }
    out
}

fn is(text: &str, range: &Range<usize>, word: &str) -> bool {
    text[range.clone()].eq_ignore_ascii_case(word)
}

/// The query's own order or limit stands outside every parenthesis; inside one, it belongs to a window, an aggregate
/// or a subquery. Each limit word counts only where it can be one, so a column named `offset` or `top` is a column.
/// Parentheses must pair, else smeltr's clauses would land inside a query of the user's.
fn refuse_own_order_or_limit(text: &str, tokens: &[Token], dialect: Dialect) -> Result<(), Refused> {
    let word = |n: usize| match tokens.get(n) {
        Some(Token::Word(range)) => Some(range),
        _ => None,
    };
    let is_word = |n: usize, w: &str| word(n).is_some_and(|r| is(text, r, w));
    let is_number = |n: usize| word(n).is_some_and(|r| text.as_bytes()[r.start].is_ascii_digit());
    // An offset counts by its value: a number, or an expression in parentheses.
    let is_value = |n: usize| is_number(n) || matches!(tokens.get(n), Some(Token::Mark(_, '(')));
    let mut depth = 0i32;
    for (n, token) in tokens.iter().enumerate() {
        match token {
            Token::Mark(_, '(') => depth += 1,
            Token::Mark(_, ')') if depth == 0 => return Err(Refused::Unbalanced),
            Token::Mark(_, ')') => depth -= 1,
            Token::Word(_) if depth == 0 => {
                if is_word(n, "ORDER") && is_word(n + 1, "BY") {
                    return Err(Refused::Ordered);
                }
                // SQL Server has no LIMIT, so there `limit` is a name.
                let limits = (dialect != Dialect::SqlServer && is_word(n, "LIMIT"))
                    || (is_word(n, "OFFSET") && is_value(n + 1))
                    || (is_word(n, "FETCH") && (is_word(n + 1, "FIRST") || is_word(n + 1, "NEXT")))
                    || (dialect == Dialect::SqlServer
                        && is_word(n, "TOP")
                        && (is_word(n.wrapping_sub(1), "SELECT")
                            || is_word(n.wrapping_sub(1), "DISTINCT")
                            || is_word(n.wrapping_sub(1), "ALL")));
                if limits {
                    return Err(Refused::Limited(text[word(n).unwrap().clone()].to_uppercase()));
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(Refused::Unbalanced);
    }
    Ok(())
}

/// A placeholder written into the query would be numbered against the statement's own.
fn refuse_placeholders(text: &str, segments: &[(Range<usize>, bool)], dialect: Dialect) -> Result<(), Refused> {
    let b = text.as_bytes();
    for (range, code) in segments {
        if !code {
            continue;
        }
        for i in range.clone() {
            let digits = |from: usize| {
                let n = b[from..range.end].iter().take_while(|c| c.is_ascii_digit()).count();
                (n > 0).then(|| text[i..from + n].to_string())
            };
            let found = match dialect {
                Dialect::Postgres if b[i] == b'$' && !after_name(b, i) => digits(i + 1),
                Dialect::SqlServer if b[i] == b'@' && b.get(i + 1).is_some_and(|c| c.eq_ignore_ascii_case(&b'p')) => {
                    digits(i + 2)
                }
                Dialect::MySql if b[i] == b'?' => Some("?".to_string()),
                _ => None,
            };
            if let Some(found) = found {
                return Err(Refused::Placeholder(found));
            }
        }
    }
    Ok(())
}

const LAST: &str = ":last.";

/// Every `:last.<column>` in code before `end`. A PostgreSQL cast (`x::last`) is not one.
fn references(
    text: &str,
    segments: &[(Range<usize>, bool)],
    end: usize,
) -> Result<Vec<(Range<usize>, String)>, Refused> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    for (range, code) in segments {
        if !code || range.start >= end {
            continue;
        }
        let code_end = range.end.min(end);
        let mut i = range.start;
        while let Some(n) = text[i..code_end].find(LAST) {
            let at = i + n;
            i = at + LAST.len();
            if at > 0 && b[at - 1] == b':' {
                continue;
            }
            let name_end = i + b[i..code_end]
                .iter()
                .take_while(|c| name_byte(**c) || **c == b'$')
                .count();
            if name_end == i || b[i].is_ascii_digit() {
                return Err(Refused::LastWithoutColumn);
            }
            out.push((at..name_end, text[i..name_end].to_string()));
            i = name_end;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVENTS: &str = "SELECT event_id, occurred_at, user_name FROM auth.events";

    fn position(columns: &[&str]) -> Vec<String> {
        columns.iter().map(|c| c.to_string()).collect()
    }

    fn poll(position: &[String], after: bool, settle_secs: Option<u32>) -> Poll<'_> {
        Poll {
            position,
            after,
            start: false,
            settle_secs,
            batch: 5000,
            fields: &[],
        }
    }

    fn sql(dialect: Dialect, query: &str, poll: &Poll<'_>) -> Statement {
        statement(&Query::parse(query, dialect).unwrap(), poll)
    }

    #[test]
    fn run_before_a_position_is_the_query_limited() {
        let none = position(&[]);
        let run = Poll {
            position: &none,
            after: false,
            start: false,
            settle_secs: None,
            batch: 20,
            fields: &[],
        };
        assert_eq!(
            sql(Dialect::Postgres, EVENTS, &run).sql,
            format!("SELECT * FROM (\n{EVENTS}\n) AS q\nLIMIT 20")
        );
        assert_eq!(
            sql(Dialect::MySql, EVENTS, &run).sql,
            format!("SELECT * FROM (\n{EVENTS}\n) AS q\nLIMIT 20")
        );
        assert_eq!(
            sql(Dialect::SqlServer, EVENTS, &run).sql,
            format!("SELECT TOP (20) * FROM (\n{EVENTS}\n) AS q")
        );
        assert!(sql(Dialect::Postgres, EVENTS, &run).binds.is_empty());
    }

    #[test]
    fn the_first_poll_from_the_beginning_orders_and_waits_but_tests_no_position() {
        let p = position(&["occurred_at", "event_id"]);
        let s = sql(Dialect::Postgres, EVENTS, &poll(&p, false, Some(30)));
        assert_eq!(
            s.sql,
            format!(
                "SELECT * FROM (\n{EVENTS}\n) AS q\nWHERE q.\"occurred_at\" <= now() - interval '30 seconds'\n\
                 ORDER BY q.\"occurred_at\", q.\"event_id\"\nLIMIT 5000"
            )
        );
        assert!(s.binds.is_empty());
    }

    #[test]
    fn the_first_poll_from_a_start_reads_the_first_column_at_or_past_it() {
        let p = position(&["occurred_at", "event_id"]);
        let mut from = poll(&p, false, Some(30));
        from.start = true;
        let s = sql(Dialect::Postgres, EVENTS, &from);
        assert_eq!(
            s.sql,
            format!(
                "SELECT * FROM (\n{EVENTS}\n) AS q\n\
                 WHERE q.\"occurred_at\" >= $1\n  AND q.\"occurred_at\" <= now() - interval '30 seconds'\n\
                 ORDER BY q.\"occurred_at\", q.\"event_id\"\nLIMIT 5000"
            )
        );
        assert_eq!(s.binds, [Bind::Start]);
        let s = sql(Dialect::SqlServer, EVENTS, &from);
        assert!(s.sql.contains("WHERE q.[occurred_at] >= @P1\n"), "{}", s.sql);
        // Once a row is sent, the poll reads past it, wherever the source started.
        from.after = true;
        assert_eq!(
            sql(Dialect::Postgres, EVENTS, &from).binds,
            [Bind::Position(0), Bind::Position(1)]
        );
    }

    #[test]
    fn the_maximum_reads_one_column_of_the_query_s_result() {
        let q = Query::parse(EVENTS, Dialect::Postgres).unwrap();
        assert_eq!(
            maximum(&q, "occurred_at", None, &[]).sql,
            format!("SELECT max(q.\"occurred_at\") FROM (\n{EVENTS}\n) AS q")
        );
        assert_eq!(
            maximum(&q, "occurred_at", Some(30), &[]).sql,
            format!(
                "SELECT max(q.\"occurred_at\") FROM (\n{EVENTS}\n) AS q\n\
                 WHERE q.\"occurred_at\" <= now() - interval '30 seconds'"
            ),
            "a row a late commit may still join does not move the start"
        );
        let q = Query::parse("SELECT * FROM t WHERE x > :last.x", Dialect::MySql).unwrap();
        let s = maximum(&q, "x", None, &[]);
        assert_eq!(s.sql, "SELECT max(q.`x`) FROM (\nSELECT * FROM t WHERE x > ?\n) AS q");
        assert_eq!(s.binds, [Bind::Last("x".into())]);
    }

    #[test]
    fn postgres_compares_the_position_as_a_row() {
        let p = position(&["occurred_at", "event_id"]);
        let s = sql(Dialect::Postgres, EVENTS, &poll(&p, true, Some(30)));
        assert_eq!(
            s.sql,
            format!(
                "SELECT * FROM (\n{EVENTS}\n) AS q\n\
                 WHERE (q.\"occurred_at\", q.\"event_id\") > ($1, $2)\n  AND q.\"occurred_at\" <= now() - interval '30 seconds'\n\
                 ORDER BY q.\"occurred_at\", q.\"event_id\"\nLIMIT 5000"
            )
        );
        assert_eq!(s.binds, [Bind::Position(0), Bind::Position(1)]);
    }

    #[test]
    fn sql_server_spells_the_position_out_and_numbers_each_value_once() {
        let p = position(&["occurred_at", "event_id"]);
        let s = sql(Dialect::SqlServer, EVENTS, &poll(&p, true, Some(30)));
        assert_eq!(
            s.sql,
            format!(
                "SELECT TOP (5000) * FROM (\n{EVENTS}\n) AS q\n\
                 WHERE (q.[occurred_at] > @P1 OR (q.[occurred_at] = @P1 AND q.[event_id] > @P2))\n  \
                 AND q.[occurred_at] <= DATEADD(second, -30, SYSUTCDATETIME())\n\
                 ORDER BY q.[occurred_at], q.[event_id]"
            )
        );
        assert_eq!(s.binds, [Bind::Position(0), Bind::Position(1)]);
    }

    #[test]
    fn mysql_binds_a_value_once_for_each_place_it_stands() {
        let p = position(&["recorded_at", "device_id", "metric"]);
        let s = sql(Dialect::MySql, EVENTS, &poll(&p, true, None));
        assert_eq!(
            s.sql,
            format!(
                "SELECT * FROM (\n{EVENTS}\n) AS q\n\
                 WHERE (q.`recorded_at` > ? OR (q.`recorded_at` = ? AND (q.`device_id` > ? OR (q.`device_id` = ? AND q.`metric` > ?))))\n\
                 ORDER BY q.`recorded_at`, q.`device_id`, q.`metric`\nLIMIT 5000"
            )
        );
        assert_eq!(
            s.binds,
            [
                Bind::Position(0),
                Bind::Position(0),
                Bind::Position(1),
                Bind::Position(1),
                Bind::Position(2)
            ]
        );
    }

    #[test]
    fn a_unique_position_is_one_comparison() {
        let p = position(&["audit_id"]);
        assert!(
            sql(Dialect::Postgres, EVENTS, &poll(&p, true, None))
                .sql
                .contains("WHERE q.\"audit_id\" > $1\n")
        );
        assert!(
            sql(Dialect::SqlServer, EVENTS, &poll(&p, true, None))
                .sql
                .contains("WHERE q.[audit_id] > @P1\n")
        );
        assert!(
            sql(Dialect::MySql, EVENTS, &poll(&p, true, None))
                .sql
                .contains("WHERE q.`audit_id` > ?\n")
        );
    }

    #[test]
    fn a_name_is_quoted_with_its_own_quotes_doubled() {
        assert_eq!(quote(Dialect::Postgres, "a\"b"), "\"a\"\"b\"");
        assert_eq!(quote(Dialect::SqlServer, "a]b"), "[a]]b]");
        assert_eq!(quote(Dialect::MySql, "a`b"), "`a``b`");
    }

    #[test]
    fn last_is_bound_where_the_query_names_it_and_the_position_is_still_read_past_outside() {
        let query = "SELECT date_trunc('hour', occurred_at) AS hour, count(*) AS n FROM auth.events\n\
                     WHERE occurred_at >= :last.hour + interval '1 hour' AND batch > :last.batch GROUP BY 1";
        let p = position(&["hour"]);
        let s = sql(Dialect::Postgres, query, &poll(&p, true, Some(3600)));
        assert_eq!(
            s.sql,
            "SELECT * FROM (\nSELECT date_trunc('hour', occurred_at) AS hour, count(*) AS n FROM auth.events\n\
             WHERE occurred_at >= $1 + interval '1 hour' AND batch > $2 GROUP BY 1\n) AS q\n\
             WHERE q.\"hour\" > $3\n  AND q.\"hour\" <= now() - interval '3600 seconds'\n\
             ORDER BY q.\"hour\"\nLIMIT 5000"
        );
        assert_eq!(
            s.binds,
            [Bind::Last("hour".into()), Bind::Last("batch".into()), Bind::Position(0)]
        );
    }

    #[test]
    fn last_used_twice_is_bound_once_where_placeholders_are_numbered() {
        let query = "SELECT * FROM t WHERE (:last.x IS NULL OR x > :last.x)";
        let none = position(&[]);
        let run = poll(&none, false, None);
        assert_eq!(sql(Dialect::Postgres, query, &run).binds, [Bind::Last("x".into())]);
        assert!(
            sql(Dialect::Postgres, query, &run)
                .sql
                .contains("($1 IS NULL OR x > $1)")
        );
        assert_eq!(
            sql(Dialect::MySql, query, &run).binds,
            [Bind::Last("x".into()), Bind::Last("x".into())]
        );
        assert_eq!(Query::parse(query, Dialect::Postgres).unwrap().last_columns(), ["x"]);
    }

    /// PostgreSQL refuses `$1 IS NULL` alone: it cannot tell the placeholder's type.
    #[test]
    fn last_takes_its_fields_type_on_postgres() {
        let query = "SELECT * FROM t WHERE (:last.at IS NULL OR at >= :last.at) AND :last.tags IS NOT NULL";
        let none = position(&[]);
        let fields = [
            ("at".to_string(), DataType::DateTime),
            ("tags".to_string(), DataType::Str),
        ];
        let run = Poll {
            fields: &fields,
            ..poll(&none, false, None)
        };
        assert!(
            sql(Dialect::Postgres, query, &run)
                .sql
                .contains("(CAST($1 AS timestamptz) IS NULL OR at >= CAST($1 AS timestamptz)) AND $2 IS NOT NULL")
        );
        assert!(sql(Dialect::MySql, query, &run).sql.contains("(? IS NULL OR at >= ?)"));
        let q = Query::parse(query, Dialect::Postgres).unwrap();
        assert!(
            maximum(&q, "at", None, &fields)
                .sql
                .contains("CAST($1 AS timestamptz) IS NULL")
        );
    }

    #[test]
    fn last_in_a_string_or_comment_is_text_and_a_cast_is_not_last() {
        let q = Query::parse(
            "SELECT ':last.a' AS s, x::last.b -- :last.c\nFROM t /* :last.d */",
            Dialect::Postgres,
        );
        assert!(q.unwrap().last_columns().is_empty());
        assert_eq!(
            Query::parse("SELECT * FROM t WHERE x > :last.", Dialect::Postgres),
            Err(Refused::LastWithoutColumn)
        );
        assert_eq!(
            Query::parse("SELECT * FROM t WHERE x > :last.1", Dialect::Postgres),
            Err(Refused::LastWithoutColumn)
        );
    }

    #[test]
    fn only_a_select_or_a_with_runs() {
        assert!(Query::parse("select 1", Dialect::Postgres).is_ok());
        assert!(
            Query::parse(
                "  -- the last hour\n WITH x AS (SELECT 1) SELECT * FROM x",
                Dialect::Postgres
            )
            .is_ok()
        );
        assert_eq!(
            Query::parse("UPDATE t SET a = 1", Dialect::Postgres),
            Err(Refused::NotARead("UPDATE".into()))
        );
        assert_eq!(
            Query::parse("(SELECT 1)", Dialect::Postgres),
            Err(Refused::NotARead("(".into()))
        );
        assert_eq!(Query::parse("  -- nothing\n", Dialect::Postgres), Err(Refused::Empty));
    }

    #[test]
    fn a_final_semicolon_and_what_follows_it_are_dropped() {
        let s = sql(Dialect::Postgres, "SELECT 1 ; -- done\n", &poll(&[], false, None));
        assert_eq!(s.sql, "SELECT * FROM (\nSELECT 1\n) AS q\nLIMIT 5000");
        assert_eq!(
            Query::parse("SELECT 1; DELETE FROM t", Dialect::Postgres),
            Err(Refused::MoreThanOneStatement)
        );
        assert!(Query::parse("SELECT ';' AS s", Dialect::Postgres).is_ok());
    }

    #[test]
    fn a_comment_ending_the_query_cannot_swallow_the_statement() {
        let s = sql(Dialect::Postgres, "SELECT 1 -- trailing", &poll(&[], false, None));
        assert_eq!(s.sql, "SELECT * FROM (\nSELECT 1 -- trailing\n) AS q\nLIMIT 5000");
    }

    #[test]
    fn the_query_may_not_order_or_limit_itself() {
        let refused = |q: &str, d: Dialect| Query::parse(q, d).err();
        assert_eq!(
            refused("SELECT a FROM t ORDER BY a", Dialect::Postgres),
            Some(Refused::Ordered)
        );
        assert_eq!(
            refused("SELECT a FROM t UNION SELECT a FROM u order by a", Dialect::MySql),
            Some(Refused::Ordered)
        );
        assert_eq!(
            refused("SELECT a FROM t LIMIT 10", Dialect::Postgres),
            Some(Refused::Limited("LIMIT".into()))
        );
        assert_eq!(
            refused("SELECT a FROM t OFFSET 5 ROWS", Dialect::SqlServer),
            Some(Refused::Limited("OFFSET".into()))
        );
        assert_eq!(
            refused("SELECT a FROM t FETCH FIRST 5 ROWS ONLY", Dialect::Postgres),
            Some(Refused::Limited("FETCH".into()))
        );
        assert_eq!(
            refused("SELECT TOP 10 a FROM t", Dialect::SqlServer),
            Some(Refused::Limited("TOP".into()))
        );
        assert_eq!(
            refused("SELECT DISTINCT TOP (10) a FROM t", Dialect::SqlServer),
            Some(Refused::Limited("TOP".into()))
        );
    }

    #[test]
    fn an_order_or_limit_inside_parentheses_belongs_to_something_else() {
        for q in [
            "SELECT a, row_number() OVER (ORDER BY a) FROM t",
            "SELECT string_agg(a, ',' ORDER BY a) FROM t",
            "WITH recent AS (SELECT * FROM t ORDER BY a LIMIT 10) SELECT * FROM recent",
            "SELECT * FROM t WHERE a IN (SELECT a FROM u ORDER BY a LIMIT 5)",
            "SELECT 'ORDER BY a LIMIT 1' AS s FROM t -- ORDER BY a",
        ] {
            assert!(Query::parse(q, Dialect::Postgres).is_ok(), "{q}");
        }
    }

    #[test]
    fn a_column_named_like_a_limit_is_a_column() {
        assert!(Query::parse("SELECT offset, partition FROM kafka_log", Dialect::MySql).is_ok());
        assert!(Query::parse("SELECT top FROM charts", Dialect::Postgres).is_ok());
        assert!(Query::parse("SELECT fetch_time FROM t", Dialect::SqlServer).is_ok());
    }

    #[test]
    fn a_placeholder_of_the_users_own_is_refused() {
        assert_eq!(
            Query::parse("SELECT * FROM t WHERE a > $1", Dialect::Postgres),
            Err(Refused::Placeholder("$1".into()))
        );
        assert_eq!(
            Query::parse("SELECT * FROM t WHERE a > @p1", Dialect::SqlServer),
            Err(Refused::Placeholder("@p1".into()))
        );
        assert_eq!(
            Query::parse("SELECT * FROM t WHERE a > ?", Dialect::MySql),
            Err(Refused::Placeholder("?".into()))
        );
        // PostgreSQL's `?` is a jsonb operator, and SQL Server's `@p` names a variable only with digits after it.
        assert!(Query::parse("SELECT * FROM t WHERE doc ? 'key'", Dialect::Postgres).is_ok());
        assert!(Query::parse("SELECT @@version AS v", Dialect::SqlServer).is_ok());
    }

    #[test]
    fn each_dialects_strings_and_names_are_not_code() {
        assert!(Query::parse("SELECT 'it''s; ORDER BY' AS s", Dialect::Postgres).is_ok());
        assert!(Query::parse(r"SELECT 'it\'s; ORDER BY' AS s", Dialect::MySql).is_ok());
        assert!(Query::parse("SELECT `order by`, \"limit\" FROM t # ORDER BY x", Dialect::MySql).is_ok());
        assert!(Query::parse("SELECT [order by]]x] FROM t", Dialect::SqlServer).is_ok());
        assert!(Query::parse("SELECT $body$ ; ORDER BY $body$ AS s", Dialect::Postgres).is_ok());
        assert!(Query::parse("SELECT 1 /* outer /* inner */ ORDER BY */ AS s", Dialect::Postgres).is_ok());
    }

    #[test]
    fn something_never_closed_is_refused_by_name() {
        assert_eq!(
            Query::parse("SELECT 'open", Dialect::Postgres),
            Err(Refused::Unterminated("string"))
        );
        assert_eq!(
            Query::parse("SELECT \"open", Dialect::Postgres),
            Err(Refused::Unterminated("quoted name"))
        );
        assert_eq!(
            Query::parse("SELECT 1 /* open", Dialect::Postgres),
            Err(Refused::Unterminated("comment"))
        );
        assert_eq!(
            Query::parse("SELECT [open", Dialect::SqlServer),
            Err(Refused::Unterminated("quoted name"))
        );
        assert_eq!(
            Query::parse("SELECT $x$ open", Dialect::Postgres),
            Err(Refused::Unterminated("string"))
        );
    }

    #[test]
    fn a_string_with_escapes_hides_no_code() {
        // Each of these reads as one string to its database; a scanner that ends it early would see a LIMIT.
        assert!(Query::parse(r"SELECT E'\' LIMIT 1 ' AS s", Dialect::Postgres).is_ok());
        assert!(Query::parse(r#"SELECT "\" LIMIT 1 " AS s"#, Dialect::MySql).is_ok());
        // Without the E, a PostgreSQL backslash is a backslash and the string ends after it.
        assert_eq!(
            Query::parse(r"SELECT '\' AS a FROM t LIMIT 1", Dialect::Postgres),
            Err(Refused::Limited("LIMIT".into()))
        );
        // An `e` ending a name does not make an E string.
        assert!(Query::parse(r"SELECT name' LIMIT 1 '", Dialect::Postgres).is_ok());
    }

    #[test]
    fn a_comment_ends_where_its_database_ends_it() {
        // SQL Server nests block comments, as PostgreSQL does; MySQL does not.
        assert!(Query::parse("SELECT 1 AS a /* /* */ LIMIT 1 */", Dialect::SqlServer).is_ok());
        assert_eq!(
            Query::parse("SELECT 1 AS a /* /* */ FROM t LIMIT 1", Dialect::MySql),
            Err(Refused::Limited("LIMIT".into()))
        );
        // MySQL's `--` is a comment only before white space.
        assert_eq!(
            Query::parse("SELECT a--1 AS x FROM t LIMIT 10", Dialect::MySql),
            Err(Refused::Limited("LIMIT".into()))
        );
        assert!(Query::parse("SELECT a -- LIMIT 10", Dialect::MySql).is_ok());
        assert_eq!(
            Query::parse("SELECT 1 /*! AS a FROM t */", Dialect::MySql),
            Err(Refused::ExecutableComment)
        );
    }

    #[test]
    fn mariadbs_executable_comment_is_refused_too() {
        assert_eq!(
            Query::parse("SELECT * FROM t /*M! ORDER BY id LIMIT 1 */", Dialect::MySql),
            Err(Refused::ExecutableComment)
        );
    }

    #[test]
    fn a_line_comment_ends_at_a_carriage_return() {
        assert_eq!(
            Query::parse("SELECT a FROM t -- note\r ORDER BY a", Dialect::SqlServer),
            Err(Refused::Ordered)
        );
    }

    #[test]
    fn limit_is_a_name_in_sql_server() {
        assert!(Query::parse("SELECT quota AS limit FROM quotas", Dialect::SqlServer).is_ok());
    }

    #[test]
    fn an_offset_by_an_expression_is_refused() {
        assert_eq!(
            Query::parse("SELECT a FROM t OFFSET (10)", Dialect::Postgres),
            Err(Refused::Limited("OFFSET".into()))
        );
    }

    #[test]
    fn a_dollar_inside_a_name_opens_nothing() {
        assert_eq!(
            Query::parse("SELECT x$y$ AS a FROM t LIMIT 5 -- $y$", Dialect::Postgres),
            Err(Refused::Limited("LIMIT".into()))
        );
        assert!(Query::parse("SELECT a$1 FROM t", Dialect::Postgres).is_ok());
    }

    #[test]
    fn parentheses_must_pair() {
        assert_eq!(
            Query::parse("SELECT 1 AS a) AS z, (SELECT 2 AS b", Dialect::Postgres),
            Err(Refused::Unbalanced)
        );
        assert_eq!(
            Query::parse("SELECT (1 AS a", Dialect::Postgres),
            Err(Refused::Unbalanced)
        );
    }

    #[test]
    fn a_name_outside_ascii_is_one_name() {
        let q = Query::parse("SELECT * FROM t WHERE größe > :last.größe", Dialect::Postgres).unwrap();
        assert_eq!(q.last_columns(), ["größe"]);
        assert!(Query::parse("SELECT a AS limité FROM t", Dialect::Postgres).is_ok());
        assert!(Query::parse("SELECT $é$ LIMIT 1 $é$ AS s", Dialect::Postgres).is_ok());
    }

    #[test]
    fn text_outside_ascii_is_carried_whole() {
        let s = sql(
            Dialect::Postgres,
            "SELECT 'café' AS name, größe FROM t",
            &poll(&[], false, None),
        );
        assert_eq!(
            s.sql,
            "SELECT * FROM (\nSELECT 'café' AS name, größe FROM t\n) AS q\nLIMIT 5000"
        );
    }
}
