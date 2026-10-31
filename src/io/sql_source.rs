//! An SQL source: a query run on a timer, its rows handed on as typed records, and the place it reached kept.
//!
//! ```text
//!   every poll:  statement(query, mode, cursor) ──► Driver::run ──► rows ──► a read per hand-out ──► records
//!                         ▲                                                          │
//!                         └──────── the last row's position and :last values ◄───────┘
//!
//!   a read every destination acknowledged, and every read before it ──► progress: cursor, or ledger entries
//! ```
//!
//! A [`Driver`] speaks one database: it runs a statement read-only and returns its rows as [`Value`]s. This engine
//! owns the rest, the same for every database: the mode, what it remembers between polls, the record each row
//! becomes, and when progress moves. Each hand-out is a partition of its own, as an API source's page is, so it is
//! done once its last record commits, and progress is what the last done hand-out reached.
//!
//! A statement runs on a thread of its own, which holds the driver until it answers, so a poll that outlasts the
//! reader's timeout leaves the reader free and is picked up by a later one.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use super::pacer::Pacer;
use super::reads::Reads;
use super::sql::{Bind, Dialect, Poll, Query, Statement};
use super::values::{Value, column, spell, stored, unbuilt};
use super::{CommitMap, IoError, Progress, RecordChunk, Source, SourceRecord};
use crate::core::column::DataType;
use crate::heartbeat::{LedgerEntry, MAX_CURSOR, MAX_LEDGER_ENTRY, MAX_LEDGER_ITEM};

/// The most rows comparing values reads in one poll. Each is a ledger entry the leader keeps.
pub const MAX_COMPARED_ROWS: u32 = 1_000_000;

/// The most a poll's result may hold, by [`held`]. The whole result is held while it is handed out, so this is what a
/// source holds beyond its lane.
pub const MAX_RESULT_BYTES: usize = 256 * 1024 * 1024;

/// About the bytes `row` takes in memory: its own allocation, and each value's. A narrow cell costs more held than its
/// eight bytes of record.
pub fn held(row: &[Value]) -> usize {
    std::mem::size_of::<Vec<Value>>() + row.iter().map(Value::held).sum::<usize>()
}

/// Why a driver stops reading a result past [`MAX_RESULT_BYTES`].
pub fn too_large() -> String {
    format!(
        "this poll's result holds more than {} MiB; read fewer rows a poll with a smaller batch, or fewer or \
         narrower fields",
        MAX_RESULT_BYTES / (1024 * 1024)
    )
}

/// A result: its columns' names, and each row's values in that order.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    /// The columns of a type the driver cannot read, read as their text. Their values cannot be bound back as
    /// their own type, so none can be remembered.
    pub as_text: Vec<String>,
}

/// One column of a result, as the database describes it without running the query.
#[derive(Clone, Debug, PartialEq)]
pub struct Described {
    pub name: String,
    /// The database's own name for the column's type.
    pub database_type: String,
    /// What the column is declared as by default; `None` for a type a record cannot hold yet.
    pub suggested: Option<DataType>,
    /// The table the column is read from, when it is a plain column of one.
    pub table: Option<String>,
    /// Whether that table has a unique index or primary key on this column alone.
    pub unique: bool,
}

/// A read of a whole table estimated past this many rows is a full scan Run reports. Below it, reading the whole table
/// each poll costs about what an index lookup would; above it, every poll re-reads a table that grows, a load on a
/// database smeltr does not own.
pub const FULL_SCAN_ROWS: u64 = 100_000;

/// What the database plans for a poll's statement.
#[derive(Clone, Debug, PartialEq)]
pub enum Verdict {
    /// Every relation is reached through an index, or is small. `indexes` names each index the plan reads through, in
    /// plan order, once; `rows` is the database's estimate of the rows the poll returns.
    Indexed { indexes: Vec<String>, rows: u64 },
    /// The poll reads the whole of `relation`, which the database estimates at `rows` rows.
    FullScan { relation: String, rows: u64 },
}

/// Whether a database connection is encrypted. `Require` also verifies the server's certificate and name: rustls has
/// no mode that encrypts without verifying.
#[derive(Clone, Debug)]
pub enum Encrypted {
    Disable,
    Require,
}

/// One database's half of an SQL source.
pub trait Driver: Send {
    fn dialect(&self) -> Dialect;

    /// Runs `statement` in a read-only transaction, `values[n]` bound to placeholder `n + 1`. A connection that
    /// broke is opened again here. Err is the database's own message, or [`too_large`] for a result past
    /// [`MAX_RESULT_BYTES`].
    fn run(&mut self, statement: &Statement, values: &[Value]) -> Result<Rows, String>;

    /// The columns `statement`'s result has, without running it.
    fn describe(&mut self, statement: &Statement) -> Result<Vec<Described>, String>;

    /// What the database plans for `statement`, `values` bound.
    fn plan(&mut self, statement: &Statement, values: &[Value]) -> Result<Verdict, String>;
}

/// The longest a driver waits for the server to accept its connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// The longest a [`Shell`] waits on one open, and on one call's work. A poll's statement is bounded by its batch, so
/// work past this is a database that has stopped answering, and the connection is dropped rather than waited on.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(300);
/// How much sooner than [`CALL_TIMEOUT`] the server stops a statement.
pub const STATEMENT_MARGIN: Duration = Duration::from_secs(10);

/// One call's work on a driver's connection.
pub type Work<'c, T> = Pin<Box<dyn Future<Output = Result<T, Failure>> + 'c>>;

/// Why a call's work failed, and whether the connection went with it.
pub struct Failure {
    pub text: String,
    /// The connection is gone, or in a state not worth finding out: the next call opens another.
    pub broken: bool,
    /// It may have been gone before this call reached the server, so the call can be made again on a new one.
    pub found_closed: bool,
}

impl From<String> for Failure {
    fn from(text: String) -> Failure {
        Failure {
            text,
            broken: false,
            found_closed: false,
        }
    }
}

/// One database's connection, as a [`Shell`] opens and watches it.
pub trait Connection: Sized {
    /// The database as a message names it: "PostgreSQL".
    const NAME: &'static str;
    /// What `open` takes, with every secret resolved, kept to open again after a break.
    type Opening: Clone;

    fn open(opening: Self::Opening) -> impl Future<Output = Result<Self, Failure>>;

    /// Known closed while idle, so the next call opens another without trying it. False where the client cannot
    /// tell.
    fn closed(&self) -> bool {
        false
    }
}

/// A driver's connection and the runtime it runs on. The client libraries are async, so the shell owns a small
/// runtime and waits on it. The connection is opened on first use and opened again on the call after it breaks.
pub struct Shell<C: Connection> {
    opening: C::Opening,
    runtime: tokio::runtime::Runtime,
    connection: Option<C>,
}

impl<C: Connection> Shell<C> {
    /// Connects lazily, on the first call.
    pub fn new(opening: C::Opening) -> Result<Shell<C>, IoError> {
        // Current-thread is enough: every call is a `block_on`, and the connection is driven while one runs.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| IoError::transient(format!("{}: cannot start a runtime: {e}", C::NAME)))?;
        Ok(Shell {
            opening,
            runtime,
            connection: None,
        })
    }

    /// Runs `work` on the connection, opening it first when there is none or it broke. A connection that closed
    /// while idle is only found closed when used, so `work` that finds it so runs once more on a new one; every
    /// statement a driver runs is read-only, so running it again changes nothing.
    pub fn call<T>(&mut self, work: impl for<'c> Fn(&'c mut C) -> Work<'c, T>) -> Result<T, String> {
        let reused = self.connection.as_ref().is_some_and(|c| !c.closed());
        match self.attempt(&work) {
            Err(failure) if reused && failure.found_closed => self.attempt(&work),
            result => result,
        }
        .map_err(|failure| failure.text)
    }

    fn attempt<T>(&mut self, work: &impl for<'c> Fn(&'c mut C) -> Work<'c, T>) -> Result<T, Failure> {
        if self.connection.as_ref().is_none_or(C::closed) {
            self.connection = None;
            let opening = self.opening.clone();
            let connection = self
                .runtime
                .block_on(async { tokio::time::timeout(CALL_TIMEOUT, C::open(opening)).await })
                .map_err(|_| unanswered::<C>())??;
            self.connection = Some(connection);
        }
        let connection = self.connection.as_mut().expect("connected above");
        // The timer is made inside `block_on`, where the runtime's clock is.
        match self
            .runtime
            .block_on(async { tokio::time::timeout(CALL_TIMEOUT, work(connection)).await })
        {
            Ok(Err(failure)) if failure.broken => {
                self.connection = None;
                Err(failure)
            }
            Ok(result) => result,
            Err(_) => {
                // The call stopped mid-way, perhaps inside a transaction; the connection's state is unknown.
                self.connection = None;
                Err(unanswered::<C>().into())
            }
        }
    }
}

fn unanswered<C: Connection>() -> String {
    format!("{} did not answer within {CALL_TIMEOUT:?}", C::NAME)
}

/// How a source knows a row is due, cheapest first.
#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    /// Rows past the last one sent, by a column that only grows as rows are added.
    Incrementing(Position),
    /// Rows past the last one sent, by a time every insert and update sets.
    UpdatedAt(Position),
    /// Every row each poll, compared with the last poll by `key`: a row is sent when it is new or its values changed.
    Comparing { key: Vec<String> },
    /// Every row each poll, sent whole.
    EveryPoll,
}

/// The columns that put the result in a fixed order, how long to wait for late commits, and where to begin.
#[derive(Clone, Debug, PartialEq)]
pub struct Position {
    /// The ordering column, then the columns that tell apart rows with the same value in it.
    pub columns: Vec<String>,
    /// Only for a position whose first column is a time.
    pub settle_secs: Option<u32>,
    /// Where a source with no progress begins. A source with progress resumes from it whatever this says.
    pub start: Start,
}

/// Where a position mode with no progress begins, in the position's first column.
#[derive(Clone, Debug, PartialEq)]
pub enum Start {
    /// Every row the query returns.
    Beginning,
    /// At the largest value the query returns on the first poll: rows already there are not read, but for those
    /// sharing that value.
    Now,
    /// At this value, of the first column's type.
    From(Value),
}

/// The key a stored cursor holds where a source began, before it sent a row. No field takes it: a field's name
/// cannot begin with `__`.
const FROM: &str = "__from";

/// What an SQL source reads, apart from its connection.
#[derive(Clone, Debug)]
pub struct SqlSourceConfig {
    pub query: Query,
    pub mode: Mode,
    /// Each field of the record, matched to a result column by name.
    pub fields: Arc<[(String, DataType)]>,
    pub poll_interval: Duration,
    /// The most rows one poll reads in the position modes.
    pub batch: u32,
    /// What each record's topic names: the source's place.
    pub place: Arc<str>,
}

impl SqlSourceConfig {
    /// The statement a poll runs: past the last row sent when `after`, else from the value the source starts from
    /// when `start`, else from the beginning. Only a position mode reads past a row or from a start.
    pub fn statement(&self, after: bool, start: bool) -> Statement {
        self.statement_reading(after, start, false)
    }

    /// [`Self::statement`], reading twice the batch when `wide`.
    fn statement_reading(&self, after: bool, start: bool, wide: bool) -> Statement {
        let (settle_secs, batch) = match &self.mode {
            Mode::Incrementing(p) | Mode::UpdatedAt(p) => (p.settle_secs, self.batch * if wide { 2 } else { 1 }),
            // One more than the most a poll may read, so a result over the limit is seen to be.
            Mode::Comparing { .. } => (None, MAX_COMPARED_ROWS + 1),
            Mode::EveryPoll => (None, self.batch + 1),
        };
        super::sql::statement(
            &self.query,
            &Poll {
                position: self.position(),
                after,
                start,
                settle_secs,
                batch,
                fields: &self.fields,
            },
        )
    }

    /// The columns that order the result, empty for a mode with no position.
    pub fn position(&self) -> &[String] {
        match &self.mode {
            Mode::Incrementing(p) | Mode::UpdatedAt(p) => &p.columns,
            Mode::Comparing { .. } | Mode::EveryPoll => &[],
        }
    }
}

/// The cursor a position mode keeps: each remembered column's value in the last row sent.
type Cursor = BTreeMap<String, Value>;

/// Rows read and not yet handed out, and what handing out the last of them moves progress to.
struct Pending {
    rows: VecDeque<Vec<Value>>,
    /// Each pending row's ledger entry, when comparing values.
    entries: VecDeque<LedgerEntry>,
    /// Where the position stands after the last pending row.
    cursor: Option<String>,
    /// Keys this poll found gone, sent with its last hand-out.
    removed: Vec<String>,
    /// What `rows` hold, by [`held`].
    held: usize,
}

/// What a read moves progress to once it is committed whole: where the position stands after it, and what comparing
/// values found added and gone.
struct Reached {
    cursor: Option<String>,
    added: Vec<LedgerEntry>,
    removed: Vec<String>,
}

pub struct SqlSource<D: Driver> {
    /// `None` while a statement runs; it comes back with the answer.
    driver: Option<D>,
    running: Option<Receiver<(D, Result<Rows, String>)>>,
    config: SqlSourceConfig,
    /// The result column each field is read from, found on the first result.
    columns: Option<Vec<usize>>,
    /// Where the next poll reads past.
    cursor: Option<Cursor>,
    /// Where a position mode begins before its first row: `None` for the beginning. Found by the first poll when the
    /// source starts now, and kept as progress at once, so a source moved before its first row begins there too.
    from: Option<Value>,
    /// Whether `from` is known.
    from_known: bool,
    /// The running statement finds where the source begins.
    finding_from: bool,
    /// What comparing values last saw: each key, and the hash of its row.
    ledger: HashMap<String, String>,
    pending: Option<Pending>,
    /// Reads not yet committed whole, each carrying what it reached.
    reads: Reads<Reached>,
    progress: Option<Progress>,
    next_poll: Instant,
    blocked: bool,
    /// When the source may ask again after a failure, and the failure to show.
    pacer: Pacer,
    warned: HashSet<String>,
    /// How many polls have answered.
    answered: u64,
    /// The next poll reads twice the batch: rows sharing one value of a position with no tie-break filled the last.
    wide: bool,
}

impl<D: Driver + 'static> SqlSource<D> {
    pub fn new(driver: D, config: SqlSourceConfig) -> SqlSource<D> {
        let (from, from_known) = match &config.mode {
            Mode::Incrementing(p) | Mode::UpdatedAt(p) => match &p.start {
                Start::Beginning => (None, true),
                Start::Now => (None, false),
                Start::From(value) => (Some(value.clone()), true),
            },
            Mode::Comparing { .. } | Mode::EveryPoll => (None, true),
        };
        SqlSource {
            driver: Some(driver),
            running: None,
            config,
            columns: None,
            cursor: None,
            from,
            from_known,
            finding_from: false,
            ledger: HashMap::new(),
            pending: None,
            reads: Reads::default(),
            progress: None,
            next_poll: Instant::now(),
            blocked: false,
            pacer: Pacer::new("SQL"),
            warned: HashSet::new(),
            answered: 0,
            wide: false,
        }
    }

    /// Whether a poll has answered and every row it read is handed out: what Run waits for, since it reads once.
    pub fn read_once(&self) -> bool {
        self.answered > 0 && self.pending.is_none()
    }

    /// Reads at most `rows` a poll from here on, in the position modes. Run reads fewer than a poll does.
    pub fn read_at_most(&mut self, rows: u32) {
        self.config.batch = rows;
    }

    /// The statement this source's next poll runs, and the values it binds. Run shows exactly this.
    pub fn next_statement(&self) -> (Statement, Vec<Value>) {
        let statement = self.config.statement_reading(
            self.cursor.is_some(),
            self.cursor.is_none() && self.from.is_some(),
            self.wide,
        );
        let values = self.values(&statement.binds, self.config.position().first());
        (statement, values)
    }

    /// What the database plans for the next poll, judged. `None` while a statement is running, since the driver is
    /// on its thread.
    pub fn plan(&mut self) -> Option<Result<Verdict, String>> {
        let (statement, values) = self.next_statement();
        self.driver.as_mut().map(|driver| driver.plan(&statement, &values))
    }

    /// The value each of `binds` takes. Before the first row, a `:last` of the position's first column is where the
    /// source begins.
    fn values(&self, binds: &[Bind], first: Option<&String>) -> Vec<Value> {
        let position = match &self.config.mode {
            Mode::Incrementing(p) | Mode::UpdatedAt(p) => &p.columns[..],
            Mode::Comparing { .. } | Mode::EveryPoll => &[],
        };
        binds
            .iter()
            .map(|bind| {
                let name = match bind {
                    Bind::Start => return self.from.clone().unwrap_or(Value::Null),
                    Bind::Position(n) => &position[*n],
                    Bind::Last(name) => name,
                };
                match &self.cursor {
                    Some(cursor) => cursor.get(name).cloned().unwrap_or(Value::Null),
                    None if Some(name) == first => self.from.clone().unwrap_or(Value::Null),
                    None => Value::Null,
                }
            })
            .collect()
    }

    /// The statement that finds where a source starting now begins: the largest value of its position's first
    /// column. `None` once that is known, or once a row was sent.
    fn finding_statement(&self) -> Option<(Statement, Vec<Value>)> {
        if self.from_known || self.cursor.is_some() {
            return None;
        }
        let (Mode::Incrementing(p) | Mode::UpdatedAt(p)) = &self.config.mode else {
            return None;
        };
        let statement = super::sql::maximum(&self.config.query, &p.columns[0], p.settle_secs, &self.config.fields);
        let values = self.values(&statement.binds, None);
        Some((statement, values))
    }

    /// Keeps where the source begins, from the largest value `result` holds, and keeps it as progress at once.
    fn found_from(&mut self, result: Result<Rows, String>) -> Result<(), String> {
        let result = result?;
        if !result.as_text.is_empty() {
            return Err(
                "the position's first column is of a type smeltr reads only as text; cast it in the query \
                        to a type smeltr reads"
                    .into(),
            );
        }
        let value = (result.rows.first().and_then(|row| row.first()).cloned()).unwrap_or(Value::Null);
        let cursor = stored_cursor([(FROM, &value)]);
        self.from = (value != Value::Null).then_some(value);
        self.from_known = true;
        self.progress.get_or_insert_with(Progress::default).cursor = Some(cursor);
        Ok(())
    }

    /// The columns a position mode remembers: the position, then each `:last.<column>` the query names.
    fn remembered(&self) -> Vec<&str> {
        let mut names: Vec<&str> = match &self.config.mode {
            Mode::Incrementing(p) | Mode::UpdatedAt(p) => p.columns.iter().map(String::as_str).collect(),
            Mode::Comparing { .. } | Mode::EveryPoll => return Vec::new(),
        };
        for name in self.config.query.last_columns() {
            if !names.contains(&name) {
                names.push(name);
            }
        }
        names
    }

    /// Runs one poll and keeps what it read as pending. Err is the reason the poll failed, to show and retry.
    /// Runs the next statement on a thread of its own.
    fn start(&mut self) {
        let found = self.finding_statement();
        self.finding_from = found.is_some();
        let (statement, values) = found.unwrap_or_else(|| self.next_statement());
        let mut driver = self.driver.take().expect("no statement is running");
        let (answer, running) = mpsc::channel();
        std::thread::spawn(move || {
            // A driver parses what the database sent, so a panic in it is the database's bad answer, reported and
            // retried like any other. The driver is kept: a connection it left broken is opened again on its next run.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| driver.run(&statement, &values)))
                .unwrap_or_else(|panic| {
                    let said = panic
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    Err(format!("the driver panicked running the statement: {said}"))
                });
            // The source may be gone; the answer then has no one to go to.
            let _ = answer.send((driver, result));
        });
        self.running = Some(running);
    }

    /// The running statement's answer, once it comes within `timeout`.
    fn answer(&mut self, timeout: Duration) -> Option<Result<Rows, String>> {
        let running = self.running.as_ref()?;
        let (driver, result) = match running.recv_timeout(timeout) {
            Ok(answered) => answered,
            Err(RecvTimeoutError::Timeout) => return None,
            Err(RecvTimeoutError::Disconnected) => panic!("a driver panicked running a statement"),
        };
        self.running = None;
        self.driver = Some(driver);
        Some(result)
    }

    fn poll(&mut self, result: Result<Rows, String>) -> Result<(), String> {
        let result = result?;
        let index = |name: &str| result.columns.iter().position(|c| c == name);
        self.columns = Some(self.match_fields(&result.columns)?);
        let mut pending = Pending {
            rows: VecDeque::new(),
            entries: VecDeque::new(),
            cursor: None,
            removed: Vec::new(),
            held: 0,
        };
        let mut full = false;
        match &self.config.mode {
            Mode::Incrementing(_) | Mode::UpdatedAt(_) => {
                let remembered: Vec<(String, usize)> = self
                    .remembered()
                    .into_iter()
                    .map(|name| index(name).map(|i| (name.to_string(), i)).ok_or(name))
                    .collect::<Result<_, _>>()
                    .map_err(|name| format!("the result has no column `{name}`, which this source remembers"))?;
                if let Some((name, _)) = remembered.iter().find(|(name, _)| result.as_text.contains(name)) {
                    return Err(format!(
                        "`{name}` is of a type smeltr reads only as text, so it cannot be compared again; \
                         cast it in the query to a type smeltr reads, or remember another column"
                    ));
                }
                // A null position sorts outside every later `>` test, so the source would stop there without a word.
                if let Mode::Incrementing(p) | Mode::UpdatedAt(p) = &self.config.mode {
                    for name in &p.columns {
                        let i = remembered
                            .iter()
                            .find(|(n, _)| n == name)
                            .expect("a position column is remembered")
                            .1;
                        if result.rows.iter().any(|row| matches!(row[i], Value::Null)) {
                            return Err(format!(
                                "a row's position `{name}` is null or infinite; a position must hold a value"
                            ));
                        }
                    }
                }
                let mut rows = result.rows;
                let read = rows.len();
                self.cut_at_last_run(&mut rows, &remembered);
                if let Some(last) = rows.last() {
                    let cursor: Cursor = remembered.iter().map(|(n, i)| (n.clone(), last[*i].clone())).collect();
                    let nested = |v: &Value| matches!(v, Value::Array(items) if items.iter().any(|i| matches!(i, Value::Array(_))));
                    if let Some(name) = cursor.iter().find(|(_, v)| nested(v)).map(|(name, _)| name) {
                        return Err(format!(
                            "`{name}` holds an array of arrays, which cannot be bound to compare again; \
                             remember another column"
                        ));
                    }
                    let text = stored_cursor(cursor.iter().map(|(name, value)| (name.as_str(), value)));
                    if text.len() > MAX_CURSOR {
                        return Err(format!(
                            "the last row's remembered values take {} bytes, over the {MAX_CURSOR} a position may",
                            text.len()
                        ));
                    }
                    self.cursor = Some(cursor);
                    pending.cursor = Some(text);
                }
                pending.rows = rows.into();
                full = read >= self.config.batch as usize;
            }
            Mode::Comparing { key } => {
                if result.rows.len() > MAX_COMPARED_ROWS as usize {
                    return Err(format!(
                        "comparing values reads at most {MAX_COMPARED_ROWS} rows a poll; this result has more"
                    ));
                }
                let key: Vec<usize> = key
                    .iter()
                    .map(|name| index(name).ok_or(name))
                    .collect::<Result<_, _>>()
                    .map_err(|name| format!("the result has no column `{name}`, which is this source's key"))?;
                let declared = self.columns.clone().expect("matched above");
                let mut seen = HashSet::with_capacity(result.rows.len());
                for row in result.rows {
                    let item = stored_row(key.iter().map(|i| &row[*i]));
                    let item = String::from_utf8(item).expect("a stored value is UTF-8");
                    if item.len() > MAX_LEDGER_ITEM {
                        return Err(format!(
                            "a key takes {} bytes, over the {MAX_LEDGER_ITEM} a key may",
                            item.len()
                        ));
                    }
                    // Only what the record carries: a column no field reads cannot make a row new.
                    let entry = hash(&declared.iter().map(|i| &row[*i]).collect::<Vec<_>>());
                    debug_assert!(entry.len() <= MAX_LEDGER_ENTRY);
                    if !seen.insert(item.clone()) {
                        return Err(format!("two rows have the key {item}; a key must be unique"));
                    }
                    if self.ledger.get(&item) != Some(&entry) {
                        pending.entries.push_back(LedgerEntry { item, entry });
                        pending.rows.push_back(row);
                    }
                }
                // The ledger changes only once nothing above can fail the poll, or a changed row would match it unsent.
                for e in &pending.entries {
                    self.ledger.insert(e.item.clone(), e.entry.clone());
                }
                let gone: Vec<String> = self.ledger.keys().filter(|k| !seen.contains(*k)).cloned().collect();
                for item in &gone {
                    self.ledger.remove(item);
                }
                pending.removed = gone;
            }
            Mode::EveryPoll => {
                if result.rows.len() > self.config.batch as usize {
                    return Err(format!(
                        "every poll reads at most {} rows; this result has more",
                        self.config.batch
                    ));
                }
                pending.rows = result.rows.into();
            }
        }
        // A position mode whose poll came back full has more waiting, and reads it at once.
        if !full {
            self.next_poll = Instant::now() + self.config.poll_interval;
        }
        if pending.rows.is_empty() {
            // Nothing to hand out: a deletion found is delivered as soon as everything before it is.
            if !pending.removed.is_empty() {
                self.reads.carry(Reached {
                    cursor: None,
                    added: Vec::new(),
                    removed: pending.removed,
                });
                self.settle();
            }
            return Ok(());
        }
        pending.held = pending.rows.iter().map(|row| held(row)).sum();
        self.pending = Some(pending);
        Ok(())
    }

    /// Without a tie-break, a cursor inside rows sharing the position's value would skip the rest of them. So a full
    /// result keeps only the rows before its last run of one value, and the next poll begins at that run. A run that
    /// fills the batch is read again at twice the batch; one that fills that too is sent whole, and the rows sharing
    /// its value past it are skipped with a warning.
    fn cut_at_last_run(&mut self, rows: &mut Vec<Vec<Value>>, remembered: &[(String, usize)]) {
        let wide = std::mem::take(&mut self.wide);
        let (Mode::Incrementing(p) | Mode::UpdatedAt(p)) = &self.config.mode else {
            return;
        };
        let limit = self.config.batch as usize * if wide { 2 } else { 1 };
        if p.columns.len() != 1 || rows.len() < limit {
            return;
        }
        let name = &p.columns[0];
        let i = remembered
            .iter()
            .find(|(n, _)| n == name)
            .expect("a position column is remembered")
            .1;
        let last = rows.last().expect("a full result has rows")[i].clone();
        let run = rows.iter().rev().take_while(|row| row[i] == last).count();
        if run < rows.len() {
            rows.truncate(rows.len() - run);
        } else if !wide {
            rows.clear();
            self.wide = true;
        } else {
            tracing::warn!(
                target: "sql.poll",
                position = %name,
                value = ?last,
                rows = limit,
                "more rows share one position value than twice the batch; the rest of them are skipped. \
                 Add a tie-break to the position, or a larger batch"
            );
        }
    }

    /// The result column each field reads, by name. A field the result lacks fails the poll; a column no field
    /// reads is warned of once.
    fn match_fields(&mut self, columns: &[String]) -> Result<Vec<usize>, String> {
        let mut names = HashSet::new();
        for c in columns {
            if !names.insert(c) {
                return Err(format!("the result has two columns named `{c}`; give one an alias"));
            }
        }
        for (name, ty) in self.config.fields.iter() {
            if let Some(t) = unbuilt(ty) {
                return Err(format!(
                    "field `{name}` is declared {}, which a SQL source cannot read",
                    t.spell()
                ));
            }
        }
        let found = self
            .config
            .fields
            .iter()
            .map(|(name, _)| {
                columns
                    .iter()
                    .position(|c| c == name)
                    .ok_or_else(|| format!("the result has no column `{name}`, which the record declares"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for c in columns {
            if !self.config.fields.iter().any(|(name, _)| name == c) && self.warned.insert(c.clone()) {
                tracing::warn!(target: "sql.poll", column = %c, "a result column no field of the record reads is left out");
            }
        }
        Ok(found)
    }

    /// Hands out up to `max_rows` pending rows as one read.
    fn hand_out(&mut self, max_rows: usize, max_bytes: usize) -> Vec<SourceRecord> {
        let Some(pending) = self.pending.as_mut() else {
            return Vec::new();
        };
        let mut n = 0;
        let mut bytes = 0;
        for row in pending.rows.iter().take(max_rows) {
            bytes += row.iter().map(Value::size).sum::<usize>();
            if n > 0 && bytes > max_bytes {
                break;
            }
            n += 1;
        }
        // A row over `max_bytes` still goes, alone: refusing it would stop the source there.
        let n = n.max(1);
        let rows: Vec<Vec<Value>> = pending.rows.drain(..n).collect();
        pending.held -= rows.iter().map(|row| held(row)).sum::<usize>();
        let added: Vec<LedgerEntry> = pending.entries.drain(..n.min(pending.entries.len())).collect();
        let last = pending.rows.is_empty();
        let (cursor, removed) = match last {
            true => (pending.cursor.take(), std::mem::take(&mut pending.removed)),
            false => (None, Vec::new()),
        };
        if last {
            self.pending = None;
        }
        let columns = self.columns.as_ref().expect("a poll matched the fields");
        let chunk = Arc::new(chunk(&self.config.fields, columns, &rows));
        let reached = Reached { cursor, added, removed };
        let partition = self
            .reads
            .open_whole(self.config.place.clone(), rows.len() as i64, reached);
        (0..rows.len())
            .map(|row| SourceRecord {
                chunk: chunk.clone(),
                row: row as u32,
                offset: row as i64,
                partition,
                topic: self.config.place.clone(),
                headers: None,
            })
            .collect()
    }

    /// Retires the oldest reads that are done, and takes what they reached as progress.
    fn settle(&mut self) {
        for reached in self.reads.settle() {
            if reached.cursor.is_none() && reached.added.is_empty() && reached.removed.is_empty() {
                continue;
            }
            let progress = self.progress.get_or_insert_with(Progress::default);
            if reached.cursor.is_some() {
                progress.cursor = reached.cursor;
            }
            for entry in reached.added {
                progress.removed.retain(|item| *item != entry.item);
                progress.added.retain(|e| e.item != entry.item);
                progress.added.push(entry);
            }
            for item in reached.removed {
                progress.added.retain(|e| e.item != item);
                progress.removed.push(item);
            }
        }
    }
}

impl<D: Driver + 'static> Source for SqlSource<D> {
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        if self.blocked {
            std::thread::sleep(timeout);
            return Ok(Vec::new());
        }
        if self.pending.is_none() {
            if self.running.is_none() {
                let now = Instant::now();
                if self.pacer.retrying() {
                    // A backoff can outlast the warning, so a failure is told again while it is waited out.
                    self.pacer.relog();
                    self.pacer.wait(now + timeout);
                    return Ok(Vec::new());
                }
                if now < self.next_poll {
                    std::thread::sleep((self.next_poll - now).min(timeout));
                    return Ok(Vec::new());
                }
                self.start();
            }
            let Some(result) = self.answer(timeout) else {
                return Ok(Vec::new());
            };
            if std::mem::take(&mut self.finding_from) {
                match self.found_from(result) {
                    Ok(()) => self.pacer.landed(),
                    Err(why) => self.pacer.failed(why, None),
                }
                return Ok(Vec::new());
            }
            match self.poll(result) {
                Ok(()) => {
                    // A poll that only asks for a wider read has read nothing yet.
                    if !self.wide {
                        self.answered += 1;
                    }
                    self.pacer.landed();
                }
                Err(why) => {
                    self.pacer.failed(why, None);
                    return Ok(Vec::new());
                }
            }
        }
        Ok(self.hand_out(max_rows, max_bytes))
    }

    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
        self.reads.commit(offsets);
        self.settle();
        Ok(())
    }

    fn block(&mut self) -> Result<(), IoError> {
        self.blocked = true;
        Ok(())
    }

    fn release(&mut self) -> Result<(), IoError> {
        self.blocked = false;
        Ok(())
    }

    fn connection_error(&self) -> Option<String> {
        self.pacer.error().map(str::to_string)
    }

    fn held_bytes(&self) -> Option<u64> {
        Some(self.pending.as_ref().map_or(0, |p| p.held) as u64)
    }

    fn take_progress(&mut self) -> Option<Progress> {
        self.progress.take()
    }

    fn resume(&mut self, from: Progress) -> Result<(), IoError> {
        self.cursor = match from.cursor {
            Some(text) => Some(
                serde_json::from_str(&text)
                    .map_err(|e| IoError::rejected(format!("the stored position is not one this source wrote: {e}")))?,
            ),
            None => None,
        };
        // Where an earlier holder began, before it sent a row.
        if let Some(begun) = self.cursor.as_mut().and_then(|c| c.remove(FROM)) {
            self.cursor = None;
            self.from = (begun != Value::Null).then_some(begun);
            self.from_known = true;
        }
        // A position column the stored cursor lacks would bind null, which no row passes: the source would read
        // nothing ever again, and say nothing.
        if let (Some(cursor), Mode::Incrementing(p) | Mode::UpdatedAt(p)) = (&self.cursor, &self.config.mode)
            && let Some(name) = p.columns.iter().find(|name| !cursor.contains_key(*name))
        {
            return Err(IoError::rejected(format!(
                "the stored position has no `{name}`, which this source's position is ordered by; \
                 it was stored under another position"
            )));
        }
        for item in from.removed {
            self.ledger.remove(&item);
        }
        for LedgerEntry { item, entry } in from.added {
            self.ledger.insert(item, entry);
        }
        Ok(())
    }
}

/// A cursor as it is stored: an object of each name's value, in the order given.
fn stored_cursor<'v>(members: impl IntoIterator<Item = (&'v str, &'v Value)>) -> String {
    let mut out = vec![b'{'];
    for (i, (name, value)) in members.into_iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        crate::format::json::write_json_string(name.as_bytes(), &mut out);
        out.push(b':');
        stored(value, &mut out);
    }
    out.push(b'}');
    String::from_utf8(out).expect("a stored value is UTF-8")
}

/// Values as they are stored: an array of each.
fn stored_row<'v>(values: impl IntoIterator<Item = &'v Value>) -> Vec<u8> {
    let mut out = vec![b'['];
    for (i, value) in values.into_iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        stored(value, &mut out);
    }
    out.push(b']');
    out
}

/// A row's values, hashed: SHA-256 of their JSON, which is canonical for one order of fields.
fn hash(row: &[&Value]) -> String {
    use std::fmt::Write;
    let text = stored_row(row.iter().copied());
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &text);
    digest.as_ref().iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// `rows` as a chunk with a column per field, each read from result column `columns[i]`. A value that cannot be
/// its field's type is null and counted failed.
fn chunk(fields: &[(String, DataType)], columns: &[usize], rows: &[Vec<Value>]) -> RecordChunk {
    let mut pool = crate::core::buffer::BufferPool::new();
    let mut failed = Vec::new();
    let mut out = Vec::with_capacity(fields.len());
    for (f, ((_, ty), &at)) in fields.iter().zip(columns).enumerate() {
        let values: Vec<Value> = rows.iter().map(|r| r[at].clone()).collect();
        let (column, refused) =
            column(&mut pool, ty, &values).expect("match_fields refuses a type no column is built as");
        failed.extend(refused.into_iter().map(|row| (row as u32, f, spell(&values[row]))));
        out.push(column);
    }
    RecordChunk::new(out, failed)
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TopicPartition;
    use std::sync::Mutex;

    /// `v` as a driver reads a JSON column.
    fn json(v: serde_json::Value) -> Value {
        crate::io::values::json(&serde_json::to_vec(&v).unwrap()).unwrap()
    }

    /// One run a driver was asked for: the statement and the values bound to it.
    type Asked = (Statement, Vec<Value>);

    /// A driver that answers each run with the next scripted result, and keeps what it was asked.
    #[derive(Clone, Default)]
    struct Scripted {
        answers: Arc<Mutex<VecDeque<Result<Rows, String>>>>,
        asked: Arc<Mutex<Vec<Asked>>>,
        /// When set, the next run panics instead of answering, as a driver does on a wire answer it cannot parse.
        panic_next: Arc<Mutex<bool>>,
        /// When set, each run answers only once a signal comes through it.
        gate: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    }

    impl Scripted {
        fn answer(&self, columns: &[&str], rows: Vec<Vec<Value>>) {
            self.answers.lock().unwrap().push_back(Ok(Rows {
                columns: columns.iter().map(|c| c.to_string()).collect(),
                rows,
                as_text: Vec::new(),
            }));
        }

        fn fail(&self, why: &str) {
            self.answers.lock().unwrap().push_back(Err(why.to_string()));
        }

        fn panic_once(&self) {
            *self.panic_next.lock().unwrap() = true;
        }

        fn last_asked(&self) -> (Statement, Vec<Value>) {
            self.asked.lock().unwrap().last().cloned().expect("a run")
        }
    }

    impl Driver for Scripted {
        fn dialect(&self) -> Dialect {
            Dialect::Postgres
        }

        fn run(&mut self, statement: &Statement, values: &[Value]) -> Result<Rows, String> {
            self.asked.lock().unwrap().push((statement.clone(), values.to_vec()));
            if std::mem::take(&mut *self.panic_next.lock().unwrap()) {
                panic!("the wire said nothing a driver expects");
            }
            if let Some(gate) = self.gate.lock().unwrap().as_ref() {
                gate.recv().unwrap();
            }
            self.answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(Rows::default()))
        }

        fn describe(&mut self, _: &Statement) -> Result<Vec<Described>, String> {
            Ok(Vec::new())
        }

        fn plan(&mut self, _: &Statement, _: &[Value]) -> Result<Verdict, String> {
            Ok(Verdict::Indexed {
                indexes: Vec::new(),
                rows: 0,
            })
        }
    }

    const COLUMNS: &[&str] = &["id", "name"];

    fn row(id: i64, name: &str) -> Vec<Value> {
        vec![Value::Int(id), Value::Str(name.into())]
    }

    fn source(driver: &Scripted, mode: Mode, batch: u32) -> SqlSource<Scripted> {
        SqlSource::new(
            driver.clone(),
            SqlSourceConfig {
                query: Query::parse("SELECT id, name FROM t", Dialect::Postgres).unwrap(),
                mode,
                fields: Arc::from(vec![
                    ("id".to_string(), DataType::I64),
                    ("name".to_string(), DataType::Str),
                ]),
                poll_interval: Duration::ZERO,
                batch,
                place: Arc::from("postgres://db/t"),
            },
        )
    }

    fn by_id() -> Mode {
        Mode::Incrementing(Position {
            columns: vec!["id".into()],
            settle_secs: None,
            start: Start::Beginning,
        })
    }

    /// A position with a tie-break.
    fn by_id_and_name() -> Mode {
        Mode::Incrementing(Position {
            columns: vec!["id".into(), "name".into()],
            settle_secs: None,
            start: Start::Beginning,
        })
    }

    /// One poll, waiting out the statement it starts.
    fn poll(s: &mut SqlSource<Scripted>, max_rows: usize) -> Vec<SourceRecord> {
        loop {
            let got = s.poll_batch(max_rows, usize::MAX, Duration::ZERO).unwrap();
            if s.running.is_none() {
                return got;
            }
        }
    }

    fn commit(s: &mut SqlSource<Scripted>, records: &[SourceRecord]) {
        let mut offsets = CommitMap::new();
        for r in records {
            let at = TopicPartition {
                topic: r.topic.clone(),
                partition: r.partition,
            };
            let o = offsets.entry(at).or_insert(r.offset);
            *o = (*o).max(r.offset);
        }
        s.commit(&offsets).unwrap();
    }

    fn ids(records: &[SourceRecord]) -> Vec<i64> {
        records
            .iter()
            .map(|r| r.chunk.column(0).i64().unwrap().values[r.row as usize])
            .collect()
    }

    #[test]
    fn a_position_moves_only_once_every_record_before_it_commits() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a"), row(2, "b"), row(3, "c")]);
        let mut s = source(&driver, by_id(), 100);
        let first = poll(&mut s, 2);
        let second = poll(&mut s, 2);
        assert_eq!((ids(&first), ids(&second)), (vec![1, 2], vec![3]));
        commit(&mut s, &second);
        assert!(s.take_progress().is_none(), "the first hand-out is not yet delivered");
        commit(&mut s, &first);
        assert_eq!(
            s.take_progress().unwrap().cursor.as_deref(),
            Some(r#"{"id":{"int":3}}"#)
        );
    }

    /// The rows a poll asks for, from its `LIMIT`.
    fn limit_asked(driver: &Scripted) -> u32 {
        let sql = driver.last_asked().0.sql;
        sql.rsplit("LIMIT ").next().unwrap().trim().parse().unwrap()
    }

    #[test]
    fn a_source_holds_the_rows_it_has_not_handed_out() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a"), row(2, "b"), row(3, "c")]);
        let mut s = source(&driver, by_id(), 100);
        assert_eq!(s.held_bytes(), Some(0));
        poll(&mut s, 2);
        assert_eq!(s.held_bytes(), Some(held(&row(3, "c")) as u64));
        poll(&mut s, 2);
        assert_eq!(s.held_bytes(), Some(0));
    }

    #[test]
    fn a_full_poll_without_a_tie_break_stops_before_its_last_run_of_one_value() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a"), row(2, "b"), row(2, "c")]);
        let mut s = source(&driver, by_id(), 3);
        assert_eq!(ids(&poll(&mut s, 10)), [1]);
        poll(&mut s, 10);
        assert_eq!((driver.last_asked().1, limit_asked(&driver)), (vec![Value::Int(1)], 3));
    }

    #[test]
    fn a_run_of_one_value_filling_the_batch_is_read_again_at_twice_the_batch() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(5, "a"), row(5, "b")]);
        driver.answer(COLUMNS, vec![row(5, "a"), row(5, "b"), row(5, "c"), row(6, "d")]);
        let mut s = source(&driver, by_id(), 2);
        assert!(poll(&mut s, 10).is_empty());
        assert!(!s.read_once(), "a poll that only asks to read wider has read nothing");
        assert_eq!(ids(&poll(&mut s, 10)), [5, 5, 5]);
        assert_eq!(limit_asked(&driver), 4);
        poll(&mut s, 10);
        assert_eq!((driver.last_asked().1, limit_asked(&driver)), (vec![Value::Int(5)], 2));
    }

    #[test]
    fn a_run_of_one_value_past_twice_the_batch_is_sent_and_the_rest_skipped() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(5, "a"), row(5, "b")]);
        driver.answer(COLUMNS, vec![row(5, "a"), row(5, "b"), row(5, "c"), row(5, "d")]);
        let mut s = source(&driver, by_id(), 2);
        poll(&mut s, 10);
        assert_eq!(ids(&poll(&mut s, 10)), [5, 5, 5, 5]);
        poll(&mut s, 10);
        assert_eq!((driver.last_asked().1, limit_asked(&driver)), (vec![Value::Int(5)], 2));
    }

    #[test]
    fn a_source_read_at_most_some_rows_plans_its_own_batch_first() {
        let driver = Scripted::default();
        let mut s = source(&driver, by_id(), 5000);
        assert!(s.next_statement().0.sql.ends_with("LIMIT 5000"));
        s.read_at_most(20);
        assert!(s.next_statement().0.sql.ends_with("LIMIT 20"));
    }

    #[test]
    fn a_position_with_a_tie_break_reads_its_whole_batch() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a"), row(2, "b"), row(2, "c")]);
        let mut s = source(&driver, by_id_and_name(), 3);
        assert_eq!(ids(&poll(&mut s, 10)), [1, 2, 2]);
    }

    #[test]
    fn the_next_poll_reads_past_the_last_row_read() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(7, "a")]);
        let mut s = source(&driver, by_id(), 100);
        poll(&mut s, 10);
        assert!(
            driver.last_asked().1.is_empty(),
            "the first poll reads from the beginning"
        );
        poll(&mut s, 10);
        let (statement, values) = driver.last_asked();
        assert!(statement.sql.contains("q.\"id\" > $1"), "{}", statement.sql);
        assert_eq!(values, [Value::Int(7)]);
    }

    #[test]
    fn a_full_poll_reads_again_at_once_and_a_short_one_waits() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a"), row(2, "b")]);
        driver.answer(COLUMNS, vec![row(3, "c")]);
        let mut s = source(&driver, by_id_and_name(), 2);
        s.config.poll_interval = Duration::from_secs(3600);
        assert_eq!(ids(&poll(&mut s, 10)), [1, 2]);
        assert_eq!(ids(&poll(&mut s, 10)), [3]);
        assert!(poll(&mut s, 10).is_empty());
        assert_eq!(driver.asked.lock().unwrap().len(), 2);
    }

    #[test]
    fn a_resumed_source_reads_past_the_stored_position() {
        let driver = Scripted::default();
        let mut s = source(&driver, by_id(), 100);
        s.resume(Progress {
            cursor: Some(r#"{"id":{"int":41}}"#.into()),
            ..Progress::default()
        })
        .unwrap();
        poll(&mut s, 10);
        assert_eq!(driver.last_asked().1, [Value::Int(41)]);
        assert!(
            s.resume(Progress {
                cursor: Some("not json".into()),
                ..Progress::default()
            })
            .is_err()
        );
    }

    #[test]
    fn a_stored_position_without_the_position_column_is_refused() {
        let driver = Scripted::default();
        let mut s = source(&driver, by_id(), 100);
        let refused = s
            .resume(Progress {
                cursor: Some(r#"{"stamp":{"int":41}}"#.into()),
                ..Progress::default()
            })
            .unwrap_err();
        assert!(refused.to_string().contains("has no `id`"), "{refused}");
    }

    #[test]
    fn a_poll_hands_out_rows_up_to_its_bytes_and_one_over_alone() {
        let driver = Scripted::default();
        let big = "x".repeat(100);
        driver.answer(COLUMNS, vec![row(1, "a"), row(2, "b"), row(3, &big), row(4, "c")]);
        let mut s = source(&driver, by_id(), 100);
        let first = s.poll_batch(10, 20, Duration::from_secs(5)).unwrap();
        assert_eq!(ids(&first), [1, 2], "each row is eight bytes and one");
        let second = s.poll_batch(10, 20, Duration::ZERO).unwrap();
        assert_eq!(ids(&second), [3], "a row over the limit goes alone");
        assert_eq!(ids(&s.poll_batch(10, 20, Duration::ZERO).unwrap()), [4]);
    }

    #[test]
    fn a_statement_that_outlasts_the_timeout_is_picked_up_by_a_later_poll() {
        let driver = Scripted::default();
        let (go, gate) = mpsc::channel();
        *driver.gate.lock().unwrap() = Some(gate);
        driver.answer(COLUMNS, vec![row(1, "a")]);
        let mut s = source(&driver, by_id(), 100);
        let started = Instant::now();
        assert!(
            s.poll_batch(10, usize::MAX, Duration::from_millis(50))
                .unwrap()
                .is_empty()
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the poll waited out the statement"
        );
        assert!(s.poll_batch(10, usize::MAX, Duration::ZERO).unwrap().is_empty());
        go.send(()).unwrap();
        let got = s.poll_batch(10, usize::MAX, Duration::from_secs(5)).unwrap();
        assert_eq!(ids(&got), [1]);
        assert_eq!(driver.asked.lock().unwrap().len(), 1, "one statement ran");
    }

    #[test]
    fn last_binds_null_before_the_first_row_and_is_remembered_after() {
        let driver = Scripted::default();
        driver.answer(
            &["id", "name", "batch"],
            vec![vec![Value::Int(1), Value::Str("a".into()), Value::Int(9)]],
        );
        let mut s = source(&driver, by_id(), 100);
        s.config.query = Query::parse(
            "SELECT id, name, batch FROM t WHERE (:last.batch IS NULL OR batch >= :last.batch)",
            Dialect::Postgres,
        )
        .unwrap();
        let records = poll(&mut s, 10);
        commit(&mut s, &records);
        assert_eq!(driver.last_asked().1, [Value::Null]);
        assert_eq!(
            s.take_progress().unwrap().cursor.as_deref(),
            Some(r#"{"batch":{"int":9},"id":{"int":1}}"#)
        );
        poll(&mut s, 10);
        assert_eq!(
            driver.last_asked().1,
            [Value::Int(9), Value::Int(1)],
            "and past the last row"
        );
    }

    #[test]
    fn a_position_too_large_to_keep_fails_the_poll() {
        let driver = Scripted::default();
        driver.answer(
            &["id", "name"],
            vec![vec![Value::Str("x".repeat(MAX_CURSOR)), Value::Null]],
        );
        let mut s = source(
            &driver,
            Mode::Incrementing(Position {
                columns: vec!["id".into()],
                settle_secs: None,
                start: Start::Beginning,
            }),
            100,
        );
        assert!(poll(&mut s, 10).is_empty());
        assert!(
            s.connection_error().unwrap().contains("over the 1024"),
            "{:?}",
            s.connection_error()
        );
    }

    #[test]
    fn comparing_sends_new_and_changed_rows_and_notes_the_gone() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a"), row(2, "b")]);
        driver.answer(COLUMNS, vec![row(1, "a"), row(2, "B")]);
        driver.answer(COLUMNS, vec![row(2, "B")]);
        let mut s = source(&driver, Mode::Comparing { key: vec!["id".into()] }, 100);
        let first = poll(&mut s, 10);
        assert_eq!(ids(&first), [1, 2]);
        commit(&mut s, &first);
        let progress = s.take_progress().unwrap();
        assert_eq!(
            progress.added.iter().map(|e| e.item.as_str()).collect::<Vec<_>>(),
            [r#"[{"int":1}]"#, r#"[{"int":2}]"#]
        );
        let second = poll(&mut s, 10);
        assert_eq!(ids(&second), [2], "only the changed row");
        commit(&mut s, &second);
        assert_eq!(s.take_progress().unwrap().added.len(), 1);
        assert!(poll(&mut s, 10).is_empty());
        assert_eq!(s.take_progress().unwrap().removed, [r#"[{"int":1}]"#]);
    }

    #[test]
    fn comparing_ignores_a_change_in_a_column_no_field_reads() {
        let driver = Scripted::default();
        let with_extra = |extra: i64| vec![vec![Value::Int(1), Value::Str("a".into()), Value::Int(extra)]];
        driver.answer(&["id", "name", "seen"], with_extra(1));
        driver.answer(&["id", "name", "seen"], with_extra(2));
        let mut s = source(&driver, Mode::Comparing { key: vec!["id".into()] }, 100);
        let first = poll(&mut s, 10);
        assert_eq!(ids(&first), [1]);
        commit(&mut s, &first);
        assert!(poll(&mut s, 10).is_empty());
    }

    #[test]
    fn a_narrow_cell_counts_more_held_than_its_record_bytes() {
        let row = [Value::Bool(true), Value::Null];
        assert!(held(&row) > row.iter().map(Value::size).sum::<usize>() + 2 * 8);
        assert_eq!(
            held(&[Value::Str("abc".into())]) - held(&[Value::Null]),
            3,
            "text counts its bytes"
        );
    }

    #[test]
    fn comparing_resumes_from_the_ledger() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a")]);
        let mut first = source(&driver, Mode::Comparing { key: vec!["id".into()] }, 100);
        let records = poll(&mut first, 10);
        commit(&mut first, &records);
        let progress = first.take_progress().unwrap();
        driver.answer(COLUMNS, vec![row(1, "a")]);
        let mut second = source(&driver, Mode::Comparing { key: vec!["id".into()] }, 100);
        second.resume(progress).unwrap();
        assert!(poll(&mut second, 10).is_empty(), "an unchanged row is not sent again");
    }

    #[test]
    fn comparing_refuses_a_key_two_rows_share() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a"), row(1, "b")]);
        let mut s = source(&driver, Mode::Comparing { key: vec!["id".into()] }, 100);
        assert!(poll(&mut s, 10).is_empty());
        assert!(s.connection_error().unwrap().contains("must be unique"));
    }

    #[test]
    fn a_changed_row_in_a_failed_poll_is_sent_by_the_next() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a")]);
        driver.answer(COLUMNS, vec![row(1, "b"), row(2, "x"), row(2, "y")]);
        driver.answer(COLUMNS, vec![row(1, "b"), row(2, "x")]);
        let mut s = source(&driver, Mode::Comparing { key: vec!["id".into()] }, 100);
        let first = poll(&mut s, 10);
        commit(&mut s, &first);
        assert!(poll(&mut s, 10).is_empty());
        assert!(s.connection_error().unwrap().contains("must be unique"));
        s.pacer.retry_now();
        s.next_poll = Instant::now();
        assert_eq!(ids(&poll(&mut s, 10)), [1, 2]);
    }

    #[test]
    fn a_null_position_fails_the_poll() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a"), vec![Value::Null, Value::Str("b".into())]]);
        let mut s = source(&driver, by_id(), 100);
        assert!(poll(&mut s, 10).is_empty());
        assert!(
            s.connection_error().unwrap().contains("position `id` is null"),
            "{:?}",
            s.connection_error()
        );
    }

    #[test]
    fn a_position_read_only_as_text_fails_the_poll() {
        let driver = Scripted::default();
        driver.answers.lock().unwrap().push_back(Ok(Rows {
            columns: COLUMNS.iter().map(|c| c.to_string()).collect(),
            rows: vec![row(1, "a")],
            as_text: vec!["id".into()],
        }));
        let mut s = source(&driver, by_id(), 100);
        assert!(poll(&mut s, 10).is_empty());
        assert!(
            s.connection_error()
                .unwrap()
                .contains("`id` is of a type smeltr reads only as text"),
            "{:?}",
            s.connection_error()
        );
    }

    #[test]
    fn a_position_holding_an_array_of_arrays_fails_the_poll() {
        let driver = Scripted::default();
        let grid = Value::Array(vec![Value::Array(vec![Value::Int(1)])]);
        driver.answer(
            &["id", "name", "batch"],
            vec![vec![Value::Int(1), Value::Str("a".into()), grid]],
        );
        let mut s = source(&driver, by_id(), 100);
        s.config.query = Query::parse("SELECT id, name FROM t WHERE batch > :last.batch", Dialect::Postgres).unwrap();
        assert!(poll(&mut s, 10).is_empty());
        assert!(
            s.connection_error()
                .unwrap()
                .contains("`batch` holds an array of arrays"),
            "{:?}",
            s.connection_error()
        );
    }

    #[test]
    fn a_float_that_is_not_finite_keeps_its_value_through_json() {
        for f in [f64::INFINITY, f64::NEG_INFINITY, 1.5] {
            let text = serde_json::to_string(&Value::Float(f)).unwrap();
            assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), Value::Float(f), "{text}");
        }
        let nan = serde_json::to_string(&Value::Float(f64::NAN)).unwrap();
        assert_eq!(nan, r#"{"float":"NaN"}"#);
        assert!(matches!(serde_json::from_str(&nan).unwrap(), Value::Float(f) if f.is_nan()));
        assert_ne!(hash(&[&Value::Float(f64::NAN)]), hash(&[&Value::Null]));
    }

    #[test]
    fn every_poll_sends_the_whole_result_and_keeps_nothing() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a")]);
        driver.answer(COLUMNS, vec![row(1, "a")]);
        let mut s = source(&driver, Mode::EveryPoll, 100);
        let first = poll(&mut s, 10);
        commit(&mut s, &first);
        assert_eq!(ids(&poll(&mut s, 10)), [1]);
        assert!(s.take_progress().is_none());
        assert!(!driver.last_asked().0.sql.contains("WHERE"));
    }

    #[test]
    fn a_declared_column_the_result_lacks_fails_the_poll_and_an_extra_one_is_left_out() {
        let driver = Scripted::default();
        driver.answer(&["id"], vec![vec![Value::Int(1)]]);
        driver.answer(
            &["extra", "name", "id"],
            vec![vec![Value::Int(0), Value::Str("a".into()), Value::Int(1)]],
        );
        let mut s = source(&driver, by_id(), 100);
        assert!(poll(&mut s, 10).is_empty());
        assert!(s.connection_error().unwrap().contains("no column `name`"));
        s.pacer.retry_now();
        s.next_poll = Instant::now();
        assert_eq!(ids(&poll(&mut s, 10)), [1], "matched by name, not by place");
        assert!(s.connection_error().is_none());
    }

    #[test]
    fn a_field_of_a_type_no_column_is_built_as_fails_the_poll() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![row(1, "a")]);
        let mut s = source(&driver, Mode::EveryPoll, 100);
        s.config.fields = Arc::from(vec![
            ("id".to_string(), DataType::I64),
            ("name".to_string(), DataType::Array(Box::new(DataType::Null))),
        ]);
        assert!(poll(&mut s, 10).is_empty());
        assert!(
            s.connection_error().unwrap().contains("field `name`"),
            "{:?}",
            s.connection_error()
        );
    }

    #[test]
    fn a_value_that_is_not_its_fields_type_is_null_and_noted() {
        let driver = Scripted::default();
        driver.answer(COLUMNS, vec![vec![Value::Int(1), json(serde_json::json!({"a": 1}))]]);
        let mut s = source(&driver, by_id(), 100);
        let records = poll(&mut s, 10);
        let chunk = &records[0].chunk;
        assert!(!chunk.column(1).is_valid(0));
        assert_eq!(chunk.failed(), [(0, 1, r#"{"a":1}"#.to_string())]);
    }

    #[test]
    fn an_array_or_object_column_takes_its_declared_shape() {
        let driver = Scripted::default();
        driver.answer(
            &["id", "tags", "owner"],
            vec![
                vec![
                    Value::Int(1),
                    json(serde_json::json!(["a", "b"])),
                    json(serde_json::json!({"name": "ann", "age": 4})),
                ],
                vec![Value::Int(2), json(serde_json::json!("not a list")), Value::Null],
            ],
        );
        let mut s = source(&driver, by_id(), 100);
        let owner = DataType::Struct(
            vec![("age".into(), DataType::I64), ("name".into(), DataType::Str)],
            false,
        );
        s.config.fields = Arc::from(vec![
            ("id".to_string(), DataType::I64),
            ("tags".to_string(), DataType::Array(Box::new(DataType::Str))),
            ("owner".to_string(), owner.clone()),
        ]);
        let records = poll(&mut s, 10);
        let chunk = &records[0].chunk;
        assert_eq!(chunk.column(1).data_type(), DataType::Array(Box::new(DataType::Str)));
        assert_eq!(chunk.column(2).data_type(), owner);
        assert!(chunk.column(1).is_valid(0) && chunk.column(2).is_valid(0));
        assert!(!chunk.column(1).is_valid(1), "text is not a list");
        assert_eq!(chunk.failed(), [(1, 1, r#""not a list""#.to_string())]);
    }

    #[test]
    fn an_array_is_built_from_its_typed_elements() {
        let driver = Scripted::default();
        driver.answer(
            &["id", "seen"],
            vec![
                vec![
                    Value::Int(1),
                    Value::Array(vec![Value::Time(5), Value::Null, Value::Time(7)]),
                ],
                vec![
                    Value::Int(2),
                    Value::Array(vec![Value::Time(9), Value::Str("never".into())]),
                ],
            ],
        );
        let mut s = source(&driver, by_id(), 100);
        let seen = DataType::Array(Box::new(DataType::DateTime));
        s.config.fields = Arc::from(vec![
            ("id".to_string(), DataType::I64),
            ("seen".to_string(), seen.clone()),
        ]);
        let records = poll(&mut s, 10);
        let column = records[0].chunk.column(1);
        assert_eq!(column.data_type(), seen);
        let list = column.list().unwrap();
        assert_eq!(list.offsets, [0, 3, 5]);
        assert_eq!(list.child.datetime().unwrap().values[..3], [5, 0, 7]);
        assert!(column.is_valid(0));
        assert!(
            !column.is_valid(1),
            "an element that is not a time refuses its row's array"
        );
        assert_eq!(records[0].chunk.failed().len(), 1);
    }

    #[test]
    fn a_failed_poll_is_retried_after_a_backoff() {
        let driver = Scripted::default();
        driver.fail("connection refused");
        let mut s = source(&driver, by_id(), 100);
        assert!(poll(&mut s, 10).is_empty());
        assert_eq!(s.connection_error().as_deref(), Some("connection refused"));
        assert!(poll(&mut s, 10).is_empty());
        assert_eq!(
            driver.asked.lock().unwrap().len(),
            1,
            "not asked again before the backoff"
        );
    }

    /// A driver parses what the database sent, so a panic in it is the database's failure: reported, backed off
    /// and retried, with the same driver.
    #[test]
    fn a_driver_that_panics_is_a_failed_poll_and_not_a_dead_source() {
        let driver = Scripted::default();
        driver.panic_once();
        driver.answer(COLUMNS, vec![row(1, "a")]);
        let mut s = source(&driver, by_id(), 100);
        assert!(poll(&mut s, 10).is_empty());
        let error = s.connection_error().expect("the panic is the poll's failure");
        assert!(
            error.starts_with("the driver panicked running the statement"),
            "{error}"
        );
        assert_eq!(driver.asked.lock().unwrap().len(), 1);
        s.pacer.retry_now();
        s.next_poll = Instant::now();
        assert_eq!(poll(&mut s, 10).len(), 1, "the retry runs on the same driver");
        assert_eq!(s.connection_error(), None);
    }

    fn by_id_from(start: Start) -> Mode {
        Mode::Incrementing(Position {
            columns: vec!["id".into()],
            settle_secs: None,
            start,
        })
    }

    #[test]
    fn a_source_starting_now_begins_at_the_largest_position_and_keeps_it_at_once() {
        let driver = Scripted::default();
        driver.answer(&["max"], vec![vec![Value::Int(41)]]);
        let mut s = source(&driver, by_id_from(Start::Now), 100);
        assert!(poll(&mut s, 10).is_empty());
        let (found, values) = driver.last_asked();
        assert!(found.sql.starts_with("SELECT max(q.\"id\")"), "{}", found.sql);
        assert!(values.is_empty());
        assert_eq!(
            s.take_progress().unwrap().cursor.as_deref(),
            Some(r#"{"__from":{"int":41}}"#)
        );
        driver.answer(COLUMNS, vec![row(41, "a"), row(42, "b")]);
        assert_eq!(ids(&poll(&mut s, 10)), [41, 42]);
        let (read, values) = driver.last_asked();
        assert!(read.sql.contains("q.\"id\" >= $1"), "{}", read.sql);
        assert_eq!(values, [Value::Int(41)]);
    }

    #[test]
    fn a_source_starting_now_on_an_empty_result_begins_at_the_beginning() {
        let driver = Scripted::default();
        driver.answer(&["max"], vec![vec![Value::Null]]);
        let mut s = source(&driver, by_id_from(Start::Now), 100);
        poll(&mut s, 10);
        assert_eq!(
            s.take_progress().unwrap().cursor.as_deref(),
            Some(r#"{"__from":"null"}"#)
        );
        poll(&mut s, 10);
        let (read, values) = driver.last_asked();
        assert!(!read.sql.contains("WHERE"), "{}", read.sql);
        assert!(values.is_empty());
    }

    #[test]
    fn a_source_moved_before_its_first_row_begins_where_it_began() {
        let driver = Scripted::default();
        let mut s = source(&driver, by_id_from(Start::Now), 100);
        s.resume(Progress {
            cursor: Some(r#"{"__from":{"int":41}}"#.into()),
            ..Progress::default()
        })
        .unwrap();
        poll(&mut s, 10);
        assert_eq!(
            driver.asked.lock().unwrap().len(),
            1,
            "where it began is not found again"
        );
        assert_eq!(driver.last_asked().1, [Value::Int(41)]);
    }

    #[test]
    fn a_source_starting_from_a_value_reads_at_or_past_it() {
        let driver = Scripted::default();
        let mut s = source(&driver, by_id_from(Start::From(Value::Int(7))), 100);
        poll(&mut s, 10);
        let (read, values) = driver.last_asked();
        assert!(read.sql.contains("q.\"id\" >= $1"), "{}", read.sql);
        assert_eq!(values, [Value::Int(7)]);
        assert!(s.take_progress().is_none(), "a start the source is given is not kept");
    }

    #[test]
    fn before_the_first_row_a_last_of_the_first_column_is_where_the_source_begins() {
        let driver = Scripted::default();
        let mut s = SqlSource::new(
            driver.clone(),
            SqlSourceConfig {
                query: Query::parse("SELECT id, name FROM t WHERE id > :last.id", Dialect::Postgres).unwrap(),
                ..source(&driver, by_id_from(Start::From(Value::Int(7))), 100).config
            },
        );
        poll(&mut s, 10);
        assert_eq!(
            driver.last_asked().1,
            [Value::Int(7), Value::Int(7)],
            "in the query, and as the start outside it"
        );
    }
}
