//! The SQL source's offline fuzzer. A table that changes between polls is read in every mode, from every start, by
//! every shape of position and batch, through a database that fails, answers late and answers wrong, by a source that
//! hands out, commits, blocks and is restarted from the progress it kept, in any order. However it goes, every row the
//! mode promises arrives, and one holder sends none twice.
//!
//! Its second half declares PostgreSQL, MySQL and MariaDB sources from any options a file can hold, sound or not: each loads or is
//! refused, and none panics.

use std::sync::Mutex;

use super::*;
use crate::fuzz::{self, Coverage, Rng};
use crate::io::TopicPartition;

/// The result's columns. `x` is a field of a type each seed picks, holding values of any type.
const COLUMNS: [&str; 5] = ["id", "t", "name", "ver", "x"];

#[derive(Clone, Debug)]
struct Row {
    id: i64,
    /// A time, in the database's own seconds.
    t: i64,
    name: String,
    /// Bumped by every update, so each version of a row is told apart.
    ver: i64,
    x: Value,
}

impl Row {
    fn get(&self, column: &str) -> i64 {
        match column {
            "id" => self.id,
            "t" => self.t,
            other => unreachable!("no position orders by {other}"),
        }
    }
}

/// One way an answer goes wrong, once.
#[derive(Clone, Copy, Debug)]
enum Fault {
    Fails,
    TooLarge,
    Late,
    NullPosition,
    AsText,
    MissingColumn,
    TwoColumnsOneName,
}

const FAULTS: [Fault; 7] = [
    Fault::Fails,
    Fault::TooLarge,
    Fault::Late,
    Fault::NullPosition,
    Fault::AsText,
    Fault::MissingColumn,
    Fault::TwoColumnsOneName,
];

/// The table, and the database's clock.
struct Db {
    rows: Vec<Row>,
    now: i64,
    settle: Option<u32>,
    position: Vec<String>,
    fault: Option<Fault>,
}

impl Db {
    /// Whether a poll that waits for late commits sees `row` yet.
    fn settled(&self, row: &Row) -> bool {
        self.settle.is_none_or(|s| row.t <= self.now - i64::from(s))
    }
}

/// A database answering smeltr's statements over [`Db`]: it reads what each statement binds, not its text, but for
/// the limit and whether it asks for the largest value.
#[derive(Clone)]
struct Fake {
    db: Arc<Mutex<Db>>,
    dialect: Dialect,
}

impl Driver for Fake {
    fn dialect(&self) -> Dialect {
        self.dialect
    }

    fn run(&mut self, statement: &Statement, values: &[Value]) -> Result<Rows, String> {
        let fault = self.db.lock().unwrap().fault.take();
        match fault {
            Some(Fault::Fails) => return Err("server closed the connection unexpectedly".into()),
            Some(Fault::TooLarge) => return Err(too_large()),
            Some(Fault::Late) => std::thread::sleep(Duration::from_millis(3)),
            _ => {}
        }
        let db = self.db.lock().unwrap();
        // MySQL binds a value once for each place it stands; each binds the same.
        let bound = |bind: &Bind| {
            statement
                .binds
                .iter()
                .position(|b| b == bind)
                .map(|i| values[i].clone())
        };
        let int = |v: Value| match v {
            Value::Int(i) => i,
            other => panic!("bound {other:?} where a position is an integer"),
        };
        let mut rows: Vec<&Row> = db.rows.iter().filter(|r| db.settled(r)).collect();
        // The query's own `:last.t IS NULL OR t >= :last.t`.
        if let Some(last) = bound(&Bind::Last("t".into())).filter(|v| *v != Value::Null) {
            let last = int(last);
            rows.retain(|r| r.t >= last);
        }
        if statement.sql.starts_with("SELECT max(") {
            let first = &db.position[0];
            let max = rows.iter().map(|r| r.get(first)).max();
            return Ok(Rows {
                columns: vec!["max".into()],
                rows: vec![vec![max.map_or(Value::Null, Value::Int)]],
                as_text: match fault {
                    Some(Fault::AsText) => vec!["max".into()],
                    _ => Vec::new(),
                },
            });
        }
        let limit: usize = (statement.sql.rsplit_once("LIMIT ").expect("a poll has a limit").1)
            .trim()
            .parse()
            .expect("a limit is a number");
        if !db.position.is_empty() {
            let key = |r: &Row| db.position.iter().map(|c| r.get(c)).collect::<Vec<i64>>();
            let past: Option<Vec<i64>> = (0..db.position.len())
                .map(|n| bound(&Bind::Position(n)).map(int))
                .collect();
            if let Some(past) = past {
                rows.retain(|r| key(r) > past);
            } else if let Some(start) = bound(&Bind::Start) {
                let (first, start) = (&db.position[0], int(start));
                rows.retain(|r| r.get(first) >= start);
            }
            rows.sort_by_key(|r| key(r));
        }
        rows.truncate(limit);
        let mut columns: Vec<String> = COLUMNS.iter().map(|c| c.to_string()).collect();
        let mut rows: Vec<Vec<Value>> = rows
            .iter()
            .map(|r| {
                vec![
                    Value::Int(r.id),
                    Value::Int(r.t),
                    Value::Str(r.name.clone()),
                    Value::Int(r.ver),
                    r.x.clone(),
                ]
            })
            .collect();
        let mut as_text = Vec::new();
        match fault {
            Some(Fault::NullPosition) if !db.position.is_empty() => {
                let at = COLUMNS.iter().position(|c| *c == db.position[0]).unwrap();
                if let Some(row) = rows.last_mut() {
                    row[at] = Value::Null;
                }
            }
            Some(Fault::AsText) => as_text = vec![db.position.first().map_or("t".into(), Clone::clone)],
            Some(Fault::MissingColumn) => {
                columns.remove(2);
                rows.iter_mut().for_each(|r| drop(r.remove(2)));
            }
            Some(Fault::TwoColumnsOneName) => columns[4] = "id".into(),
            _ => {}
        }
        Ok(Rows { columns, rows, as_text })
    }

    fn describe(&mut self, _: &Statement) -> Result<Vec<Described>, String> {
        Ok(Vec::new())
    }

    fn plan(&mut self, _: &Statement, _: &[Value]) -> Result<Verdict, String> {
        Ok(Verdict::FullScan {
            relation: "events".into(),
            rows: self.db.lock().unwrap().rows.len() as u64,
        })
    }
}

/// The types `x` may be declared, each a type an SQL source builds.
fn x_types() -> Vec<(&'static str, DataType)> {
    vec![
        ("i64", DataType::I64),
        ("f64", DataType::F64),
        ("str", DataType::Str),
        ("bool", DataType::Bool),
        ("datetime", DataType::DateTime),
        ("duration", DataType::Duration),
        ("bytes", DataType::Bytes),
        ("variant", DataType::Variant),
        ("array<str>", DataType::Array(Box::new(DataType::Str))),
        ("array<i64>", DataType::Array(Box::new(DataType::I64))),
        (
            "struct",
            DataType::Struct(vec![("a".into(), DataType::I64), ("b".into(), DataType::Str)], false),
        ),
    ]
}

/// Any value a driver may read, its type whatever `x` is declared.
fn any_value(rng: &mut Rng, depth: usize) -> Value {
    match rng.below(if depth > 1 { 9 } else { 11 }) {
        0 => Value::Null,
        1 => Value::Bool(rng.chance(2)),
        2 => Value::Int(*rng.pick(&[0, -1, 1, i64::MIN, i64::MAX, 1_700_000_000])),
        3 => Value::Float(*rng.pick(&[0.0, -0.0, 1.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MAX])),
        4 => Value::Str(
            rng.pick(&["", "a", "ünï", "\0", "{\"a\":1}", "2026-10-12T00:00:00Z"])
                .to_string(),
        ),
        5 => Value::Bytes(vec![0xff; rng.below(4)]),
        6 => Value::Time(*rng.pick(&[0, i64::MIN, i64::MAX, 1_700_000_000_000_000_000])),
        7 => Value::Duration(*rng.pick(&[0, -1, i64::MAX])),
        8 => crate::io::values::json(rng.pick(&[&b"null"[..], br#"{"a": 1, "b": "x"}"#, b"[1, [2]]"])).unwrap(),
        _ => Value::Array((0..rng.below(4)).map(|_| any_value(rng, depth + 1)).collect()),
    }
}

/// What one seed reads, and how its table changes.
struct Case {
    mode: Mode,
    /// Whether the mode reads updates again.
    updates: bool,
    batch: u32,
    dialect: Dialect,
    last_filter: bool,
    x_type: DataType,
}

fn case(rng: &mut Rng, coverage: &mut Coverage) -> Case {
    let settle = |rng: &mut Rng, first: &str| (first == "t" && rng.chance(2)).then(|| 1 + rng.below(4) as u32);
    let start = |rng: &mut Rng, coverage: &mut Coverage| match rng.below(3) {
        0 => {
            coverage.reach("start beginning");
            Start::Beginning
        }
        1 => {
            coverage.reach("start now");
            Start::Now
        }
        _ => {
            coverage.reach("start from");
            Start::From(Value::Int(rng.below(40) as i64))
        }
    };
    let positions: [&[&str]; 3] = [&["id"], &["t"], &["t", "id"]];
    let (mode, updates) = match rng.below(6) {
        n @ (0 | 1) => {
            let columns: Vec<String> = match n {
                0 => rng.pick(&positions).iter().map(|c| c.to_string()).collect(),
                _ => rng.pick(&positions[1..]).iter().map(|c| c.to_string()).collect(),
            };
            match columns.as_slice() {
                [only] if only == "id" => coverage.reach("position unique"),
                [_] => coverage.reach("position of one column"),
                _ => coverage.reach("position with a tie-break"),
            }
            let position = Position {
                settle_secs: settle(rng, &columns[0]),
                start: start(rng, coverage),
                columns,
            };
            if position.settle_secs.is_some() {
                coverage.reach("settle");
            }
            match n {
                0 => {
                    coverage.reach("mode incrementing");
                    (Mode::Incrementing(position), false)
                }
                _ => {
                    coverage.reach("mode updated at");
                    (Mode::UpdatedAt(position), true)
                }
            }
        }
        2 | 3 => {
            coverage.reach("mode comparing");
            (Mode::Comparing { key: vec!["id".into()] }, true)
        }
        _ => {
            coverage.reach("mode every poll");
            (Mode::EveryPoll, true)
        }
    };
    let last_filter =
        matches!(&mode, Mode::Incrementing(p) | Mode::UpdatedAt(p) if p.columns[0] == "t") && rng.chance(2);
    if last_filter {
        coverage.reach(":last filter");
    }
    let dialect = *rng.pick(&[Dialect::Postgres, Dialect::MySql]);
    coverage.reach(format!("dialect {dialect:?}"));
    let types = x_types();
    let (name, x_type) = rng.pick(&types).clone();
    coverage.reach(format!("x {name}"));
    Case {
        mode,
        updates,
        batch: *rng.pick(&[1, 2, 3, 5, 8, 20]),
        dialect,
        last_filter,
        x_type,
    }
}

/// A row's id and version, from a record.
fn version(record: &SourceRecord) -> (i64, i64) {
    let at = |c: usize| record.chunk.column(c).i64().unwrap().values[record.row as usize];
    (at(0), at(3))
}

/// Folds `delta` into `kept`, as the leader keeps a source's progress.
fn keep(kept: &mut Progress, delta: Progress) {
    if delta.cursor.is_some() {
        kept.cursor = delta.cursor;
    }
    for entry in delta.added {
        kept.added.retain(|e| e.item != entry.item);
        kept.added.push(entry);
    }
    for item in delta.removed {
        kept.added.retain(|e| e.item != item);
    }
}

/// Seed by seed: the table, the source reading it, and what reached the destination.
struct Run<'a> {
    rng: Rng,
    coverage: &'a mut Coverage,
    case: Case,
    db: Arc<Mutex<Db>>,
    config: SqlSourceConfig,
    source: SqlSource<Fake>,
    next_id: i64,
    /// Rows sharing each value of `t`, every version counted.
    sharing: HashMap<i64, usize>,
    /// The largest `t` ever written, a deleted row's too: a time only grows.
    latest: i64,
    /// Handed out and not yet committed.
    handed: Vec<SourceRecord>,
    delivered: Vec<(i64, i64)>,
    kept: Progress,
    restarted: bool,
    /// Where the source began, once known: rows before it are not read.
    begun: Option<Value>,
}

impl Run<'_> {
    /// A `t` for a row written now: past every row a poll has seen, so a row is never late behind the position. Rows
    /// written together may share one.
    fn fresh_t(&mut self, db: &Db, floor: &mut Option<i64>) -> i64 {
        let t = match db.settle {
            // Within the wait, so no poll has passed it.
            Some(s) => db.now - self.rng.below(s as usize) as i64,
            None => {
                let base = *floor.get_or_insert(self.latest + 1);
                base + self.rng.below(2) as i64
            }
        };
        *floor = Some(floor.unwrap_or(t).max(t));
        self.latest = self.latest.max(t);
        *self.sharing.entry(t).or_default() += 1;
        t
    }

    fn change(&mut self) {
        let db = self.db.clone();
        let mut db = db.lock().unwrap();
        let mut floor = None;
        let n = db.rows.len();
        match self.rng.below(if self.case.updates { 4 } else { 2 }) {
            1 if n > 0 => {
                self.coverage.reach("a row deleted");
                let at = self.rng.below(n);
                db.rows.remove(at);
            }
            2 | 3 if n > 0 => {
                self.coverage.reach("a row updated");
                let at = self.rng.below(n);
                let t = self.fresh_t(&db, &mut floor);
                let x = any_value(&mut self.rng, 0);
                let row = &mut db.rows[at];
                row.ver += 1;
                row.t = t;
                row.x = x;
                row.name = format!("v{}", row.ver);
            }
            _ => {
                for _ in 0..1 + self.rng.below(5) {
                    let t = self.fresh_t(&db, &mut floor);
                    let x = any_value(&mut self.rng, 0);
                    db.rows.push(Row {
                        id: self.next_id,
                        t,
                        name: "v0".into(),
                        ver: 0,
                        x,
                    });
                    self.next_id += 1;
                }
            }
        }
    }

    fn poll(&mut self, faults: bool) -> usize {
        if faults && self.rng.chance(5) {
            let fault = *self.rng.pick(&FAULTS);
            self.coverage.reach(format!("fault {fault:?}"));
            self.db.lock().unwrap().fault = Some(fault);
        }
        // A backoff lasts seconds, and a blocked source sleeps its timeout: those polls wait no time.
        let mut timeout = match faults && self.rng.chance(3) {
            true => Duration::ZERO,
            false => Duration::from_secs(5),
        };
        if !faults || !self.rng.chance(4) {
            self.source.next_poll = Instant::now();
            self.source.pacer.retry_now();
        } else if self.source.pacer.retrying() {
            self.coverage.reach("a poll waits out its backoff");
            timeout = Duration::ZERO;
        }
        if self.source.blocked {
            timeout = Duration::ZERO;
        }
        let max_rows = 1 + self.rng.below(self.case.batch as usize * 2 + 2);
        let max_bytes = *self.rng.pick(&[1, 64, usize::MAX]);
        let known = self.source.from_known;
        let records = self.source.poll_batch(max_rows, max_bytes, timeout).unwrap();
        if !known && self.source.from_known {
            self.begun = self.source.from.clone();
        }
        if self.source.running.is_some() {
            self.coverage.reach("a statement outlasts the poll");
        }
        if self.source.wide {
            self.coverage.reach("a wide read");
        }
        if self.source.pacer.error().is_some() {
            self.coverage.reach("a failed poll");
        }
        if records.len() == 1 && max_bytes == 1 && max_rows > 1 {
            self.coverage.reach("a record over the bytes alone");
        }
        assert!(
            records.len() <= max_rows,
            "{} records past max_rows {max_rows}",
            records.len()
        );
        self.delivered.extend(records.iter().map(version));
        let n = records.len();
        self.handed.extend(records);
        n
    }

    /// Commits the records of one hand-out up to a random one of them.
    fn commit(&mut self) {
        let Some(pick) = self.rng.index(self.handed.len()) else {
            return;
        };
        let (partition, offset) = (self.handed[pick].partition, self.handed[pick].offset);
        if self
            .handed
            .iter()
            .any(|r| r.partition == partition && r.offset > offset)
        {
            self.coverage.reach("a hand-out committed in part");
        }
        self.commit_where(|r| r.partition == partition && r.offset <= offset);
    }

    fn commit_where(&mut self, done: impl Fn(&SourceRecord) -> bool) {
        let mut offsets = CommitMap::new();
        for r in self.handed.iter().filter(|r| done(r)) {
            let at = TopicPartition {
                topic: r.topic.clone(),
                partition: r.partition,
            };
            let o = offsets.entry(at).or_insert(r.offset);
            *o = (*o).max(r.offset);
        }
        self.handed.retain(|r| !done(r));
        self.source.commit(&offsets).unwrap();
        if let Some(delta) = self.source.take_progress() {
            if !delta.removed.is_empty() {
                self.coverage.reach("a deletion kept");
            }
            keep(&mut self.kept, delta);
        }
    }

    /// A new holder of the source, from what the leader kept. What was handed out and not committed is read again.
    fn restart(&mut self) {
        self.coverage.reach("restart");
        if let Some(delta) = self.source.take_progress() {
            keep(&mut self.kept, delta);
        }
        let driver = Fake {
            db: self.db.clone(),
            dialect: self.case.dialect,
        };
        self.source = SqlSource::new(driver, self.config.clone());
        let known = self.source.from_known;
        self.source
            .resume(self.kept.clone())
            .expect("a source resumes from what it kept");
        if !known && self.source.from_known {
            self.begun = self.source.from.clone();
        }
        self.handed.clear();
        self.restarted = true;
    }

    /// Polls with nothing going wrong and every record committed, until a poll finds nothing new.
    fn drain(&mut self) {
        self.source.release().unwrap();
        {
            let mut db = self.db.lock().unwrap();
            db.fault = None;
            db.now += i64::from(db.settle.unwrap_or(0)) + 1;
        }
        let mut quiet = 0;
        for _ in 0..100_000 {
            let n = self.poll(false);
            self.commit_where(|_| true);
            quiet = match n == 0 && self.source.pending.is_none() && self.source.running.is_none() {
                true => quiet + 1,
                false => 0,
            };
            if quiet == 3 {
                return;
            }
        }
        panic!("the source never stopped reading");
    }
}

fn engine_seed(seed: u64, coverage: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let case = case(&mut rng, coverage);
    let (position, settle) = match &case.mode {
        Mode::Incrementing(p) | Mode::UpdatedAt(p) => (p.columns.clone(), p.settle_secs),
        Mode::Comparing { .. } | Mode::EveryPoll => (Vec::new(), None),
    };
    let db = Arc::new(Mutex::new(Db {
        rows: Vec::new(),
        now: 100,
        settle,
        position,
        fault: None,
    }));
    let text = match case.last_filter {
        true => "SELECT id, t, name, ver, x FROM events WHERE :last.t IS NULL OR t >= :last.t",
        false => "SELECT id, t, name, ver, x FROM events",
    };
    let config = SqlSourceConfig {
        query: Query::parse(text, case.dialect).unwrap(),
        mode: case.mode.clone(),
        fields: Arc::from(vec![
            ("id".to_string(), DataType::I64),
            ("t".to_string(), DataType::I64),
            ("name".to_string(), DataType::Str),
            ("ver".to_string(), DataType::I64),
            ("x".to_string(), case.x_type.clone()),
        ]),
        poll_interval: Duration::ZERO,
        batch: case.batch,
        place: Arc::from("postgres://db/events"),
    };
    let driver = Fake {
        db: db.clone(),
        dialect: case.dialect,
    };
    let begun = match &case.mode {
        Mode::Incrementing(Position {
            start: Start::From(v), ..
        })
        | Mode::UpdatedAt(Position {
            start: Start::From(v), ..
        }) => Some(v.clone()),
        _ => None,
    };
    let mut run = Run {
        rng,
        coverage,
        source: SqlSource::new(driver, config.clone()),
        config,
        db,
        next_id: 1,
        sharing: HashMap::new(),
        latest: 0,
        handed: Vec::new(),
        delivered: Vec::new(),
        kept: Progress::default(),
        restarted: false,
        begun,
        case,
    };
    for _ in 0..run.rng.below(30) {
        run.change();
    }
    let steps = 20 + run.rng.below(150);
    for _ in 0..steps {
        match run.rng.below(20) {
            0..=4 => run.change(),
            5 => {
                let mut db = run.db.lock().unwrap();
                db.now += 1 + run.rng.below(3) as i64;
            }
            6..=12 => {
                run.poll(true);
            }
            13..=16 => run.commit(),
            17 => {
                run.coverage.reach("block");
                run.source.block().unwrap();
            }
            18 => run.source.release().unwrap(),
            _ if run.rng.chance(3) => run.restart(),
            _ => {}
        }
    }
    check(run)
}

/// What reached the destination, against the table the source last read.
fn check(mut run: Run<'_>) -> Result<(), String> {
    let batch = run.case.batch as usize;
    if matches!(run.case.mode, Mode::EveryPoll) {
        run.source.release().unwrap();
        run.db.lock().unwrap().fault = None;
        // What was read before is handed out first; then one poll of the table as it stands.
        while run.source.pending.is_some() || run.source.running.is_some() {
            run.poll(false);
        }
        run.commit_where(|_| true);
        let from = run.delivered.len();
        let answered = run.source.answered;
        while run.source.answered == answered || run.source.pending.is_some() {
            run.poll(false);
            run.commit_where(|_| true);
            if run.source.pacer.error().is_some() {
                break;
            }
        }
        let rows = run.db.lock().unwrap().rows.clone();
        if rows.len() > batch {
            run.coverage.reach("every poll over its batch");
            return match run.source.connection_error() {
                Some(_) => Ok(()),
                None => Err(format!("{} rows read every poll past a batch of {batch}", rows.len())),
            };
        }
        let mut sent = run.delivered[from..].to_vec();
        let mut table: Vec<(i64, i64)> = rows.iter().map(|r| (r.id, r.ver)).collect();
        sent.sort();
        table.sort();
        return match sent == table {
            true => Ok(()),
            false => Err(format!("a poll sent {sent:?} of a table holding {table:?}")),
        };
    }
    run.drain();
    if let Some(why) = run.source.connection_error() {
        return Err(format!("the source still fails once nothing goes wrong: {why}"));
    }
    if !run.restarted {
        let mut seen = HashSet::new();
        if let Some(twice) = run.delivered.iter().find(|v| !seen.insert(**v)) {
            return Err(format!(
                "row {} version {} was sent twice by one holder",
                twice.0, twice.1
            ));
        }
    }
    let rows = run.db.lock().unwrap().rows.clone();
    let first = run.config.position().first().cloned();
    // Without a tie-break, rows sharing one value past twice the batch are skipped, by design.
    if run.config.position().len() == 1 && first.as_deref() == Some("t") && run.sharing.values().any(|n| *n > 2 * batch)
    {
        run.coverage.reach("a run past twice the batch");
        return Ok(());
    }
    let begun = match &run.begun {
        Some(Value::Int(v)) => Some(*v),
        Some(other) => return Err(format!("the source began at {other:?}")),
        None => None,
    };
    let delivered: HashSet<(i64, i64)> = run.delivered.iter().copied().collect();
    for row in &rows {
        if let (Some(first), Some(begun)) = (&first, begun)
            && row.get(first) < begun
        {
            continue;
        }
        if !delivered.contains(&(row.id, row.ver)) {
            return Err(format!(
                "row {} version {} (t {}) was never sent; the source began at {begun:?}",
                row.id, row.ver, row.t
            ));
        }
    }
    if let Mode::Comparing { .. } = run.case.mode {
        let mut kept: Vec<&str> = run.kept.added.iter().map(|e| e.item.as_str()).collect();
        let ids: Vec<String> = rows
            .iter()
            .map(|r| serde_json::to_string(&[Value::Int(r.id)]).unwrap())
            .collect();
        let mut ids: Vec<&str> = ids.iter().map(String::as_str).collect();
        kept.sort();
        ids.sort();
        if kept != ids {
            return Err(format!("the ledger kept {kept:?} for a table of {ids:?}"));
        }
    }
    Ok(())
}

#[test]
fn every_mode_reads_every_row_it_promises_whatever_the_database_does() {
    let mut coverage = Coverage::new(
        [
            "mode incrementing",
            "mode updated at",
            "mode comparing",
            "mode every poll",
            "position unique",
            "position of one column",
            "position with a tie-break",
            "start beginning",
            "start now",
            "start from",
            "settle",
            ":last filter",
            "a row deleted",
            "a row updated",
            "a statement outlasts the poll",
            "a poll waits out its backoff",
            "a wide read",
            "a failed poll",
            "a record over the bytes alone",
            "a hand-out committed in part",
            "a deletion kept",
            "a run past twice the batch",
            "every poll over its batch",
            "block",
            "restart",
        ]
        .into_iter()
        .map(String::from)
        .chain(FAULTS.iter().map(|f| format!("fault {f:?}")))
        .chain(x_types().into_iter().map(|(name, _)| format!("x {name}")))
        .chain(["dialect Postgres".into(), "dialect MySql".into()]),
    );
    fuzz::run(0..400, |seed| engine_seed(seed, &mut coverage));
    coverage.assert_complete();
}

/// Draws each option of one source: from its sound values, or, for a seed that tries to break the check, now and
/// then from its unsound ones or a value of the wrong shape.
struct Options<'a> {
    rng: Rng,
    breaking: bool,
    coverage: &'a mut Coverage,
}

impl Options<'_> {
    fn pick(&mut self, sound: &[serde_json::Value], unsound: &[serde_json::Value]) -> serde_json::Value {
        use serde_json::json;
        if !self.breaking || !self.rng.chance(4) {
            return self.rng.pick(sound).clone();
        }
        let shapes = [
            json!(null),
            json!(""),
            json!(-1),
            json!(1.5),
            json!(u64::MAX),
            json!([]),
            json!({}),
            json!(true),
        ];
        match unsound.is_empty() || self.rng.chance(3) {
            true => self.rng.pick(&shapes).clone(),
            false => self.rng.pick(unsound).clone(),
        }
    }

    /// Some of `names`, in any order: a position's columns, or a key's.
    fn columns(&mut self, names: &[&str]) -> serde_json::Value {
        let mut out: Vec<&str> = Vec::new();
        for _ in 0..1 + self.rng.below(2) {
            let name = *self.rng.pick(names);
            if self.breaking || !out.contains(&name) {
                out.push(name);
            }
        }
        if self.breaking && self.rng.chance(4) {
            out = vec![*self.rng.pick(&["nope", "", "__x"])];
        }
        serde_json::json!(out)
    }
}

/// An SQL source of either kind, each option drawn from what the file may hold.
fn source_options(o: &mut Options<'_>) -> serde_json::Value {
    use serde_json::json;
    let declared = [
        ("id", "i64"),
        ("occurred_at", "datetime"),
        ("name", "str"),
        ("tags", "array<str>"),
    ];
    let mut fields: Vec<serde_json::Value> = declared
        .iter()
        .filter(|_| o.rng.chance(4) || !o.breaking)
        .map(|(name, ty)| json!({"name": name, "type": ty}))
        .collect();
    if o.breaking && o.rng.chance(2) {
        let name = o.pick(&[json!("id")], &[json!("__x"), json!(""), json!("a b")]);
        let ty = o.pick(
            &[json!("i64")],
            &[json!("null"), json!("nope"), json!("{a: i64}"), json!("variant")],
        );
        fields.push(json!({"name": name, "type": ty}));
    }
    let incrementing = ["id", "occurred_at"];
    let mode = match o.rng.below(5) {
        0..=2 => {
            let kind = *o.rng.pick(&["incrementing", "updatedAt"]);
            o.coverage.reach(format!("config mode {kind}"));
            let mut mode = json!({"type": kind, "position": o.columns(&incrementing)});
            // Updated at reads by a time.
            if kind == "updatedAt" && !o.breaking {
                mode["position"] = json!(["occurred_at", "id"][..1 + o.rng.below(2)]);
            }
            let first = mode["position"][0].as_str().unwrap_or("").to_string();
            if (first == "occurred_at" || o.breaking) && o.rng.chance(2) {
                o.coverage.reach("config settleSecs");
                mode["settleSecs"] = o.pick(&[json!(0), json!(30), json!(u32::MAX)], &[json!(-1), json!(u64::MAX)]);
            }
            if o.rng.chance(2) {
                o.coverage.reach("config start");
                mode["start"] = o.pick(&[json!("now"), json!("beginning"), json!("from")], &[json!("later")]);
            }
            if mode["start"] == json!("from") || (o.breaking && o.rng.chance(3)) {
                o.coverage.reach("config startFrom");
                let sound = match first.as_str() {
                    "id" => vec![json!(7), json!(i64::MIN), json!(i64::MAX)],
                    _ => vec![json!("2026-10-12T00:00:00Z"), json!("2026-10-12T00:00:00.123456+02:00")],
                };
                mode["startFrom"] = o.pick(
                    &sound,
                    &[
                        json!("7"),
                        json!(9_007_199_254_740_993u64),
                        json!(1.5),
                        json!("2026-10-12T00:00:00"),
                        json!("yesterday"),
                        json!(7),
                    ],
                );
            }
            mode
        }
        3 => {
            o.coverage.reach("config mode comparing");
            json!({"type": "comparing", "key": o.columns(&["id", "name"])})
        }
        _ => {
            o.coverage.reach("config mode everyPoll");
            o.pick(
                &[json!({"type": "everyPoll"})],
                &[
                    json!({"type": "sometimes"}),
                    json!({"type": "everyPoll", "key": ["id"]}),
                ],
            )
        }
    };
    let positioned = mode["position"].is_array();
    let mut sound_queries = vec![
        json!("SELECT id, occurred_at, name, tags FROM events"),
        json!("WITH e AS (SELECT * FROM events) SELECT * FROM e;"),
        json!("SELECT * FROM events WHERE name <> 'ORDER BY' -- LIMIT 1"),
    ];
    if positioned {
        sound_queries.push(json!("SELECT * FROM events WHERE :last.id IS NULL OR id >= :last.id"));
    }
    let query = o.pick(
        &sound_queries,
        &[
            json!("SELECT * FROM events WHERE id > :last.nope"),
            json!("SELECT * FROM events WHERE id > :last.id"),
            json!("SELECT * FROM events ORDER BY id"),
            json!("SELECT * FROM events LIMIT 5"),
            json!("SELECT * FROM events WHERE id = $1"),
            json!("SELECT 1; DROP TABLE events"),
            json!("SELECT (1"),
            json!("SELECT ':last.x"),
            json!("DELETE FROM events"),
            json!("SELECT :last."),
            json!("SELECT 1 /*! , 2 */"),
            json!("   "),
        ],
    );
    let kind = *o.rng.pick(&["postgres", "mysql"]);
    o.coverage.reach(format!("config kind {kind}"));
    let mut source = json!({
        "id": "pg", "type": kind,
        "host": o.pick(&[json!("db"), json!(" db.internal "), json!("::1"), json!("10.0.0.7")], &[json!("")]),
        "database": o.pick(&[json!("auth")], &[json!("")]),
        "user": o.pick(&[json!("reader")], &[json!("")]),
        "query": query, "mode": mode, "fields": fields,
    });
    let pem = {
        let key = rcgen::KeyPair::generate().unwrap();
        rcgen::CertificateParams::new(vec!["fuzz-ca".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap()
            .pem()
    };
    let optional: [(&str, &[serde_json::Value], &[serde_json::Value]); 8] = [
        (
            "port",
            &[json!(5432), json!(1), json!(65535)],
            &[json!(0), json!(70000)],
        ),
        (
            "password",
            &[json!({"secret": "pg", "version": 2})],
            &[json!({"secret": ""})],
        ),
        ("tls", &[json!("require"), json!("disable")], &[json!("prefer")]),
        ("certificate", &[json!("pg-client")], &[json!(""), json!(" ")]),
        ("authorities", &[json!(pem)], &[json!("nope"), json!("")]),
        ("trustPublic", &[json!(true)], &[json!("yes")]),
        ("pollIntervalSecs", &[json!(1), json!(60)], &[json!(0), json!(u64::MAX)]),
        (
            "batch",
            &[json!(1), json!(5000), json!(100_000)],
            &[json!(0), json!(100_001)],
        ),
    ];
    for (key, sound, unsound) in optional {
        if (o.breaking || !sound.is_empty()) && o.rng.chance(2) {
            o.coverage.reach(format!("config {key}"));
            source[key] = match sound.is_empty() {
                true => o.rng.pick(unsound).clone(),
                false => o.pick(sound, unsound),
            };
        }
    }
    // TLS settings are for an encrypted connection; a sound draw turns encryption on for them. Whether public roots
    // are trusted says nothing on its own, so a sound draw gives it an authority to stand beside.
    if !o.breaking
        && ["certificate", "authorities", "trustPublic"]
            .iter()
            .any(|k| source.get(k).is_some())
    {
        source["tls"] = json!("require");
        if source.get("trustPublic").is_some() && source.get("certificate").is_none() && source.get("authorities").is_none() {
            source["authorities"] = json!(pem);
        }
    }
    source
}

#[test]
fn any_sql_source_a_file_declares_loads_or_is_refused() {
    let mut coverage = Coverage::new([
        "config kind postgres",
        "config kind mysql",
        "config mode incrementing",
        "config mode updatedAt",
        "config mode comparing",
        "config mode everyPoll",
        "config settleSecs",
        "config start",
        "config startFrom",
        "config port",
        "config password",
        "config tls",
        "config certificate",
        "config authorities",
        "config trustPublic",
        "config pollIntervalSecs",
        "config batch",
        "loads",
        "refused",
    ]);
    fuzz::run(0..3000, |seed| {
        let mut rng = Rng::seeded(seed);
        let breaking = rng.chance(2);
        let mut options = Options {
            rng,
            breaking,
            coverage: &mut coverage,
        };
        let source = source_options(&mut options);
        let json = serde_json::json!({"version": 1, "sources": [source]}).to_string();
        match crate::config_file::load_str(&json) {
            Ok(system) => {
                coverage.reach("loads");
                system
                    .compile_topology()
                    .map_err(|e| format!("{json} loads and does not compile: {e:?}"))?;
            }
            Err(e) if !breaking => return Err(format!("{json} is sound and is refused: {e:?}")),
            Err(_) => coverage.reach("refused"),
        }
        Ok(())
    });
    coverage.assert_complete();
}
