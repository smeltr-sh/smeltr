//! Generated cases against the SQL source on a real PostgreSQL, MySQL and MariaDB, as `just live-up` runs them.
//!
//! Each seed makes a table and draws a mode, a position, where it starts and a batch. Then, in any order, the owner
//! inserts, updates and deletes rows while the source, signed in as the reader, polls, hands out any number of
//! records, commits some of them, blocks, and is restarted from the progress it kept. Once the table stops changing
//! and the source reads nothing new, what reached the destination must hold:
//!
//! - every row's latest version the mode promises, each read past where the source began;
//! - no row version twice from one holder;
//! - for comparing values, a ledger that keeps exactly the table's keys;
//! - for every poll, the table as it stands, or a refusal once it outgrows the batch.
//!
//! Point it at `POSTGRES_TEST_HOST`, `MYSQL_TEST_HOST` and `MARIADB_TEST_HOST`. Under `SMELTR_LIVE_TESTS=required` a
//! missing one fails rather than skips.

use crate::harness::fuzz::{self, Coverage, Rng};
use crate::live;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use smeltr::core::column::DataType;
use smeltr::io::mysql::{MySqlConnection, MySqlDriver};
use smeltr::io::postgres::{PostgresConnection, PostgresDriver};
use smeltr::io::sql::{Dialect, Query};
use smeltr::io::sql_source::{Mode, Position, SqlSource, SqlSourceConfig, Start, Tls};
use smeltr::io::values::Value;
use smeltr::io::{CommitMap, Progress, Source, SourceRecord, TopicPartition};
use smeltr::plan::topology::{HeldSecrets, SecretRef, SourceMeta};

const OWNER: &str = "smeltr";
const READER: &str = "smeltr_reader";
const PASSWORD: &str = "smeltr-live-1";
const DATABASE: &str = "smeltr";

/// Each seed makes a table and a few dozen statements; this many run in under a minute against one server.
const SEEDS: std::ops::Range<u64> = 0..30;

/// How long a time position waits for late commits here.
const SETTLE_SECS: u32 = 1;

/// The record each row becomes.
fn fields() -> Vec<(String, DataType)> {
    [
        ("id", DataType::I64),
        ("t", DataType::I64),
        ("at", DataType::DateTime),
        ("name", DataType::Str),
        ("ver", DataType::I64),
    ]
    .into_iter()
    .map(|(n, t)| (n.to_string(), t))
    .collect()
}

/// One server: the owner writing the table, and sources reading it as the reader.
trait Server {
    fn dialect(&self) -> Dialect;
    fn run(&mut self, sql: &str);
    /// The table's columns, in this server's types.
    fn columns(&self) -> &'static str;
    /// The current time, to the microsecond.
    fn now(&self) -> &'static str;
    fn source(&self, config: SqlSourceConfig) -> Box<dyn Source>;
}

fn secrets() -> HeldSecrets {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("password".to_string(), 1, PASSWORD.to_string())]);
    secrets
}

fn password() -> Option<SecretRef> {
    Some(SecretRef::LeaderStored {
        name: "password".to_string(),
        version: 1,
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

struct Postgres {
    host: String,
    port: u16,
    runtime: tokio::runtime::Runtime,
    client: tokio_postgres::Client,
}

impl Postgres {
    fn connect(host: String, port: u16) -> Postgres {
        let runtime = runtime();
        let config = format!("host={host} port={port} dbname={DATABASE} user={OWNER} password={PASSWORD}");
        let client = runtime.block_on(async {
            let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
                .await
                .expect("the owner signs in");
            tokio::spawn(connection);
            client
        });
        Postgres {
            host,
            port,
            runtime,
            client,
        }
    }
}

impl Server for Postgres {
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn run(&mut self, sql: &str) {
        self.runtime
            .block_on(self.client.batch_execute(sql))
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    fn columns(&self) -> &'static str {
        "id bigint PRIMARY KEY, t bigint NOT NULL, at timestamptz NOT NULL, name text NOT NULL, ver bigint NOT NULL"
    }

    fn now(&self) -> &'static str {
        "now()"
    }

    fn source(&self, config: SqlSourceConfig) -> Box<dyn Source> {
        let connection = PostgresConnection {
            host: self.host.clone(),
            port: self.port,
            database: DATABASE.to_string(),
            user: READER.to_string(),
            password: password(),
            tls: Tls::Disable,
            identity: None,
        };
        let driver = PostgresDriver::new(connection, &secrets()).expect("a driver");
        Box::new(SqlSource::new(driver, config))
    }
}

struct MySql {
    host: String,
    port: u16,
    runtime: tokio::runtime::Runtime,
    conn: mysql_async::Conn,
}

impl MySql {
    fn connect(host: String, port: u16) -> MySql {
        let runtime = runtime();
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(host.clone())
            .tcp_port(port)
            .db_name(Some(DATABASE))
            .user(Some(OWNER))
            .pass(Some(PASSWORD))
            .prefer_socket(false);
        let conn = runtime
            .block_on(mysql_async::Conn::new(opts))
            .unwrap_or_else(|e| panic!("the owner signs in: {e}"));
        MySql {
            host,
            port,
            runtime,
            conn,
        }
    }
}

impl Server for MySql {
    fn dialect(&self) -> Dialect {
        Dialect::MySql
    }

    fn run(&mut self, sql: &str) {
        self.runtime
            .block_on(self.conn.query_drop(sql))
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    fn columns(&self) -> &'static str {
        // The source reads in UTC; the owner writes in it too.
        "id BIGINT PRIMARY KEY, t BIGINT NOT NULL, at DATETIME(6) NOT NULL, name VARCHAR(64) NOT NULL, \
         ver BIGINT NOT NULL"
    }

    fn now(&self) -> &'static str {
        "UTC_TIMESTAMP(6)"
    }

    fn source(&self, config: SqlSourceConfig) -> Box<dyn Source> {
        let connection = MySqlConnection {
            host: self.host.clone(),
            port: self.port,
            database: DATABASE.to_string(),
            user: READER.to_string(),
            password: password(),
            tls: Tls::Disable,
            identity: None,
        };
        let driver = MySqlDriver::new(connection, &secrets()).expect("a driver");
        Box::new(SqlSource::new(driver, config))
    }
}

/// A table name no earlier run used.
fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// A row as the test last wrote it.
#[derive(Clone, Debug)]
struct Row {
    t: i64,
    ver: i64,
}

/// What one seed reads.
struct Case {
    mode: Mode,
    updates: bool,
    batch: u32,
    last_filter: bool,
}

fn case(rng: &mut Rng, coverage: &mut Coverage) -> Case {
    let (mode, updates) = match rng.below(6) {
        n @ (0 | 1) => {
            let positions: &[&[&str]] = match n {
                0 => &[&["id"], &["t"], &["t", "id"]],
                _ => &[&["t"], &["t", "id"], &["at"], &["at", "id"]],
            };
            let columns: Vec<String> = rng.pick(positions).iter().map(|c| c.to_string()).collect();
            match columns.as_slice() {
                [only] if only == "id" => coverage.reach("position unique"),
                [_] => coverage.reach("position of one column"),
                _ => coverage.reach("position with a tie-break"),
            }
            // A time position waits for late commits; a start is drawn only for a position of integers.
            let time = columns[0] == "at";
            let start = match (time, rng.below(3)) {
                (true, _) | (false, 0) => Start::Beginning,
                (false, 1) => Start::Now,
                (false, _) => Start::From(Value::Int(rng.below(30) as i64)),
            };
            coverage.reach(match (&start, time) {
                (_, true) => "settle".to_string(),
                (Start::Beginning, _) => "start beginning".to_string(),
                (Start::Now, _) => "start now".to_string(),
                (Start::From(_), _) => "start from".to_string(),
            });
            let position = Position {
                settle_secs: time.then_some(SETTLE_SECS),
                start,
                columns,
            };
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
    Case {
        mode,
        updates,
        batch: *rng.pick(&[1, 2, 3, 5, 20]),
        last_filter,
    }
}

/// One seed against one server.
struct Run<'a> {
    rng: Rng,
    coverage: &'a mut Coverage,
    server: &'a mut dyn Server,
    table: String,
    case: Case,
    config: SqlSourceConfig,
    source: Box<dyn Source>,
    meta: SourceMeta,
    rows: BTreeMap<i64, Row>,
    next_id: i64,
    /// The largest `t` ever written: `t` only grows.
    latest: i64,
    /// Row versions written with each value of the position's first column, by one statement or many.
    sharing: HashMap<String, usize>,
    handed: Vec<SourceRecord>,
    delivered: Vec<(i64, i64)>,
    kept: Progress,
    restarted: bool,
    /// A blocked source sleeps a poll's whole timeout.
    blocked: bool,
    /// Where the source began, from the progress it kept; `None` for the beginning.
    begun: Option<i64>,
}

impl Run<'_> {
    fn first(&self) -> Option<&str> {
        self.config.position().first().map(String::as_str)
    }

    /// Notes the rows sharing a value of the first column. Every statement writes one `at` to all its rows.
    fn share(&mut self, t: i64, statement: usize) {
        let key = match self.first() {
            Some("at") => format!("at{statement}"),
            _ => format!("t{t}"),
        };
        *self.sharing.entry(key).or_default() += 1;
    }

    fn change(&mut self, statement: usize) {
        let n = self.rows.len();
        let now = self.server.now();
        // Every poll refuses a table past its batch, and backs off for seconds; the table outgrows it only at the end.
        let room = match self.case.mode {
            Mode::EveryPoll => self.case.batch as usize - n,
            _ => usize::MAX,
        };
        match self.rng.below(if self.case.updates { 4 } else { 2 }) {
            0 if room == 0 => self.change(statement),
            1 if n > 0 => {
                self.coverage.reach("a row deleted");
                let id = *self.rows.keys().nth(self.rng.below(n)).unwrap();
                self.rows.remove(&id);
                self.server.run(&format!("DELETE FROM {} WHERE id = {id}", self.table));
            }
            2 | 3 if n > 0 => {
                self.coverage.reach("a row updated");
                let id = *self.rows.keys().nth(self.rng.below(n)).unwrap();
                self.latest += 1;
                let t = self.latest;
                let row = self.rows.get_mut(&id).unwrap();
                row.ver += 1;
                row.t = t;
                let ver = row.ver;
                self.share(t, statement);
                self.server.run(&format!(
                    "UPDATE {} SET t = {t}, at = {now}, name = 'v{ver}', ver = {ver} WHERE id = {id}",
                    self.table
                ));
            }
            _ => {
                let base = self.latest + 1;
                let mut values = Vec::new();
                for _ in 0..(1 + self.rng.below(5)).min(room) {
                    let t = base + self.rng.below(2) as i64;
                    self.latest = self.latest.max(t);
                    let id = self.next_id;
                    self.next_id += 1;
                    self.rows.insert(id, Row { t, ver: 0 });
                    self.share(t, statement);
                    values.push(format!("({id}, {t}, {now}, 'v0', 0)"));
                }
                self.server.run(&format!(
                    "INSERT INTO {} (id, t, at, name, ver) VALUES {}",
                    self.table,
                    values.join(", ")
                ));
            }
        }
    }

    fn poll(&mut self, max_rows: usize) -> Result<usize, String> {
        let records = self
            .source
            .poll_batch(
                max_rows,
                usize::MAX,
                match self.blocked {
                    true => Duration::from_millis(1),
                    false => Duration::from_secs(5),
                },
            )
            .map_err(|e| format!("a poll: {e:?}"))?;
        if let Some(why) = self.source.connection_error() {
            return Err(format!("a poll failed: {why}"));
        }
        if records.len() > max_rows {
            return Err(format!("{} records past max_rows {max_rows}", records.len()));
        }
        for r in &records {
            let object = self.meta.record_object(r, "");
            let at = |f: &str| object[f].as_i64().unwrap_or_else(|| panic!("{f} in {object:?}"));
            self.delivered.push((at("id"), at("ver")));
        }
        let n = records.len();
        self.handed.extend(records);
        // The leader takes progress with every heartbeat, so it sees where a source starting now began.
        self.keep();
        Ok(n)
    }

    fn commit_where(&mut self, done: impl Fn(&SourceRecord) -> bool) {
        let mut offsets = CommitMap::new();
        for r in self.handed.iter().filter(|r| done(r)) {
            let at = offsets
                .entry(TopicPartition::new(r.topic.clone(), r.partition))
                .or_insert(r.offset);
            *at = (*at).max(r.offset);
        }
        self.handed.retain(|r| !done(r));
        self.source.commit(&offsets).expect("committed");
        self.keep();
    }

    /// Folds what the source kept into what the leader holds, as the leader does.
    fn keep(&mut self) {
        let Some(delta) = self.source.take_progress() else {
            return;
        };
        if let Some(cursor) = &delta.cursor
            && let Ok(map) = serde_json::from_str::<BTreeMap<String, Value>>(cursor)
            && let Some(from) = map.get("__from")
        {
            self.begun = match from {
                Value::Int(v) => Some(*v),
                _ => None,
            };
        }
        if delta.cursor.is_some() {
            self.kept.cursor = delta.cursor;
        }
        for entry in delta.added {
            self.kept.added.retain(|e| e.item != entry.item);
            self.kept.added.push(entry);
        }
        for item in delta.removed {
            self.coverage.reach("a deletion kept");
            self.kept.added.retain(|e| e.item != item);
        }
    }

    fn restart(&mut self) {
        self.coverage.reach("restart");
        self.keep();
        self.source = self.server.source(self.config.clone());
        self.source
            .resume(self.kept.clone())
            .expect("a source resumes from what it kept");
        self.handed.clear();
        self.restarted = true;
    }

    /// Polls, committing everything, until three polls in a row find nothing new.
    fn drain(&mut self) -> Result<(), String> {
        self.source.release().expect("released");
        self.blocked = false;
        if self.first() == Some("at") {
            std::thread::sleep(Duration::from_millis(u64::from(SETTLE_SECS) * 1000 + 200));
        }
        let mut quiet = 0;
        for _ in 0..10_000 {
            let n = self.poll(100_000)?;
            self.commit_where(|_| true);
            quiet = if n == 0 { quiet + 1 } else { 0 };
            if quiet == 3 {
                return Ok(());
            }
        }
        Err("the source never stopped reading".into())
    }
}

fn seed(server: &mut dyn Server, seed: u64, coverage: &mut Coverage) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let case = case(&mut rng, coverage);
    let table = unique("fuzz");
    server.run(&format!("CREATE TABLE {table} ({})", server.columns()));
    if server.dialect() == Dialect::Postgres {
        server.run(&format!("GRANT SELECT ON {table} TO {READER}"));
    }
    let text = match case.last_filter {
        true => format!("SELECT id, t, at, name, ver FROM {table} WHERE :last.t IS NULL OR t >= :last.t"),
        false => format!("SELECT id, t, at, name, ver FROM {table}"),
    };
    let config = SqlSourceConfig {
        query: Query::parse(&text, server.dialect()).expect("a query"),
        mode: case.mode.clone(),
        fields: fields().into(),
        poll_interval: Duration::ZERO,
        batch: case.batch,
        place: Arc::from("sql-live-fuzz"),
    };
    let begun = match &case.mode {
        Mode::Incrementing(Position {
            start: Start::From(Value::Int(v)),
            ..
        })
        | Mode::UpdatedAt(Position {
            start: Start::From(Value::Int(v)),
            ..
        }) => Some(*v),
        _ => None,
    };
    let meta = SourceMeta::NONE.declaring(fields()).expect("a record");
    let source = server.source(config.clone());
    let mut run = Run {
        rng,
        coverage,
        server,
        table,
        case,
        config,
        source,
        meta,
        rows: BTreeMap::new(),
        next_id: 1,
        latest: 0,
        sharing: HashMap::new(),
        handed: Vec::new(),
        delivered: Vec::new(),
        kept: Progress::default(),
        restarted: false,
        blocked: false,
        begun,
    };
    for statement in 0..run.rng.below(6) {
        run.change(statement);
    }
    for step in 0..20 + run.rng.below(40) {
        match run.rng.below(12) {
            0..=2 => run.change(1000 + step),
            3..=6 => {
                let max_rows = 1 + run.rng.below(run.case.batch as usize * 2 + 2);
                run.poll(max_rows)?;
            }
            7 | 8 => {
                let Some(pick) = run.rng.index(run.handed.len()) else {
                    continue;
                };
                let (partition, offset) = (run.handed[pick].partition, run.handed[pick].offset);
                if run.handed.iter().any(|r| r.partition == partition && r.offset > offset) {
                    run.coverage.reach("a hand-out committed in part");
                }
                run.commit_where(|r| r.partition == partition && r.offset <= offset);
            }
            9 => {
                run.coverage.reach("block");
                run.source.block().expect("blocked");
                run.blocked = true;
            }
            10 => {
                run.source.release().expect("released");
                run.blocked = false;
            }
            _ => run.restart(),
        }
    }
    let result = check(&mut run);
    let table = run.table.clone();
    run.server.run(&format!("DROP TABLE {table}"));
    result
}

fn check(run: &mut Run<'_>) -> Result<(), String> {
    let batch = run.case.batch as usize;
    if matches!(run.case.mode, Mode::EveryPoll) {
        run.source.release().expect("released");
        run.blocked = false;
        if run.rng.chance(3) {
            run.coverage.reach("every poll over its batch");
            while run.rows.len() <= batch {
                run.case.batch = u32::MAX;
                run.change(2000);
            }
        }
        let table: Vec<(i64, i64)> = run.rows.iter().map(|(id, r)| (*id, r.ver)).collect();
        // The first poll hands out what an earlier one left; the next two read the table as it stands.
        let mut last = Vec::new();
        for _ in 0..3 {
            let from = run.delivered.len();
            match run.poll(100_000) {
                Ok(_) => {}
                Err(why) if table.len() > batch && why.contains("reads at most") => return Ok(()),
                Err(why) => return Err(why),
            }
            run.commit_where(|_| true);
            last = run.delivered[from..].to_vec();
        }
        last.sort();
        return match last == table {
            true => Ok(()),
            false => Err(format!("a poll sent {last:?} of a table holding {table:?}")),
        };
    }
    run.drain()?;
    if !run.restarted {
        let mut seen = HashSet::new();
        if let Some(twice) = run.delivered.iter().find(|v| !seen.insert(**v)) {
            return Err(format!(
                "row {} version {} was sent twice by one holder",
                twice.0, twice.1
            ));
        }
    }
    // Without a tie-break, rows sharing one value past twice the batch are skipped, by design.
    if run.config.position().len() == 1 && run.first() != Some("id") && run.sharing.values().any(|n| *n > 2 * batch) {
        run.coverage.reach("a run past twice the batch");
        return Ok(());
    }
    let delivered: HashSet<(i64, i64)> = run.delivered.iter().copied().collect();
    for (id, row) in &run.rows {
        let first = match run.first() {
            Some("id") => *id,
            Some("t") => row.t,
            _ => i64::MAX,
        };
        if run.begun.is_some_and(|b| first < b) {
            continue;
        }
        if !delivered.contains(&(*id, row.ver)) {
            return Err(format!(
                "row {id} version {} (t {}) was never sent; the source began at {:?}",
                row.ver, row.t, run.begun
            ));
        }
    }
    if let Mode::Comparing { .. } = run.case.mode {
        let mut kept: Vec<String> = run.kept.added.iter().map(|e| e.item.clone()).collect();
        let mut ids: Vec<String> = run
            .rows
            .keys()
            .map(|id| serde_json::to_string(&[Value::Int(*id)]).unwrap())
            .collect();
        kept.sort();
        ids.sort();
        if kept != ids {
            return Err(format!("the ledger kept {kept:?} for a table of {ids:?}"));
        }
    }
    Ok(())
}

fn options() -> Coverage {
    Coverage::new([
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
        "a hand-out committed in part",
        "a deletion kept",
        "block",
        "restart",
        "every poll over its batch",
    ])
}

#[test]
fn live_postgres_reads_every_row_it_promises_however_the_table_changes() {
    let Some((host, port)) = live::postgres("live_postgres_reads_every_row_it_promises_however_the_table_changes")
    else {
        return;
    };
    let mut server = Postgres::connect(host, port);
    let mut coverage = options();
    fuzz::run(SEEDS, |n| seed(&mut server, n, &mut coverage));
    coverage.assert_complete();
}

#[test]
fn live_mysql_and_mariadb_read_every_row_they_promise_however_the_table_changes() {
    const NAME: &str = "live_mysql_and_mariadb_read_every_row_they_promise_however_the_table_changes";
    for address in [live::mysql(NAME), live::mariadb(NAME)].into_iter().flatten() {
        let mut server = MySql::connect(address.0, address.1);
        let mut coverage = options();
        fuzz::run(SEEDS, |n| seed(&mut server, n, &mut coverage));
        coverage.assert_complete();
    }
}
