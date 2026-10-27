//! The SQL source against a real MySQL and a real MariaDB, as `just live-up` runs them: each mode's polls, the
//! progress a commit moves and a resume from it, the read-only transaction every statement runs in, the connection
//! opened again after it breaks, and what the database describes and plans. Every test runs against both servers.
//!
//! The source signs in as `smeltr_reader`, which may only SELECT. The tests write as `smeltr`, the database's owner,
//! each into tables of its own, so a rerun against services left up starts clean.
//!
//! Point it at `MYSQL_TEST_HOST` and `MARIADB_TEST_HOST`. Under `SMELTR_LIVE_TESTS=required` a missing one fails
//! rather than skips.

use crate::live;

use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable as _;
use smeltr::core::column::DataType;
use smeltr::io::mysql::{MySqlConnection, MySqlDriver};
use smeltr::io::sql::{Dialect, Poll, Query, Statement};
use smeltr::io::sql_source::{Driver, Encrypted, Mode, Position, SqlSource, SqlSourceConfig, Start, Verdict};
use smeltr::io::values::Value;
use smeltr::io::{CommitMap, Progress, RecordObject, Source, SourceRecord, TopicPartition};
use smeltr::plan::topology::{HeldSecrets, SecretRef, SourceMeta};

const OWNER: &str = "smeltr";
const READER: &str = "smeltr_reader";
const PASSWORD: &str = "smeltr-live-1";
const DATABASE: &str = "smeltr";
const PLACE: &str = "mysql-live";

/// One server a test runs against.
struct Server {
    name: &'static str,
    host: String,
    port: u16,
}

impl Server {
    fn mariadb(&self) -> bool {
        self.name == "MariaDB"
    }
}

/// MySQL and MariaDB, each when it is up.
fn servers(test_name: &str) -> Vec<Server> {
    [("MySQL", live::mysql(test_name)), ("MariaDB", live::mariadb(test_name))]
        .into_iter()
        .filter_map(|(name, address)| address.map(|(host, port)| Server { name, host, port }))
        .collect()
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

/// A driver signed in as `user`, its password a stored secret.
fn driver_with(server: &Server, user: &str, encrypted: Encrypted) -> Result<MySqlDriver, smeltr::io::IoError> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("mysql-password".to_string(), 1, PASSWORD.to_string())]);
    let connection = MySqlConnection {
        host: server.host.clone(),
        port: server.port,
        database: DATABASE.to_string(),
        user: user.to_string(),
        password: Some(SecretRef::LeaderStored {
            name: "mysql-password".to_string(),
            version: 1,
        }),
        encrypted,
        tls: None,
    };
    MySqlDriver::new(connection, &secrets)
}

fn driver(server: &Server, user: &str) -> MySqlDriver {
    driver_with(server, user, Encrypted::Disable).expect("a driver")
}

/// The database's owner, for writing the rows a source reads.
struct Owner {
    runtime: tokio::runtime::Runtime,
    conn: mysql_async::Conn,
}

impl Owner {
    fn connect(server: &Server) -> Owner {
        Owner::sign_in(server, OWNER)
    }

    /// Signed in as `user` rather than the owner.
    fn sign_in(server: &Server, user: &str) -> Owner {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(server.host.clone())
            .tcp_port(server.port)
            .db_name(Some(DATABASE))
            .user(Some(user))
            .pass(Some(PASSWORD))
            .prefer_socket(false);
        let conn = runtime
            .block_on(mysql_async::Conn::new(opts))
            .unwrap_or_else(|e| panic!("the owner signs in to {}: {e}", server.name));
        Owner { runtime, conn }
    }

    fn run(&mut self, sql: &str) {
        self.runtime
            .block_on(self.conn.query_drop(sql))
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// The first column of the first row `sql` returns, as text.
    fn one(&mut self, sql: &str) -> String {
        self.runtime
            .block_on(self.conn.query_first::<String, _>(sql))
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .expect("a row")
    }

    /// Makes `table` with `columns`. The reader may read every table of the database.
    fn table(&mut self, table: &str, columns: &str) {
        self.run(&format!("CREATE TABLE {table} ({columns})"));
    }
}

fn config(query: &str, mode: Mode, fields: &[(&str, DataType)]) -> SqlSourceConfig {
    SqlSourceConfig {
        query: Query::parse(query, Dialect::MySql).expect("a query"),
        mode,
        fields: fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect(),
        poll_interval: Duration::from_millis(100),
        batch: 100,
        place: Arc::from(PLACE),
    }
}

fn source(server: &Server, config: SqlSourceConfig) -> SqlSource<MySqlDriver> {
    SqlSource::new(driver(server, READER), config)
}

/// Polls until records come, failing on a poll that errs or on none within the deadline.
fn records(source: &mut SqlSource<MySqlDriver>) -> Vec<SourceRecord> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let got = source
            .poll_batch(10_000, usize::MAX, Duration::from_millis(50))
            .expect("a poll");
        if let Some(e) = source.connection_error() {
            panic!("the poll failed: {e}");
        }
        if !got.is_empty() {
            return got;
        }
        assert!(Instant::now() < deadline, "no records within 30s");
    }
}

/// Polls until a poll fails, failing on any record or on no failure within the deadline. The failure's message.
fn failure(source: &mut SqlSource<MySqlDriver>, what: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    while source.connection_error().is_none() {
        let got = source
            .poll_batch(10_000, usize::MAX, Duration::from_millis(50))
            .expect("a poll");
        assert!(got.is_empty(), "{what} sends no record");
        assert!(Instant::now() < deadline, "the poll never failed");
    }
    source.connection_error().unwrap().to_string()
}

/// Commits every record of `records`.
fn commit(source: &mut SqlSource<MySqlDriver>, records: &[SourceRecord]) {
    let mut offsets = CommitMap::new();
    for r in records {
        let at = offsets
            .entry(TopicPartition::new(r.topic.clone(), r.partition))
            .or_insert(r.offset);
        *at = (*at).max(r.offset);
    }
    source.commit(&offsets).expect("committed");
}

/// Each record's object, as a pipeline with no steps would see it.
fn objects(fields: &[(&str, DataType)], records: &[SourceRecord]) -> Vec<RecordObject> {
    let meta = SourceMeta::NONE
        .declaring(fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect())
        .expect("a record");
    records.iter().map(|r| meta.record_object(r, "")).collect()
}

fn ids(objects: &[RecordObject]) -> Vec<i64> {
    objects.iter().map(|o| o["id"].as_i64().expect("an id")).collect()
}

/// The statement a source with no position runs for `query`.
fn plain_statement(query: &str) -> Statement {
    smeltr::io::sql::statement(
        &Query::parse(query, Dialect::MySql).expect("a query"),
        &Poll {
            position: &[],
            after: false,
            start: false,
            settle_secs: None,
            batch: 1000,
            fields: &[],
        },
    )
}

fn bare(sql: &str) -> Statement {
    Statement {
        sql: sql.to_string(),
        binds: Vec::new(),
    }
}

/// BOOL is TINYINT(1), which holds any TINYINT: a value but 0 or 1 is read as the number it is.
#[test]
fn a_boolean_column_holding_another_number_reads_as_that_number() {
    for server in servers("a_boolean_column_holding_another_number_reads_as_that_number") {
        let mut owner = Owner::connect(&server);
        let table = unique("flags");
        owner.table(&table, "id bigint PRIMARY KEY, flag boolean");
        owner.run(&format!("INSERT INTO {table} VALUES (1, 0), (2, 1), (3, 2)"));
        let mut d = driver(&server, READER);
        let rows = d
            .run(&bare(&format!("SELECT flag FROM {table} ORDER BY id")), &[])
            .expect("read");
        let flags: Vec<Value> = rows.rows.into_iter().map(|mut r| r.remove(0)).collect();
        assert_eq!(
            flags,
            [Value::Bool(false), Value::Bool(true), Value::Int(2)],
            "{}",
            server.name
        );
    }
}

#[test]
fn incrementing_reads_typed_rows_in_order_and_resumes_past_the_last_committed() {
    for server in servers("incrementing_reads_typed_rows_in_order_and_resumes_past_the_last_committed") {
        let mut owner = Owner::connect(&server);
        let table = unique("incrementing");
        owner.table(
            &table,
            "id bigint PRIMARY KEY, name text, stamp timestamp(6) NULL, amount decimal(10, 2), doc json, ok boolean",
        );
        owner.run(&format!(
            "INSERT INTO {table} VALUES
               (2, 'b', '2024-01-02 03:04:05.5', 12.5, '{{\"a\": 1, \"b\": [\"x\"]}}', true),
               (1, 'a', '2024-01-01 00:00:00', 1, '{{}}', false),
               (3, 'c', NULL, NULL, NULL, NULL)"
        ));
        // MariaDB's JSON is LONGTEXT under another name: its text, not a value.
        let doc = if server.mariadb() {
            DataType::Str
        } else {
            DataType::Variant
        };
        let fields = [
            ("id", DataType::I64),
            ("name", DataType::Str),
            ("stamp", DataType::DateTime),
            ("amount", DataType::Str),
            ("doc", doc),
            ("ok", DataType::Bool),
        ];
        let mode = Mode::Incrementing(Position {
            columns: vec!["id".to_string()],
            settle_secs: None,
            start: Start::Beginning,
        });
        let query = format!("SELECT id, name, stamp, amount, doc, ok FROM {table}");
        let mut first = source(&server, config(&query, mode.clone(), &fields));

        let read = records(&mut first);
        let rows = objects(&fields, &read);
        assert_eq!(ids(&rows), [1, 2, 3], "{}: in the position's order", server.name);
        let two = &rows[1];
        assert_eq!(two["name"], "b");
        assert!(
            two["stamp"]
                .as_str()
                .is_some_and(|t| t.starts_with("2024-01-02T03:04:05.5")),
            "{}: a timestamp is a time, in UTC: {two:?}",
            server.name
        );
        assert_eq!(two["amount"], "12.50", "a decimal read into text keeps its digits");
        if server.mariadb() {
            let text = two["doc"].as_str().expect("JSON's text");
            let parsed: serde_json::Value = serde_json::from_str(text).expect("JSON");
            assert_eq!(parsed, serde_json::json!({"a": 1, "b": ["x"]}));
        } else {
            assert_eq!(two["doc"], serde_json::json!({"a": 1, "b": ["x"]}), "json is a variant");
        }
        assert_eq!(two["ok"], true);
        assert_eq!(rows[0]["ok"], false);
        assert!(
            !rows[2].contains_key("stamp") && !rows[2].contains_key("doc"),
            "a null is left out"
        );

        assert_eq!(first.take_progress(), None, "nothing committed, nothing reached");
        commit(&mut first, &read);
        let progress = first.take_progress().expect("progress");
        let cursor: serde_json::Value = serde_json::from_str(progress.cursor.as_deref().expect("a cursor")).unwrap();
        assert!(cursor.get("id").is_some(), "the cursor holds the position: {cursor}");

        owner.run(&format!("INSERT INTO {table} (id, name) VALUES (4, 'd'), (5, 'e')"));
        let mut resumed = source(&server, config(&query, mode, &fields));
        resumed.resume(progress).expect("resumed");
        let read = records(&mut resumed);
        assert_eq!(
            ids(&objects(&fields, &read)),
            [4, 5],
            "{}: only the rows past the cursor",
            server.name
        );
    }
}

#[test]
fn a_source_starting_now_skips_the_rows_already_there() {
    for server in servers("a_source_starting_now_skips_the_rows_already_there") {
        let mut owner = Owner::connect(&server);
        let table = unique("start_now");
        owner.table(&table, "id bigint PRIMARY KEY, name text");
        owner.run(&format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')"));
        let fields = [("id", DataType::I64), ("name", DataType::Str)];
        let query = format!("SELECT id, name FROM {table}");
        let at = |start: Start| {
            Mode::Incrementing(Position {
                columns: vec!["id".to_string()],
                settle_secs: None,
                start,
            })
        };

        let mut now = source(&server, config(&query, at(Start::Now), &fields));
        let read = records(&mut now);
        assert_eq!(
            ids(&objects(&fields, &read)),
            [3],
            "{}: from the largest position, which may come again",
            server.name
        );
        let begun = now
            .take_progress()
            .expect("where it began is kept before any row commits");
        assert_eq!(begun.cursor.as_deref(), Some(r#"{"__from":{"int":3}}"#));

        owner.run(&format!("INSERT INTO {table} VALUES (4, 'd')"));
        let mut moved = source(&server, config(&query, at(Start::Now), &fields));
        moved.resume(begun).expect("resumed");
        assert_eq!(
            ids(&objects(&fields, &records(&mut moved))),
            [3, 4],
            "where the first holder began"
        );

        let mut from = source(&server, config(&query, at(Start::From(Value::Int(2))), &fields));
        assert_eq!(ids(&objects(&fields, &records(&mut from))), [2, 3, 4]);

        // A row a late commit may still join does not move the start: it is read once it settles.
        let timed = unique("start_now_settled");
        owner.table(&timed, "id bigint PRIMARY KEY, at datetime(6) NOT NULL");
        owner.run(&format!(
            "INSERT INTO {timed} VALUES (1, NOW(6) - INTERVAL 1 HOUR), (2, NOW(6))"
        ));
        let fields = [("id", DataType::I64), ("at", DataType::DateTime)];
        let mode = Mode::Incrementing(Position {
            columns: vec!["at".to_string(), "id".to_string()],
            settle_secs: Some(30),
            start: Start::Now,
        });
        let mut now = source(&server, config(&format!("SELECT id, at FROM {timed}"), mode, &fields));
        assert_eq!(
            ids(&objects(&fields, &records(&mut now))),
            [1],
            "{}: begins at the newest settled row",
            server.name
        );
        let begun = now.take_progress().expect("where it began").cursor.unwrap();
        assert!(begun.starts_with(r#"{"__from":{"time":"#), "{begun}");
    }
}

#[test]
fn updated_at_sends_an_updated_row_again() {
    for server in servers("updated_at_sends_an_updated_row_again") {
        let mut owner = Owner::connect(&server);
        let table = unique("updated_at");
        owner.table(&table, "id bigint PRIMARY KEY, updated datetime(6) NOT NULL, name text");
        owner.run(&format!(
            "INSERT INTO {table} VALUES (1, NOW(6) - INTERVAL 1 MINUTE, 'a'), (2, NOW(6) - INTERVAL 1 MINUTE, 'b')"
        ));
        let fields = [("id", DataType::I64), ("name", DataType::Str)];
        let mode = Mode::UpdatedAt(Position {
            columns: vec!["updated".to_string(), "id".to_string()],
            settle_secs: Some(0),
            start: Start::Beginning,
        });
        let mut s = source(
            &server,
            config(&format!("SELECT id, updated, name FROM {table}"), mode, &fields),
        );

        let read = records(&mut s);
        assert_eq!(ids(&objects(&fields, &read)), [1, 2], "the same time, told apart by id");
        commit(&mut s, &read);
        assert!(s.take_progress().and_then(|p| p.cursor).is_some());

        owner.run(&format!(
            "UPDATE {table} SET name = 'changed', updated = NOW(6) WHERE id = 1"
        ));
        let read = records(&mut s);
        let rows = objects(&fields, &read);
        assert_eq!(ids(&rows), [1], "{}: only the updated row", server.name);
        assert_eq!(rows[0]["name"], "changed");
    }
}

#[test]
fn comparing_values_sends_what_changed_and_records_what_went() {
    for server in servers("comparing_values_sends_what_changed_and_records_what_went") {
        let mut owner = Owner::connect(&server);
        let table = unique("comparing");
        owner.table(&table, "id bigint PRIMARY KEY, name text");
        owner.run(&format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')"));
        let fields = [("id", DataType::I64), ("name", DataType::Str)];
        let mode = Mode::Comparing {
            key: vec!["id".to_string()],
        };
        let mut s = source(&server, config(&format!("SELECT id, name FROM {table}"), mode, &fields));

        let read = records(&mut s);
        let mut first = ids(&objects(&fields, &read));
        first.sort();
        assert_eq!(first, [1, 2, 3], "every row, the first poll");
        commit(&mut s, &read);
        assert_eq!(
            s.take_progress().expect("progress").added.len(),
            3,
            "a ledger entry a row"
        );

        owner.run(&format!("UPDATE {table} SET name = 'changed' WHERE id = 2"));
        let read = records(&mut s);
        let rows = objects(&fields, &read);
        assert_eq!(ids(&rows), [2], "{}: only the changed row", server.name);
        assert_eq!(rows[0]["name"], "changed");
        commit(&mut s, &read);
        let progress = s.take_progress().expect("progress");
        assert_eq!(progress.added.len(), 1);
        assert!(progress.removed.is_empty());

        owner.run(&format!("DELETE FROM {table} WHERE id = 3"));
        let deadline = Instant::now() + Duration::from_secs(30);
        let removed = loop {
            let got = s
                .poll_batch(10_000, usize::MAX, Duration::from_millis(50))
                .expect("a poll");
            assert!(got.is_empty(), "a deletion sends no record");
            if let Some(Progress { removed, .. }) = s.take_progress() {
                break removed;
            }
            assert!(Instant::now() < deadline, "the deletion never showed in progress");
        };
        assert_eq!(removed.len(), 1, "{removed:?}");
        assert!(removed[0].contains('3'), "the gone row's key: {removed:?}");
    }
}

#[test]
fn every_poll_sends_the_whole_result_each_poll() {
    for server in servers("every_poll_sends_the_whole_result_each_poll") {
        let mut owner = Owner::connect(&server);
        let table = unique("every_poll");
        owner.table(&table, "id bigint PRIMARY KEY, name text");
        owner.run(&format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b')"));
        let fields = [("id", DataType::I64), ("name", DataType::Str)];
        let mut s = source(
            &server,
            config(&format!("SELECT id, name FROM {table}"), Mode::EveryPoll, &fields),
        );

        for poll in 0..2 {
            let read = records(&mut s);
            let mut got = ids(&objects(&fields, &read));
            got.sort();
            assert_eq!(got, [1, 2], "{}: poll {poll}", server.name);
            commit(&mut s, &read);
        }
    }
}

#[test]
fn a_write_inside_the_query_is_refused_and_writes_nothing() {
    for server in servers("a_write_inside_the_query_is_refused_and_writes_nothing") {
        let mut owner = Owner::connect(&server);
        let table = unique("read_only");
        owner.table(&table, "id bigint PRIMARY KEY");
        owner.run(&format!("INSERT INTO {table} VALUES (1), (2)"));
        // A SELECT writes through a function that does.
        owner.run(&format!(
            "CREATE FUNCTION {table}_wipe() RETURNS int MODIFIES SQL DATA BEGIN DELETE FROM {table}; RETURN 1; END"
        ));
        let query = format!("SELECT {table}_wipe() AS id");

        // Run as written, by a driver signed in as the owner, who may delete: only the read-only transaction stops it.
        let mut as_owner = driver(&server, OWNER);
        let err = as_owner.run(&bare(&query), &[]).expect_err("a write is refused");
        assert!(
            err.contains("READ ONLY transaction"),
            "{}'s read-only error: {err}",
            server.name
        );
        assert_eq!(
            owner.one(&format!("SELECT count(*) FROM {table}")),
            "2",
            "nothing deleted"
        );

        // Through the source, whose statement holds the query inside its own.
        let fields = [("id", DataType::I64)];
        let mut s = SqlSource::new(driver(&server, OWNER), config(&query, Mode::EveryPoll, &fields));
        let e = failure(&mut s, "a write");
        assert!(e.contains("READ ONLY transaction"), "{e}");
        assert_eq!(
            owner.one(&format!("SELECT count(*) FROM {table}")),
            "2",
            "nothing deleted"
        );

        // One statement only: a second one is refused before anything runs.
        let err = as_owner
            .run(&bare(&format!("SELECT 1; DELETE FROM {table}")), &[])
            .expect_err("two statements are refused");
        assert!(err.contains("SQL syntax"), "{err}");
        assert_eq!(
            owner.one(&format!("SELECT count(*) FROM {table}")),
            "2",
            "nothing deleted"
        );
    }
}

#[test]
fn a_broken_connection_is_opened_again_in_utc_with_a_statement_timeout() {
    for server in servers("a_broken_connection_is_opened_again_in_utc_with_a_statement_timeout") {
        // A user may end its own connections.
        let mut reader = Owner::sign_in(&server, READER);
        let mut d = driver(&server, READER);
        let timeout = if server.mariadb() {
            "@@max_statement_time"
        } else {
            "@@max_execution_time"
        };
        let session = bare(&format!(
            "SELECT CONNECTION_ID() AS id, @@time_zone AS zone, CAST({timeout} AS CHAR) AS timeout"
        ));
        let first = d.run(&session, &[]).expect("a session").rows.remove(0);
        assert_eq!(first[1], Value::Str("+00:00".into()), "{}", server.name);
        let expected = if server.mariadb() { "290.000000" } else { "290000" };
        assert_eq!(
            first[2],
            Value::Str(expected.into()),
            "{}: shorter than a call",
            server.name
        );

        let Value::Int(id) = first[0] else {
            panic!("a connection id: {first:?}");
        };
        reader.run(&format!("KILL {id}"));
        let again = d.run(&session, &[]).expect("a new connection, at once").rows.remove(0);
        assert_ne!(again[0], first[0], "{}: another connection", server.name);
        assert_eq!(again[1], Value::Str("+00:00".into()));
    }
}

#[test]
fn describe_names_each_column_its_type_table_and_uniqueness() {
    for server in servers("describe_names_each_column_its_type_table_and_uniqueness") {
        let mut owner = Owner::connect(&server);
        let table = unique("describe");
        owner.table(
            &table,
            "id bigint unsigned PRIMARY KEY, name text, stamp datetime(6), code varchar(10) UNIQUE, \
             pair_a int, pair_b int, UNIQUE (pair_a, pair_b)",
        );

        let mut d = driver(&server, READER);
        let described = d
            .describe(&plain_statement(&format!(
                "SELECT id AS ident, name, stamp, code, pair_a, 'two' AS two FROM {table}"
            )))
            .expect("described");
        let names: Vec<&str> = described.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["ident", "name", "stamp", "code", "pair_a", "two"]);
        let found: Vec<_> = described
            .iter()
            .map(|c| (c.database_type.as_str(), c.suggested.clone(), c.unique))
            .collect();
        assert_eq!(
            found,
            [
                ("bigint unsigned", Some(DataType::I64), true),
                ("text", Some(DataType::Str), false),
                ("datetime", Some(DataType::DateTime), false),
                ("varchar", Some(DataType::Str), true),
                ("int", Some(DataType::I64), false),
                ("varchar", Some(DataType::Str), false),
            ],
            "{}",
            server.name
        );
        for c in &described[..5] {
            assert_eq!(
                c.table.as_deref(),
                Some(format!("{DATABASE}.{table}").as_str()),
                "{}: {c:?} is read from {table}",
                server.name
            );
        }
        assert_eq!(described[5].table, None, "an expression is read from no table");
    }
}

#[test]
fn plan_tells_a_full_scan_from_an_indexed_read() {
    for server in servers("plan_tells_a_full_scan_from_an_indexed_read") {
        let mut d = driver(&server, READER);

        let scan = d
            .plan(&plain_statement("SELECT id FROM plan_big WHERE plain = 5"), &[])
            .expect("planned");
        match scan {
            Verdict::FullScan { relation, rows } => {
                assert_eq!(relation, "plan_big");
                assert!(
                    rows >= 500_000,
                    "{}: the estimate is the table's size: {rows}",
                    server.name
                );
            }
            other => panic!(
                "{}: a filter on a column with no index reads the whole table: {other:?}",
                server.name
            ),
        }
        let statement = plain_statement("SELECT id FROM plan_big WHERE indexed = :last.indexed");
        let indexed = d.plan(&statement, &[Value::Int(5)]).expect("planned");
        match indexed {
            Verdict::Indexed { indexes, rows } => {
                assert_eq!(
                    indexes,
                    ["plan_big_indexed"],
                    "{}: the index the filter reads through",
                    server.name
                );
                assert!(
                    (1..=1000).contains(&rows),
                    "the estimate of what the poll returns, at most its limit: {rows}"
                );
            }
            other => panic!(
                "{}: a filter on an indexed column reads through its index: {other:?}",
                server.name
            ),
        }
    }
}

#[test]
fn json_bytes_and_odd_types_arrive_as_their_declared_types() {
    for server in servers("json_bytes_and_odd_types_arrive_as_their_declared_types") {
        let mut owner = Owner::connect(&server);
        let table = unique("odd");
        owner.table(
            &table,
            "id bigint PRIMARY KEY, doc json, data varbinary(8), body blob, flags bit(8), born year, span time(6), \
             mood enum('glad', 'sad'), tags set('a', 'b'), huge bigint unsigned, ratio float, exact double, \
             day date, at datetime NULL, spot point",
        );
        // A zero date is MySQL's own; it is stored only where the session allows it.
        owner.run("SET SESSION sql_mode = ''");
        owner.run(&format!(
            "INSERT INTO {table} VALUES (1, '{{\"name\": \"ann\", \"at\": \"2024-01-01T00:00:01Z\"}}', 0xdead00, \
             'words', b'00000101', 2024, '-12:00:00.5', 'sad', 'a,b', 18446744073709551615, 0.5, 0.25, \
             '2024-02-29', '0000-00-00 00:00:00', ST_GeomFromText('POINT(1 2)'))"
        ));
        let doc = if server.mariadb() {
            DataType::Str
        } else {
            DataType::Struct(
                vec![("at".into(), DataType::DateTime), ("name".into(), DataType::Str)],
                false,
            )
        };
        let fields = [
            ("id", DataType::I64),
            ("doc", doc.clone()),
            ("data", DataType::Bytes),
            ("body", DataType::Bytes),
            ("flags", DataType::I64),
            ("born", DataType::I64),
            ("span", DataType::Duration),
            ("mood", DataType::Str),
            ("tags", DataType::Str),
            ("huge", DataType::Str),
            ("ratio", DataType::F64),
            ("exact", DataType::F64),
            ("day", DataType::DateTime),
            ("at", DataType::DateTime),
            ("spot", DataType::Str),
        ];
        let query = format!("SELECT * FROM {table}");
        let mut s = source(&server, config(&query, Mode::EveryPoll, &fields));
        let read = records(&mut s);
        let chunk = &read[0].chunk;
        assert!(chunk.failed().is_empty(), "{}: {:?}", server.name, chunk.failed());

        let doc_column = chunk.column(1);
        if server.mariadb() {
            assert!(doc_column.var().unwrap().get(0).starts_with(b"{"), "JSON's text");
        } else {
            assert_eq!(doc_column.data_type(), doc);
            assert_eq!(
                doc_column.field("at").unwrap().datetime().unwrap().values[0],
                1_704_067_201_000_000,
                "a time inside JSON is parsed as one"
            );
        }
        assert_eq!(chunk.column(2).var().unwrap().get(0), [0xde, 0xad, 0x00]);
        assert_eq!(chunk.column(3).var().unwrap().get(0), b"words");
        assert_eq!(chunk.column(4).i64().unwrap().values[0], 5);
        assert_eq!(chunk.column(5).i64().unwrap().values[0], 2024);
        assert_eq!(
            chunk.column(6).duration().unwrap().values[0],
            -12 * 3600 * 1_000_000 - 500_000
        );
        assert_eq!(chunk.column(7).var().unwrap().get(0), b"sad");
        assert_eq!(chunk.column(8).var().unwrap().get(0), b"a,b");
        assert_eq!(
            chunk.column(9).var().unwrap().get(0),
            b"18446744073709551615",
            "past i64, kept whole as text"
        );
        assert_eq!(chunk.column(10).f64().unwrap().values[0], 0.5);
        assert_eq!(chunk.column(11).f64().unwrap().values[0], 0.25);
        // 2024-02-29 is 19_782 days after 1970-01-01.
        assert_eq!(chunk.column(12).datetime().unwrap().values[0], 19_782 * 86_400_000_000);
        assert!(
            !chunk.column(13).is_valid(0),
            "{}: a zero date names no time",
            server.name
        );
        assert_eq!(
            chunk.column(14).var().unwrap().get(0),
            b"POINT(1 2)",
            "a point is its text"
        );

        // A position read only as text cannot be bound back as its own type.
        let mode = Mode::Incrementing(Position {
            columns: vec!["spot".to_string()],
            settle_secs: None,
            start: Start::Beginning,
        });
        let mut s = source(&server, config(&query, mode, &fields));
        let e = failure(&mut s, "a position read only as text");
        assert!(e.contains("`spot` is of a type smeltr reads only as text"), "{e}");
    }
}

#[test]
fn require_tls_refuses_a_server_whose_certificate_it_cannot_verify() {
    for server in servers("require_tls_refuses_a_server_whose_certificate_it_cannot_verify") {
        // Each image serves a certificate it signed itself at its first start, which no root here trusts.
        let mut d = driver_with(&server, READER, Encrypted::Require).expect("a driver");
        let err = d.run(&bare("SELECT 1 AS one"), &[]).expect_err("an unverified server");
        assert!(err.contains("invalid peer certificate"), "{}: {err}", server.name);
        let mut clear = driver(&server, READER);
        assert!(
            clear.run(&bare("SELECT 1 AS one"), &[]).is_ok(),
            "{}: in the clear",
            server.name
        );
    }
}

/// A `mysql` source as a configuration names it: loaded, opened as a deployment opens it, read, committed, and
/// resumed past its progress.
#[test]
fn a_mysql_source_from_a_bundle_reads_its_record_and_resumes() {
    for server in servers("a_mysql_source_from_a_bundle_reads_its_record_and_resumes") {
        let mut owner = Owner::connect(&server);
        let table = unique("bundle");
        owner.table(&table, "id BIGINT PRIMARY KEY, at DATETIME(6) NOT NULL, who TEXT");
        owner.run(&format!(
            "INSERT INTO {table} VALUES (1, UTC_TIMESTAMP(6) - INTERVAL 1 HOUR, 'ana'), \
             (2, UTC_TIMESTAMP(6) - INTERVAL 1 HOUR, 'li')"
        ));
        let json = serde_json::json!({
            "version": 1,
            "sources": [{
                "id": "my", "type": "mysql", "host": server.host, "port": server.port, "database": DATABASE,
                "user": READER, "password": {"secret": "mysql-password", "version": 1}, "tls": "disable",
                "query": format!("SELECT id, at, who FROM {table}"),
                "mode": {"type": "incrementing", "position": ["at", "id"], "settleSecs": 1, "start": "beginning"},
                "fields": [{"name": "id", "type": "i64"}, {"name": "at", "type": "datetime"}, {"name": "who", "type": "str"}],
                "pollIntervalSecs": 1
            }]
        })
        .to_string();
        let system = smeltr::config_file::load_str(&json).expect("loads");
        let kind = system.topology.sources[0].kind.clone();
        assert!(kind.single_worker().is_some(), "{}: one worker reads it", server.name);
        let mut secrets = HeldSecrets::default();
        secrets.set_stored([("mysql-password".to_string(), 1, PASSWORD.to_string())]);
        let open = || {
            smeltr::io::build_source(
                &kind,
                &Arc::new(smeltr::metrics::Metrics::new()),
                &Arc::new(smeltr::metrics::Subjects::default()),
                &secrets,
                None,
            )
            .expect("opens")
        };
        let read = |source: &mut Box<dyn Source>| {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let got = source
                    .poll_batch(10_000, usize::MAX, Duration::from_millis(50))
                    .expect("a poll");
                if let Some(e) = source.connection_error() {
                    panic!("{}: the poll failed: {e}", server.name);
                }
                if !got.is_empty() {
                    return got;
                }
                assert!(Instant::now() < deadline, "{}: no records within 30s", server.name);
            }
        };
        let meta = kind.meta();
        let mut first = open();
        let got = read(&mut first);
        let rows: Vec<RecordObject> = got.iter().map(|r| meta.record_object(r, "")).collect();
        assert_eq!(rows.iter().map(|o| o["who"].clone()).collect::<Vec<_>>(), ["ana", "li"]);
        assert_eq!(
            &*got[0].topic,
            format!("{}:{}/{DATABASE}", server.host, server.port),
            "__source.database"
        );

        let mut offsets = CommitMap::new();
        for r in &got {
            let at = offsets
                .entry(TopicPartition::new(r.topic.clone(), r.partition))
                .or_insert(r.offset);
            *at = (*at).max(r.offset);
        }
        first.commit(&offsets).expect("committed");
        let progress: Progress = first.take_progress().expect("progress");
        drop(first);

        owner.run(&format!(
            "INSERT INTO {table} VALUES (3, UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE, 'mo')"
        ));
        let mut second = open();
        second.resume(progress).expect("resumed");
        let got = read(&mut second);
        let rows: Vec<RecordObject> = got.iter().map(|r| meta.record_object(r, "")).collect();
        assert_eq!(
            rows.iter().map(|o| o["who"].clone()).collect::<Vec<_>>(),
            ["mo"],
            "{}: only past the progress",
            server.name
        );
    }
}

/// Run on a `mysql` source: the worker describes the query, reads its first rows before any field is declared, and
/// judges the plan of a position mode.
#[test]
fn run_describes_the_query_and_reads_before_any_field_is_declared() {
    for server in servers("run_describes_the_query_and_reads_before_any_field_is_declared") {
        let mut owner = Owner::connect(&server);
        let table = unique("run");
        owner.table(&table, "id BIGINT PRIMARY KEY, name TEXT");
        owner.run(&format!(
            "INSERT INTO {table} WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 30) \
             SELECT i, CONCAT('n', i) FROM n"
        ));
        let mut secrets = HeldSecrets::default();
        secrets.set_stored([("mysql-password".to_string(), 1, PASSWORD.to_string())]);
        let run = |mode: serde_json::Value, fields: serde_json::Value| {
            let source: smeltr::config_file::SourceDto = serde_json::from_value(serde_json::json!({
                "id": "my", "type": "mysql", "host": server.host, "port": server.port, "database": DATABASE,
                "user": READER, "password": {"secret": "mysql-password", "version": 1}, "tls": "disable",
                "query": format!("SELECT id, name FROM {table}"), "mode": mode, "fields": fields
            }))
            .unwrap();
            let captures = smeltr::worker::capture::Captures::default();
            let admitted = captures.admit(&[smeltr::heartbeat::CaptureInvitation {
                session: "s1".to_string(),
                group: "g".to_string(),
                source: Some(source),
                destination: None,
                secrets: Vec::new(),
                filter: None,
                start: None,
                max_matches: 20,
                max_bytes: 1 << 20,
                deadline_secs: 30,
            }]);
            smeltr::worker::capture::run_cold_capture(
                &captures,
                &admitted[0],
                &Arc::new(smeltr::metrics::Metrics::new()),
                &secrets,
            );
            captures.finished("w1").pop().expect("a report")
        };

        let report = run(serde_json::json!({"type": "everyPoll"}), serde_json::json!([]));
        assert_eq!(
            report.finished,
            Some(smeltr::heartbeat::CaptureEnd::Matches),
            "{}: {report:?}",
            server.name
        );
        let sql = report.sql.expect("what Run found");
        let columns: Vec<_> = sql
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.suggested.as_str(), c.unique))
            .collect();
        assert_eq!(
            columns,
            [("id", "i64", true), ("name", "str", false)],
            "{}",
            server.name
        );
        let rows: usize = report.batches.iter().map(|b| b.rows.len()).sum();
        assert_eq!(rows, 20, "{}: a capture's worth", server.name);

        let report = run(
            serde_json::json!({"type": "incrementing", "position": ["id"]}),
            serde_json::json!([{"name": "id", "type": "i64"}, {"name": "name", "type": "str"}]),
        );
        let verdict = report.sql.expect("what Run found").verdict;
        assert!(
            matches!(verdict, Some(smeltr::heartbeat::SqlVerdict::Indexed { .. })),
            "{}: the primary key serves the position: {verdict:?}",
            server.name
        );
        assert_eq!(
            report.batches[0].rows[0].values[0],
            serde_json::json!(1),
            "{}",
            server.name
        );
    }
}
