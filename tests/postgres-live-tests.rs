//! The SQL source against a real PostgreSQL, as `just live-up` runs it: each mode's polls, the progress a commit
//! moves and a resume from it, the read-only transaction every statement runs in, and what the database describes
//! and plans.
//!
//! The source signs in as `smeltr_reader`, which may only SELECT. The tests write as `smeltr`, the database's owner,
//! each into tables of its own, so a rerun against services left up starts clean.
//!
//! Point it at `POSTGRES_TEST_HOST`. Under `SMELTR_LIVE_TESTS=required` a missing one fails rather than skips.

use crate::live;

use std::sync::Arc;
use std::time::{Duration, Instant};

use smeltr::core::column::DataType;
use smeltr::io::postgres::{PostgresConnection, PostgresDriver};
use smeltr::io::sql::{Dialect, Poll, Query, Statement};
use smeltr::io::sql_source::Encrypted;
use smeltr::io::sql_source::{Driver, Mode, Position, SqlSource, SqlSourceConfig, Start, Verdict};
use smeltr::io::values::Value;
use smeltr::io::{CommitMap, Progress, RecordObject, Source, SourceRecord, TopicPartition};
use smeltr::plan::topology::{HeldSecrets, SecretRef, SourceMeta};

const OWNER: &str = "smeltr";
const READER: &str = "smeltr_reader";
const PASSWORD: &str = "smeltr-live-1";
const DATABASE: &str = "smeltr";
const PLACE: &str = "postgres-live";

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
fn driver(host: &str, port: u16, user: &str) -> PostgresDriver {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("postgres-password".to_string(), 1, PASSWORD.to_string())]);
    let connection = PostgresConnection {
        host: host.to_string(),
        port,
        database: DATABASE.to_string(),
        user: user.to_string(),
        password: Some(SecretRef::LeaderStored {
            name: "postgres-password".to_string(),
            version: 1,
        }),
        encrypted: Encrypted::Disable,
        tls: None,
    };
    PostgresDriver::new(connection, &secrets).expect("a driver")
}

/// The database's owner, for writing the rows a source reads.
struct Owner {
    runtime: tokio::runtime::Runtime,
    client: tokio_postgres::Client,
}

impl Owner {
    fn connect(host: &str, port: u16) -> Owner {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let config = format!("host={host} port={port} dbname={DATABASE} user={OWNER} password={PASSWORD}");
        let client = runtime.block_on(async {
            let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
                .await
                .expect("the owner signs in");
            tokio::spawn(connection);
            client
        });
        Owner { runtime, client }
    }

    fn run(&self, sql: &str) {
        self.runtime
            .block_on(self.client.batch_execute(sql))
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// The first column of the first row `sql` returns, as text.
    fn one(&self, sql: &str) -> String {
        let messages = self
            .runtime
            .block_on(self.client.simple_query(sql))
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        messages
            .iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).expect("a value").to_string()),
                _ => None,
            })
            .expect("a row")
    }

    /// Makes `table` with `columns` and lets the reader read it.
    fn table(&self, table: &str, columns: &str) {
        self.run(&format!(
            "CREATE TABLE {table} ({columns}); GRANT SELECT ON {table} TO {READER};"
        ));
    }
}

fn config(query: &str, mode: Mode, fields: &[(&str, DataType)]) -> SqlSourceConfig {
    SqlSourceConfig {
        query: Query::parse(query, Dialect::Postgres).expect("a query"),
        mode,
        fields: fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect(),
        poll_interval: Duration::from_millis(100),
        batch: 100,
        place: Arc::from(PLACE),
    }
}

fn source(host: &str, port: u16, config: SqlSourceConfig) -> SqlSource<PostgresDriver> {
    SqlSource::new(driver(host, port, READER), config)
}

/// Polls until records come, failing on a poll that errs or on none within the deadline.
fn records(source: &mut SqlSource<PostgresDriver>) -> Vec<SourceRecord> {
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

/// Commits every record of `records`.
fn commit(source: &mut SqlSource<PostgresDriver>, records: &[SourceRecord]) {
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

#[test]
fn incrementing_reads_typed_rows_in_order_and_resumes_past_the_last_committed() {
    let Some((host, port)) =
        live::postgres("incrementing_reads_typed_rows_in_order_and_resumes_past_the_last_committed")
    else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("incrementing");
    owner.table(
        &table,
        "id bigint PRIMARY KEY, name text, stamp timestamptz, amount numeric, doc jsonb, ok boolean",
    );
    owner.run(&format!(
        "INSERT INTO {table} VALUES
           (2, 'b', '2024-01-02T03:04:05Z', 12.5, '{{\"a\": 1, \"b\": [\"x\"]}}', true),
           (1, 'a', '2024-01-01T00:00:00Z', 1, '{{}}', false),
           (3, 'c', NULL, NULL, NULL, NULL);"
    ));
    let fields = [
        ("id", DataType::I64),
        ("name", DataType::Str),
        ("stamp", DataType::DateTime),
        ("amount", DataType::Str),
        ("doc", DataType::Variant),
        ("ok", DataType::Bool),
    ];
    let mode = Mode::Incrementing(Position {
        columns: vec!["id".to_string()],
        settle_secs: None,
        start: Start::Beginning,
    });
    let query = format!("SELECT id, name, stamp, amount, doc, ok FROM {table}");
    let mut first = source(&host, port, config(&query, mode.clone(), &fields));

    let read = records(&mut first);
    let rows = objects(&fields, &read);
    assert_eq!(ids(&rows), [1, 2, 3], "in the position's order");
    let two = &rows[1];
    assert_eq!(two["name"], "b");
    assert!(
        two["stamp"]
            .as_str()
            .is_some_and(|t| t.starts_with("2024-01-02T03:04:05")),
        "a timestamptz is a time: {two:?}"
    );
    assert_eq!(two["amount"], "12.5", "a numeric read into text keeps its digits");
    assert_eq!(
        two["doc"],
        serde_json::json!({"a": 1, "b": ["x"]}),
        "jsonb is a variant"
    );
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

    owner.run(&format!("INSERT INTO {table} (id, name) VALUES (4, 'd'), (5, 'e');"));
    let mut resumed = source(&host, port, config(&query, mode, &fields));
    resumed.resume(progress).expect("resumed");
    let read = records(&mut resumed);
    assert_eq!(ids(&objects(&fields, &read)), [4, 5], "only the rows past the cursor");
}

#[test]
fn a_source_starting_now_skips_the_rows_already_there() {
    let Some((host, port)) = live::postgres("a_source_starting_now_skips_the_rows_already_there") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("start_now");
    owner.table(&table, "id bigint PRIMARY KEY, name text");
    owner.run(&format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c');"));
    let fields = [("id", DataType::I64), ("name", DataType::Str)];
    let query = format!("SELECT id, name FROM {table}");
    let at = |start: Start| {
        Mode::Incrementing(Position {
            columns: vec!["id".to_string()],
            settle_secs: None,
            start,
        })
    };

    let mut now = source(&host, port, config(&query, at(Start::Now), &fields));
    let read = records(&mut now);
    assert_eq!(
        ids(&objects(&fields, &read)),
        [3],
        "from the largest position, which may come again"
    );
    let begun = now
        .take_progress()
        .expect("where it began is kept before any row commits");
    assert_eq!(begun.cursor.as_deref(), Some(r#"{"__from":{"int":3}}"#));

    owner.run(&format!("INSERT INTO {table} VALUES (4, 'd');"));
    let mut moved = source(&host, port, config(&query, at(Start::Now), &fields));
    moved.resume(begun).expect("resumed");
    assert_eq!(
        ids(&objects(&fields, &records(&mut moved))),
        [3, 4],
        "where the first holder began"
    );

    let mut from = source(&host, port, config(&query, at(Start::From(Value::Int(2))), &fields));
    assert_eq!(ids(&objects(&fields, &records(&mut from))), [2, 3, 4]);

    // A row a late commit may still join does not move the start: it is read once it settles.
    let timed = unique("start_now_settled");
    owner.table(&timed, "id bigint PRIMARY KEY, at timestamptz NOT NULL");
    owner.run(&format!(
        "INSERT INTO {timed} VALUES (1, now() - interval '1 hour'), (2, now());"
    ));
    let fields = [("id", DataType::I64), ("at", DataType::DateTime)];
    let mode = Mode::Incrementing(Position {
        columns: vec!["at".to_string(), "id".to_string()],
        settle_secs: Some(30),
        start: Start::Now,
    });
    let mut now = source(
        &host,
        port,
        config(&format!("SELECT id, at FROM {timed}"), mode, &fields),
    );
    assert_eq!(
        ids(&objects(&fields, &records(&mut now))),
        [1],
        "begins at the newest settled row"
    );
    let begun = now.take_progress().expect("where it began").cursor.unwrap();
    assert!(begun.starts_with(r#"{"__from":{"time":"#), "{begun}");
}

#[test]
fn updated_at_sends_an_updated_row_again() {
    let Some((host, port)) = live::postgres("updated_at_sends_an_updated_row_again") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("updated_at");
    owner.table(&table, "id bigint PRIMARY KEY, updated timestamptz NOT NULL, name text");
    owner.run(&format!(
        "INSERT INTO {table} VALUES (1, now() - interval '1 minute', 'a'), (2, now() - interval '1 minute', 'b');"
    ));
    let fields = [("id", DataType::I64), ("name", DataType::Str)];
    let mode = Mode::UpdatedAt(Position {
        columns: vec!["updated".to_string(), "id".to_string()],
        settle_secs: Some(0),
        start: Start::Beginning,
    });
    let mut s = source(
        &host,
        port,
        config(&format!("SELECT id, updated, name FROM {table}"), mode, &fields),
    );

    let read = records(&mut s);
    assert_eq!(ids(&objects(&fields, &read)), [1, 2], "the same time, told apart by id");
    commit(&mut s, &read);
    assert!(s.take_progress().and_then(|p| p.cursor).is_some());

    owner.run(&format!(
        "UPDATE {table} SET name = 'changed', updated = now() WHERE id = 1;"
    ));
    let read = records(&mut s);
    let rows = objects(&fields, &read);
    assert_eq!(ids(&rows), [1], "only the updated row");
    assert_eq!(rows[0]["name"], "changed");
}

#[test]
fn comparing_values_sends_what_changed_and_records_what_went() {
    let Some((host, port)) = live::postgres("comparing_values_sends_what_changed_and_records_what_went") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("comparing");
    owner.table(&table, "id bigint PRIMARY KEY, name text");
    owner.run(&format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c');"));
    let fields = [("id", DataType::I64), ("name", DataType::Str)];
    let mode = Mode::Comparing {
        key: vec!["id".to_string()],
    };
    let mut s = source(
        &host,
        port,
        config(&format!("SELECT id, name FROM {table}"), mode, &fields),
    );

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

    owner.run(&format!("UPDATE {table} SET name = 'changed' WHERE id = 2;"));
    let read = records(&mut s);
    let rows = objects(&fields, &read);
    assert_eq!(ids(&rows), [2], "only the changed row");
    assert_eq!(rows[0]["name"], "changed");
    commit(&mut s, &read);
    let progress = s.take_progress().expect("progress");
    assert_eq!(progress.added.len(), 1);
    assert!(progress.removed.is_empty());

    owner.run(&format!("DELETE FROM {table} WHERE id = 3;"));
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

#[test]
fn every_poll_sends_the_whole_result_each_poll() {
    let Some((host, port)) = live::postgres("every_poll_sends_the_whole_result_each_poll") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("every_poll");
    owner.table(&table, "id bigint PRIMARY KEY, name text");
    owner.run(&format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b');"));
    let fields = [("id", DataType::I64), ("name", DataType::Str)];
    let mut s = source(
        &host,
        port,
        config(&format!("SELECT id, name FROM {table}"), Mode::EveryPoll, &fields),
    );

    for poll in 0..2 {
        let read = records(&mut s);
        let mut got = ids(&objects(&fields, &read));
        got.sort();
        assert_eq!(got, [1, 2], "poll {poll}");
        commit(&mut s, &read);
    }
}

#[test]
fn a_write_inside_the_query_is_refused_and_writes_nothing() {
    let Some((host, port)) = live::postgres("a_write_inside_the_query_is_refused_and_writes_nothing") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("read_only");
    owner.table(&table, "id bigint PRIMARY KEY");
    owner.run(&format!("INSERT INTO {table} VALUES (1), (2);"));
    let query = format!("WITH x AS (DELETE FROM {table} RETURNING *) SELECT * FROM x");

    // Run as written, by a driver signed in as the owner, who may delete: only the read-only transaction stops it.
    let mut as_owner = driver(&host, port, OWNER);
    let statement = Statement {
        sql: query.clone(),
        binds: Vec::new(),
    };
    let err = as_owner.run(&statement, &[]).expect_err("a write is refused");
    assert!(
        err.contains("read-only transaction"),
        "PostgreSQL's read-only error: {err}"
    );
    assert_eq!(
        owner.one(&format!("SELECT count(*) FROM {table}")),
        "2",
        "nothing deleted"
    );

    // Through the source, whose statement holds the query inside its own.
    let fields = [("id", DataType::I64)];
    let mut s = source(&host, port, config(&query, Mode::EveryPoll, &fields));
    let deadline = Instant::now() + Duration::from_secs(30);
    while s.connection_error().is_none() {
        let got = s
            .poll_batch(10_000, usize::MAX, Duration::from_millis(50))
            .expect("a poll");
        assert!(got.is_empty(), "a write sends no record");
        assert!(Instant::now() < deadline, "the poll never failed");
    }
    assert_eq!(
        owner.one(&format!("SELECT count(*) FROM {table}")),
        "2",
        "nothing deleted"
    );
}

/// The statement a source with no position runs for `query`.
fn plain_statement(query: &str) -> Statement {
    smeltr::io::sql::statement(
        &Query::parse(query, Dialect::Postgres).expect("a query"),
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

#[test]
fn describe_names_each_column_its_type_table_and_uniqueness() {
    let Some((host, port)) = live::postgres("describe_names_each_column_its_type_table_and_uniqueness") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("describe");
    owner.table(&table, "id bigint PRIMARY KEY, name text, stamp timestamptz");

    let mut d = driver(&host, port, READER);
    let described = d
        .describe(&plain_statement(&format!("SELECT id, name, stamp FROM {table}")))
        .expect("described");
    let names: Vec<&str> = described.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "stamp"]);
    let (id, name, at) = (&described[0], &described[1], &described[2]);
    assert_eq!(id.suggested, Some(DataType::I64), "{id:?}");
    assert_eq!(name.suggested, Some(DataType::Str), "{name:?}");
    assert_eq!(at.suggested, Some(DataType::DateTime), "{at:?}");
    assert!(described.iter().all(|c| !c.database_type.is_empty()), "{described:?}");
    for c in &described {
        assert!(
            c.table.as_deref().is_some_and(|t| t.ends_with(&table)),
            "{c:?} is read from {table}"
        );
    }
    assert!(id.unique, "the primary key is unique");
    assert!(!name.unique, "a column with no unique index is not");
}

#[test]
fn plan_tells_a_full_scan_from_an_indexed_read() {
    let Some((host, port)) = live::postgres("plan_tells_a_full_scan_from_an_indexed_read") else {
        return;
    };
    let mut d = driver(&host, port, READER);

    let scan = d
        .plan(&plain_statement("SELECT id FROM plan_big WHERE plain = 5"), &[])
        .expect("planned");
    match scan {
        Verdict::FullScan { relation, rows } => {
            assert!(relation.ends_with("plan_big"), "{relation}");
            assert!(rows >= 500_000, "the estimate is the table's size: {rows}");
        }
        other => panic!("a filter on a column with no index reads the whole table: {other:?}"),
    }
    let indexed = d
        .plan(&plain_statement("SELECT id FROM plan_big WHERE indexed = 5"), &[])
        .expect("planned");
    match indexed {
        Verdict::Indexed { indexes, rows } => {
            assert_eq!(indexes, ["plan_big_indexed"], "the index the filter reads through");
            assert!(rows >= 1, "the estimate of what the poll returns: {rows}");
        }
        other => panic!("a filter on an indexed column reads through its index: {other:?}"),
    }
}

#[test]
fn an_array_and_an_object_arrive_as_their_declared_types() {
    let Some((host, port)) = live::postgres("an_array_and_an_object_arrive_as_their_declared_types") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("shapes");
    owner.table(
        &table,
        "id bigint PRIMARY KEY, seen timestamptz[], counts int4[], doc jsonb",
    );
    owner.run(&format!(
        "INSERT INTO {table} VALUES
           (1, ARRAY['2024-01-01T00:00:00Z', NULL]::timestamptz[], ARRAY[3, 4], '{{\"name\": \"ann\", \"at\": \"2024-01-01T00:00:01Z\", \"extra\": 1}}');"
    ));
    let doc = DataType::Struct(
        vec![("at".into(), DataType::DateTime), ("name".into(), DataType::Str)],
        false,
    );
    let fields = [
        ("id", DataType::I64),
        ("seen", DataType::Array(Box::new(DataType::DateTime))),
        ("counts", DataType::Array(Box::new(DataType::I64))),
        ("doc", doc.clone()),
    ];
    let query = format!("SELECT id, seen, counts, doc FROM {table}");
    let mut s = source(&host, port, config(&query, Mode::EveryPoll, &fields));
    let read = records(&mut s);
    let chunk = &read[0].chunk;
    assert!(chunk.failed().is_empty(), "{:?}", chunk.failed());

    let seen = chunk.column(1);
    assert_eq!(seen.data_type(), DataType::Array(Box::new(DataType::DateTime)));
    let list = seen.list().unwrap();
    assert_eq!(list.offsets, [0, 2]);
    assert_eq!(list.child.datetime().unwrap().values[0], 1_704_067_200_000_000);
    assert!(!list.child.is_valid(1), "a null element stays null");

    let counts = chunk.column(2).list().unwrap();
    assert_eq!(counts.child.i64().unwrap().values, [3, 4]);

    let doc_column = chunk.column(3);
    assert_eq!(doc_column.data_type(), doc);
    assert_eq!(
        doc_column.field("at").unwrap().datetime().unwrap().values[0],
        1_704_067_201_000_000,
        "a time inside JSON is parsed as one"
    );
}

#[test]
fn a_column_smeltr_cannot_read_arrives_as_its_text() {
    let Some((host, port)) = live::postgres("a_column_smeltr_cannot_read_arrives_as_its_text") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("odd");
    owner.table(&table, "id bigint PRIMARY KEY, price money, spot point");
    owner.run(&format!("INSERT INTO {table} VALUES (1, 12.5, '(1,2)');"));
    let fields = [("id", DataType::I64), ("price", DataType::Str)];
    let query = format!("SELECT * FROM {table}");
    let mut s = source(&host, port, config(&query, Mode::EveryPoll, &fields));
    let read = records(&mut s);
    let chunk = &read[0].chunk;
    assert!(chunk.failed().is_empty(), "{:?}", chunk.failed());
    assert_eq!(chunk.column(1).var().unwrap().get(0), b"$12.50");

    let mode = Mode::Incrementing(Position {
        columns: vec!["price".to_string()],
        settle_secs: None,
        start: Start::Beginning,
    });
    let mut s = source(&host, port, config(&query, mode, &fields));
    let deadline = Instant::now() + Duration::from_secs(30);
    while s.connection_error().is_none() {
        let got = s
            .poll_batch(10_000, usize::MAX, Duration::from_millis(50))
            .expect("a poll");
        assert!(got.is_empty(), "a position read only as text sends nothing");
        assert!(Instant::now() < deadline, "the poll never failed");
    }
    let e = s.connection_error().unwrap();
    assert!(e.contains("`price` is of a type smeltr reads only as text"), "{e}");
}

#[test]
fn an_array_of_two_dimensions_arrives_nested_and_a_far_date_fails_the_poll() {
    let Some((host, port)) = live::postgres("an_array_of_two_dimensions_arrives_nested_and_a_far_date_fails_the_poll")
    else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("grid");
    owner.table(&table, "id bigint PRIMARY KEY, grid int4[], day date");
    owner.run(&format!(
        "INSERT INTO {table} VALUES (1, ARRAY[[1, 2], [3, NULL]], '2024-01-01');"
    ));
    let grid = DataType::Array(Box::new(DataType::Array(Box::new(DataType::I64))));
    let fields = [("id", DataType::I64), ("grid", grid.clone())];
    let query = format!("SELECT id, grid FROM {table}");
    let mut s = source(&host, port, config(&query, Mode::EveryPoll, &fields));
    let read = records(&mut s);
    let chunk = &read[0].chunk;
    assert!(chunk.failed().is_empty(), "{:?}", chunk.failed());
    let rows = chunk.column(1).list().unwrap();
    assert_eq!(rows.offsets, [0, 2]);
    let cells = rows.child.list().unwrap();
    assert_eq!(cells.offsets, [0, 2, 4]);
    assert_eq!(cells.child.i64().unwrap().values[..3], [1, 2, 3]);
    assert!(!cells.child.is_valid(3), "a null element stays null");

    owner.run(&format!("UPDATE {table} SET day = '5000000-01-01';"));
    let fields = [("id", DataType::I64), ("day", DataType::DateTime)];
    let query = format!("SELECT id, day FROM {table}");
    let mut s = source(&host, port, config(&query, Mode::EveryPoll, &fields));
    let deadline = Instant::now() + Duration::from_secs(30);
    while s.connection_error().is_none() {
        let got = s
            .poll_batch(10_000, usize::MAX, Duration::from_millis(50))
            .expect("a poll");
        assert!(got.is_empty(), "a date smeltr cannot hold sends nothing");
        assert!(Instant::now() < deadline, "the poll never failed");
    }
    let e = s.connection_error().unwrap();
    assert!(e.contains("a date past the range smeltr holds"), "{e}");
}

#[test]
fn a_bytea_column_arrives_as_bytes() {
    let Some((host, port)) = live::postgres("a_bytea_column_arrives_as_bytes") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("blob");
    owner.table(&table, "id bigint PRIMARY KEY, data bytea");
    owner.run(&format!("INSERT INTO {table} VALUES (1, '\\xdead00');"));
    let fields = [("id", DataType::I64), ("data", DataType::Bytes)];
    let query = format!("SELECT id, data FROM {table}");
    let mut s = source(&host, port, config(&query, Mode::EveryPoll, &fields));
    let read = records(&mut s);
    let chunk = &read[0].chunk;
    assert!(chunk.failed().is_empty(), "{:?}", chunk.failed());
    assert_eq!(chunk.column(1).data_type(), DataType::Bytes);
    assert_eq!(chunk.column(1).var().unwrap().get(0), [0xde, 0xad, 0x00]);
}

/// A source as a bundle declares it: loaded, opened as a worker opens it, read as its declared record, and resumed
/// from its progress by a second opening, as a worker the hold moves to would.
#[test]
fn a_postgres_source_from_a_bundle_reads_its_record_and_resumes() {
    let Some((host, port)) = live::postgres("a_postgres_source_from_a_bundle_reads_its_record_and_resumes") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("bundle");
    owner.table(&table, "id bigint PRIMARY KEY, at timestamptz NOT NULL, who text");
    owner.run(&format!(
        "INSERT INTO {table} VALUES (1, now() - interval '1 hour', 'ana'), (2, now() - interval '1 hour', 'li');"
    ));
    let json = serde_json::json!({
        "version": 1,
        "sources": [{
            "id": "pg", "type": "postgres", "host": host, "port": port, "database": DATABASE, "user": READER,
            "password": {"secret": "postgres-password", "version": 1}, "tls": "disable",
            "query": format!("SELECT id, at, who FROM {table}"),
            "mode": {"type": "incrementing", "position": ["at", "id"], "settleSecs": 1, "start": "beginning"},
            "fields": [{"name": "id", "type": "i64"}, {"name": "at", "type": "datetime"}, {"name": "who", "type": "str"}],
            "pollIntervalSecs": 1
        }]
    })
    .to_string();
    let system = smeltr::config_file::load_str(&json).expect("loads");
    let kind = system.topology.sources[0].kind.clone();
    assert!(kind.single_worker().is_some(), "one worker reads it");
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("postgres-password".to_string(), 1, PASSWORD.to_string())]);
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
                panic!("the poll failed: {e}");
            }
            if !got.is_empty() {
                return got;
            }
            assert!(Instant::now() < deadline, "no records within 30s");
        }
    };
    let meta = kind.meta();
    let mut first = open();
    let got = read(&mut first);
    let rows: Vec<RecordObject> = got.iter().map(|r| meta.record_object(r, "")).collect();
    assert_eq!(rows.iter().map(|o| o["who"].clone()).collect::<Vec<_>>(), ["ana", "li"]);
    assert_eq!(&*got[0].topic, format!("{host}:{port}/{DATABASE}"), "__source.database");

    let mut offsets = CommitMap::new();
    for r in &got {
        let at = offsets
            .entry(TopicPartition::new(r.topic.clone(), r.partition))
            .or_insert(r.offset);
        *at = (*at).max(r.offset);
    }
    first.commit(&offsets).expect("committed");
    let progress = first.take_progress().expect("progress");
    drop(first);

    owner.run(&format!(
        "INSERT INTO {table} VALUES (3, now() - interval '1 minute', 'mo');"
    ));
    let mut second = open();
    second.resume(progress).expect("resumed");
    let got = read(&mut second);
    let rows: Vec<RecordObject> = got.iter().map(|r| meta.record_object(r, "")).collect();
    assert_eq!(
        rows.iter().map(|o| o["who"].clone()).collect::<Vec<_>>(),
        ["mo"],
        "only past the progress"
    );
}

/// Run: a capture of the source as its author has it, before any field is declared, then with them. The worker
/// describes the query, reads from the beginning, and says what the database plans for a poll.
#[test]
fn run_describes_the_query_and_reads_before_any_field_is_declared() {
    let Some((host, port)) = live::postgres("run_describes_the_query_and_reads_before_any_field_is_declared") else {
        return;
    };
    let owner = Owner::connect(&host, port);
    let table = unique("run");
    owner.table(&table, "id bigint PRIMARY KEY, name text, doc jsonb");
    owner.run(&format!(
        "INSERT INTO {table} SELECT n, 'n' || n, '{{}}' FROM generate_series(1, 30) AS n;"
    ));
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("postgres-password".to_string(), 1, PASSWORD.to_string())]);
    let run_query = |query: &str, mode: serde_json::Value, fields: serde_json::Value, batch: u32| {
        let source: smeltr::config_file::SourceDto = serde_json::from_value(serde_json::json!({
            "id": "pg", "type": "postgres", "host": host, "port": port, "database": DATABASE, "user": READER,
            "password": {"secret": "postgres-password", "version": 1}, "tls": "disable",
            "query": query, "mode": mode, "fields": fields, "batch": batch
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
    let whole = format!("SELECT id, name, doc FROM {table}");
    let run = |mode, fields, batch| run_query(&whole, mode, fields, batch);

    let report = run(serde_json::json!({"type": "everyPoll"}), serde_json::json!([]), 5000);
    assert_eq!(
        report.finished,
        Some(smeltr::heartbeat::CaptureEnd::Matches),
        "{report:?}"
    );
    let sql = report.sql.expect("what Run found");
    let columns: Vec<_> = sql
        .columns
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                c.database_type.as_str(),
                c.suggested.as_str(),
                c.unique,
            )
        })
        .collect();
    assert_eq!(
        columns,
        [
            ("id", "int8", "i64", true),
            ("name", "text", "str", false),
            ("doc", "jsonb", "variant", false)
        ]
    );
    assert!(sql.columns[0].table.as_deref().is_some_and(|t| t.ends_with(&table)));
    assert_eq!(
        sql.verdict, None,
        "a mode that reads the whole result has nothing to judge"
    );
    let rows: usize = report.batches.iter().map(|b| b.rows.len()).sum();
    assert_eq!(rows, 20, "a capture's worth, no more");
    assert_eq!(
        report.batches[0].columns[0].name, "id",
        "every column, declared as suggested"
    );

    let by_id = || serde_json::json!({"type": "incrementing", "position": ["id"]});
    let id_and_name = || serde_json::json!([{"name": "id", "type": "i64"}, {"name": "name", "type": "str"}]);
    let report = run(by_id(), id_and_name(), 20);
    let sql = report.sql.expect("what Run found");
    assert_eq!(
        sql.verdict,
        Some(smeltr::heartbeat::SqlVerdict::Indexed {
            indexes: vec![format!("{table}_pkey")],
            rows: 20
        }),
        "the primary key serves a poll of 20 rows"
    );
    // The plan is the source's own poll, not Run's 20 rows: a batch past the table reads it whole, and it is small.
    let sql = run(by_id(), id_and_name(), 5000).sql.expect("what Run found");
    assert_eq!(
        sql.verdict,
        Some(smeltr::heartbeat::SqlVerdict::Indexed {
            indexes: Vec::new(),
            rows: 810
        }),
        "a batch of 5000 plans the whole table"
    );
    let first = &report.batches[0];
    assert_eq!(
        first.rows[0].values[0],
        serde_json::json!(1),
        "from the beginning, whatever the start"
    );
    assert_eq!(
        first.columns.iter().filter(|c| !c.name.starts_with("__")).count(),
        2,
        "only the declared fields"
    );

    // A source whose poll would refuse a result past its own bound: Run reads the query's first rows instead.
    let report = run(serde_json::json!({"type": "everyPoll"}), serde_json::json!([]), 10);
    assert_eq!(
        report.finished,
        Some(smeltr::heartbeat::CaptureEnd::Matches),
        "{report:?}"
    );
    let rows: usize = report.batches.iter().map(|b| b.rows.len()).sum();
    assert_eq!(rows, 20, "a capture's worth, past the source's own bound of 10");

    let report = run(
        serde_json::json!({"type": "comparing", "key": ["id"]}),
        serde_json::json!([{"name": "id", "type": "i64"}, {"name": "name", "type": "str"}]),
        10,
    );
    assert_eq!(
        report.finished,
        Some(smeltr::heartbeat::CaptureEnd::Matches),
        "{report:?}"
    );
    assert_eq!(report.sql.expect("what Run found").verdict, None);
    let rows: usize = report.batches.iter().map(|b| b.rows.len()).sum();
    assert_eq!(rows, 20, "comparing values, Run reads a capture's worth too");

    // A query edited since its fields were declared: Run reads every column it returns now.
    let report = run(
        serde_json::json!({"type": "incrementing", "position": ["gone"]}),
        serde_json::json!([{"name": "gone", "type": "i64"}]),
        5000,
    );
    assert_eq!(
        report.finished,
        Some(smeltr::heartbeat::CaptureEnd::Matches),
        "{report:?}"
    );
    let names: Vec<_> = report.batches[0]
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .filter(|n| !n.starts_with("__"))
        .collect();
    assert_eq!(names, ["id", "name", "doc"]);

    // A result shorter than a capture's worth is read once: Run answers when the database does, each row once.
    let started = std::time::Instant::now();
    let report = run_query(
        &format!("SELECT id, name, doc FROM {table} WHERE id <= 3"),
        serde_json::json!({"type": "everyPoll"}),
        serde_json::json!([]),
        5000,
    );
    assert_eq!(
        report.finished,
        Some(smeltr::heartbeat::CaptureEnd::Matches),
        "{report:?}"
    );
    let rows: usize = report.batches.iter().map(|b| b.rows.len()).sum();
    assert_eq!(rows, 3, "each row once");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "not its deadline"
    );
}
