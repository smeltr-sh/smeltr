//! What an SQL source runs, for the Sources page to show before anything runs.
//!
//! The leader builds the statement text with the same `io::sql::statement` a poll runs, and opens no connection.

use axum::Json;

use smeltr::config_file::SourceDto;
use smeltr::io::sql::{Bind, Query, Refused, Statement};
use smeltr::io::sql_source::{Mode, SqlSourceConfig, Start};
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::plan::topology::SourceKind;

#[derive(serde::Deserialize)]
pub struct StatementRequest {
    source: SourceDto,
}

#[derive(Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Statements {
    /// What a poll runs before any row is sent.
    #[serde(skip_serializing_if = "Option::is_none")]
    first: Option<StatementDto>,
    /// What a poll runs once a row is sent, for a mode that reads past the last row.
    #[serde(skip_serializing_if = "Option::is_none")]
    later: Option<StatementDto>,
    /// What Run executes: the poll from the beginning, its first [`RUN_ROWS`] rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    run: Option<StatementDto>,
    /// What Run reads but a save refuses.
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<String>,
}

/// Run reads a query's own ORDER BY (`io::sql::Query::parse_for_run`), so its author can reach the choice that
/// replaces it.
const OWN_ORDER: &str = "Take the ORDER BY out before you save: Run reads it, but smeltr orders the rows itself, \
                         by the column you choose under how rows are found.";

/// How many rows Run reads, as the Sources page asks for them.
const RUN_ROWS: u32 = 20;

#[derive(Debug, PartialEq, serde::Serialize)]
pub struct StatementDto {
    sql: String,
    /// What fills each placeholder, in order.
    binds: Vec<BindDto>,
}

/// What fills one placeholder: a column's value in the last row sent, or the value the source starts from.
#[derive(Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum BindDto {
    Last(String),
    Start,
}

/// The statements `source` runs, as `io::sql::statement` writes them. A source that declares no fields yet reads
/// every poll, as Run reads it.
pub async fn sql_statement(Json(request): Json<StatementRequest>) -> Envelope<Statements> {
    statements(request.source)
        .map(
            |Shown {
                 first,
                 later,
                 run,
                 warning,
             }| Statements {
                first: Some(first),
                later,
                run: Some(run),
                warning,
            },
        )
        .map_err(Refusal::invalid)
        .into()
}

struct Shown {
    first: StatementDto,
    later: Option<StatementDto>,
    run: StatementDto,
    warning: Option<String>,
}

fn statements(source: SourceDto) -> Result<Shown, String> {
    let mut warning = None;
    let config = match source.kind.sql() {
        Some((kind, sql)) if sql.fields.is_empty() => SqlSourceConfig {
            query: match Query::parse(&sql.query, kind.dialect()) {
                Err(Refused::Ordered) => {
                    warning = Some(OWN_ORDER.to_string());
                    Query::parse_for_run(&sql.query, kind.dialect())
                }
                parsed => parsed,
            }
            .map_err(|why| format!("the query: {why}"))?,
            mode: Mode::EveryPoll,
            fields: Vec::new().into(),
            poll_interval: std::time::Duration::from_secs(sql.poll_interval_secs.0),
            batch: sql.batch.0,
            place: "".into(),
        },
        Some(_) => match smeltr::worker::capture::source_kind_of(source)? {
            SourceKind::Sql(sql) => sql.source,
            _ => unreachable!("an SQL source loads as an SQL one"),
        },
        None => return Err("only an SQL source runs a statement".to_string()),
    };
    let start = match &config.mode {
        Mode::Incrementing(p) | Mode::UpdatedAt(p) => p.start != Start::Beginning,
        Mode::Comparing { .. } | Mode::EveryPoll => false,
    };
    let first = shown(&config, config.statement(false, start));
    let later = (!config.position().is_empty()).then(|| shown(&config, config.statement(true, false)));
    // A mode that reads past the last row is run from the beginning; one that reads the whole result, only its first
    // rows (`worker::capture::run_sql_capture`).
    let run = match &config.mode {
        Mode::Incrementing(_) | Mode::UpdatedAt(_) => SqlSourceConfig {
            batch: RUN_ROWS,
            ..config.clone()
        },
        Mode::Comparing { .. } | Mode::EveryPoll => SqlSourceConfig {
            mode: Mode::Incrementing(smeltr::io::sql_source::Position {
                columns: Vec::new(),
                settle_secs: None,
                start: Start::Beginning,
            }),
            batch: RUN_ROWS,
            ..config.clone()
        },
    };
    let run = shown(&run, run.statement(false, false));
    Ok(Shown {
        first,
        later,
        run,
        warning,
    })
}

/// A statement with each bind named by the column it reads.
fn shown(config: &SqlSourceConfig, statement: Statement) -> StatementDto {
    let binds = statement
        .binds
        .into_iter()
        .map(|bind| match bind {
            Bind::Position(at) => BindDto::Last(config.position()[at].clone()),
            Bind::Last(column) => BindDto::Last(column),
            Bind::Start => BindDto::Start,
        })
        .collect();
    StatementDto {
        sql: statement.sql,
        binds,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(json: serde_json::Value) -> SourceDto {
        let mut base = serde_json::json!({
            "id": "s", "type": "postgres", "host": "db", "database": "d", "user": "u",
            "query": "SELECT id, at FROM t"
        });
        base.as_object_mut().unwrap().extend(json.as_object().unwrap().clone());
        serde_json::from_value(base).unwrap()
    }

    /// Before its fields are declared a source reads every poll, the whole query, nothing bound. Run reads its first rows.
    #[test]
    fn a_source_with_no_fields_shows_the_whole_query() {
        let Shown { first, later, run, .. } =
            statements(source(serde_json::json!({"mode": {"type": "everyPoll"}, "fields": []}))).expect("a statement");
        assert!(first.sql.contains("SELECT id, at FROM t"), "{}", first.sql);
        assert!(!first.sql.contains("ORDER BY"), "{}", first.sql);
        assert!(first.binds.is_empty());
        assert_eq!(later, None, "every poll is the same");
        assert!(run.sql.ends_with("LIMIT 20"), "Run reads its first rows: {}", run.sql);
    }

    /// A position mode shows its first poll, from where it starts, and every later one, past the last row sent,
    /// each bind named by its column.
    #[test]
    fn a_position_mode_shows_its_first_and_later_polls() {
        let Shown { first, later, run, .. } = statements(source(serde_json::json!({
            "mode": {"type": "incrementing", "position": ["id"], "start": "from", "startFrom": 10},
            "fields": [{"name": "id", "type": "i64"}, {"name": "at", "type": "str"}]
        })))
        .expect("a statement");
        assert_eq!(first.binds, [BindDto::Start], "{}", first.sql);
        let later = later.expect("a later poll");
        assert_eq!(later.binds, [BindDto::Last("id".to_string())], "{}", later.sql);
        assert!(later.sql.contains("ORDER BY q.\"id\""), "{}", later.sql);
        assert!(run.binds.is_empty(), "Run reads from the beginning: {}", run.sql);
        assert!(run.sql.ends_with("LIMIT 20"), "{}", run.sql);
    }

    /// Run reads a query with its own ORDER BY, so its author can reach the position that replaces it; the page warns
    /// that a save will not take it. Once fields are declared, the query is checked as a save checks it.
    #[test]
    fn a_query_with_its_own_order_runs_with_a_warning_until_its_fields_are_declared() {
        let query = "SELECT id, at FROM t ORDER BY id";
        let Shown { run, warning, .. } = statements(source(
            serde_json::json!({"query": query, "mode": {"type": "everyPoll"}, "fields": []}),
        ))
        .expect("Run reads it");
        assert!(run.sql.contains(query), "{}", run.sql);
        assert_eq!(warning.as_deref(), Some(OWN_ORDER));
        let declared = statements(source(serde_json::json!({
            "query": query,
            "mode": {"type": "everyPoll"},
            "fields": [{"name": "id", "type": "i64"}, {"name": "at", "type": "str"}]
        })));
        assert!(declared.is_err());
    }

    #[test]
    fn a_source_that_will_not_load_says_why() {
        let refused = statements(source(
            serde_json::json!({"query": "UPDATE t SET a = 1", "mode": {"type": "everyPoll"}, "fields": []}),
        ));
        assert!(refused.is_err());
    }
}
