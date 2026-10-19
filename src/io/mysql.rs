//! The MySQL and MariaDB [`Driver`]: an SQL source's statements, run read-only on one connection, values in the
//! binary protocol of prepared statements.
//!
//! ```text
//!   Statement + values ──► START TRANSACTION READ ONLY ──► prepare ──► execute, each value bound ──► rows
//!                                                                                                    │
//!                                   Value per cell, decoded by the column's type  ◄──── COMMIT ◄─────┘
//! ```
//!
//! mysql_async is async; the connection and the runtime it is driven on are a [`Shell`]'s. MySQL and MariaDB speak
//! one protocol; where they differ (the statement timeout, how `EXPLAIN` writes its plan, what a JSON column is) the
//! server's own version string says which one answers.

use std::sync::Arc;

use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::prelude::Queryable as _;
use mysql_async::{ClientIdentity, Column, Conn, Opts, OptsBuilder, Params, SslOpts, Value as MyValue};

use super::IoError;
use super::sql::{Dialect, Statement, quote};
use super::sql_source::{
    CALL_TIMEOUT, CONNECT_TIMEOUT, Connection, Described, Driver, FULL_SCAN_ROWS, Failure, MAX_RESULT_BYTES, Rows,
    STATEMENT_MARGIN, Shell, Tls, Verdict, held, too_large,
};
use super::values::Value;
use crate::core::column::DataType;
use crate::core::variant;
use crate::ops::civil::{MICROS_PER_DAY, civil_from_days, days_from_civil};
use crate::plan::topology::{HeldSecrets, Identity, SecretRef};

const MICROS_PER_SECOND: i64 = 1_000_000;

/// The character set MySQL gives a column of bytes rather than text.
const BINARY_CHARSET: u16 = 63;

/// Server errors after which the server has closed the connection: a client idle past `wait_timeout` (MySQL), a
/// server shutting down, a connection killed (MariaDB).
const CLOSING_ERRORS: [u16; 3] = [4031, 1053, 1927];

/// Where a MySQL or MariaDB source connects, and as whom.
#[derive(Clone, Debug)]
pub struct MySqlConnection {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<SecretRef>,
    pub tls: Tls,
    /// The certificate presented and the authorities trusted, under `Tls::Require`.
    pub identity: Option<Arc<Identity>>,
}

pub struct MySqlDriver {
    shell: Shell<Conn>,
}

fn failed(e: mysql_async::Error) -> Failure {
    let (broken, found_closed) = match &e {
        mysql_async::Error::Server(s) => {
            let closing = CLOSING_ERRORS.contains(&s.code);
            (closing, closing)
        }
        mysql_async::Error::Io(_) | mysql_async::Error::Driver(mysql_async::DriverError::ConnectionClosed) => {
            (true, true)
        }
        // A driver error is the protocol going somewhere mysql_async did not expect; a new connection starts clean.
        _ => (true, false),
    };
    Failure {
        text: message(&e),
        broken,
        found_closed,
    }
}

impl MySqlDriver {
    /// Resolves the password and the identity from `secrets` now (an absent secret is an IoError, never an
    /// unauthenticated try); connects lazily on first use and again after a broken connection.
    pub fn new(connection: MySqlConnection, secrets: &HeldSecrets) -> Result<MySqlDriver, IoError> {
        let password = match &connection.password {
            Some(reference) => Some(super::held(secrets, reference, "the MySQL source", "password")?),
            None => None,
        };
        let ssl = match connection.tls {
            Tls::Disable => None,
            Tls::Require => Some(ssl_opts(connection.identity.as_deref(), secrets)?),
        };
        let opts = OptsBuilder::default()
            .ip_or_hostname(connection.host)
            .tcp_port(connection.port)
            .db_name(Some(connection.database))
            .user(Some(connection.user))
            .pass(password)
            // A server on this host would otherwise be reached through its socket, outside TLS.
            .prefer_socket(false)
            .ssl_opts(ssl)
            .connect_attribute("program_name", "smeltr");
        Ok(MySqlDriver {
            shell: Shell::new(opts.into())?,
        })
    }
}

impl Connection for Conn {
    const NAME: &'static str = "MySQL";
    type Opening = Opts;

    async fn open(opts: Opts) -> Result<Conn, Failure> {
        connect(opts).await
    }
}

/// The TLS a connection under `Tls::Require` uses. mysql_async builds its own rustls configuration, from roots and an
/// identity rather than a configuration, and verifies the server's certificate and name with them; its rustls is
/// smeltr's, so the provider is the one the process installed. The roots and identity are what
/// [`super::objects::tls`] gives every other client (`io::trust`), in place of mysql_async's built-in roots.
fn ssl_opts(identity: Option<&Identity>, secrets: &HeldSecrets) -> Result<SslOpts, IoError> {
    // Refuses a certificate, an authority or a key that cannot be read, in the words every other client uses.
    super::objects::tls(identity, secrets)?;
    let mut ssl = SslOpts::default()
        .with_root_certs(vec![super::trust::pem(identity)?.into_bytes().into()])
        .with_disable_built_in_roots(true);
    if let Some(identity) = identity
        && let Some(certificate) = &identity.certificate
        && let Some(presented) = super::trust::presented(identity, secrets)?
    {
        ssl = ssl.with_client_identity(Some(ClientIdentity::new(
            certificate.as_bytes().to_vec().into(),
            presented.key_pem.into_bytes().into(),
        )));
    }
    Ok(ssl)
}

async fn connect(opts: Opts) -> Result<Conn, Failure> {
    let mut conn = tokio::time::timeout(CONNECT_TIMEOUT, Conn::new(opts))
        .await
        .map_err(|_| format!("MySQL did not accept a connection within {CONNECT_TIMEOUT:?}"))?
        .map_err(failed)?;
    let version: Option<String> = conn.query_first("SELECT VERSION()").await.map_err(failed)?;
    let mariadb = version.is_some_and(|v| v.contains("MariaDB"));
    // smeltr reads a DATETIME as UTC, and the wait for late commits compares such a column with NOW() in the
    // session's zone; UTC makes the two agree, and reads a TIMESTAMP in UTC too. A call that times out here drops the
    // connection, and the server stops its statement only on a timeout of its own, a little earlier. A shorter one
    // the database already sets stays. MySQL counts its timeout in milliseconds, MariaDB in seconds; 0 is none.
    let limit = CALL_TIMEOUT - STATEMENT_MARGIN;
    let (variable, limit) = if mariadb {
        ("max_statement_time", limit.as_secs_f64())
    } else {
        ("max_execution_time", limit.as_millis() as f64)
    };
    let current: Option<String> = conn
        .query_first(format!("SELECT CAST(@@{variable} AS CHAR)"))
        .await
        .map_err(failed)?;
    let current = current.and_then(|c| c.parse::<f64>().ok()).unwrap_or(0.0);
    let timeout = if current > 0.0 && current <= limit {
        current
    } else {
        limit
    };
    conn.query_drop(format!("SET time_zone = '+00:00', SESSION {variable} = {timeout}"))
        .await
        .map_err(failed)?;
    Ok(conn)
}

/// Runs `work` in a read-only transaction, ended whichever way `work` ends, so no snapshot or lock outlives the
/// call. The server refuses a write inside it, whatever the user may do.
async fn read_only<T>(conn: &mut Conn, work: impl AsyncFnOnce(&mut Conn) -> Result<T, Failure>) -> Result<T, Failure> {
    conn.query_drop("START TRANSACTION READ ONLY").await.map_err(failed)?;
    match work(conn).await {
        Ok(found) => {
            conn.query_drop("COMMIT").await.map_err(failed)?;
            Ok(found)
        }
        Err(failure) => {
            if !failure.broken {
                // The call has failed already, and for a better reason than a rollback could give.
                let _ = conn.query_drop("ROLLBACK").await;
            }
            Err(failure)
        }
    }
}

impl Driver for MySqlDriver {
    fn dialect(&self) -> Dialect {
        Dialect::MySql
    }

    fn run(&mut self, statement: &Statement, values: &[Value]) -> Result<Rows, String> {
        let sql = statement.sql.clone();
        let values = values.to_vec();
        self.shell.call(move |conn| {
            let (sql, values) = (sql.clone(), values.clone());
            Box::pin(read_only(conn, async move |conn: &mut Conn| {
                // A prepared statement is exactly one statement: a second one smuggled into the query is refused
                // here, before anything runs.
                let mut prepared = conn.prep(&sql).await.map_err(failed)?;
                let cast = cast_unreadable(&sql, &prepared.columns());
                if let Some((cast_sql, _)) = &cast {
                    prepared = conn.prep(cast_sql).await.map_err(failed)?;
                }
                let cast = cast.map(|(_, names)| names);
                let mut found = conn.exec_iter(&prepared, params(&values)?).await.map_err(failed)?;
                let columns = found.columns().unwrap_or_else(|| prepared.columns());
                let mut rows = Vec::new();
                let mut bytes = 0;
                while let Some(row) = found.next().await.map_err(failed)? {
                    let row = read_row(&columns, row.unwrap())?;
                    bytes += held(&row);
                    if bytes > MAX_RESULT_BYTES {
                        // The rest of the result is still coming; a new connection is quicker than reading it.
                        return Err(Failure {
                            text: too_large(),
                            broken: true,
                            found_closed: false,
                        });
                    }
                    rows.push(row);
                }
                Ok(Rows {
                    columns: columns.iter().map(|c| c.name_str().into_owned()).collect(),
                    rows,
                    as_text: cast.into_iter().flatten().collect(),
                })
            }))
        })
    }

    fn describe(&mut self, statement: &Statement) -> Result<Vec<Described>, String> {
        // Its columns are the statement's, and only prepared alone does each name the table beneath it on MariaDB.
        let sql = inner(&statement.sql).to_string();
        self.shell.call(move |conn| {
            let sql = sql.clone();
            Box::pin(read_only(conn, async move |conn: &mut Conn| {
                let prepared = conn.prep(&sql).await.map_err(failed)?;
                let mut described = Vec::with_capacity(prepared.columns().len());
                for column in prepared.columns().iter() {
                    let (schema, table) = (column.schema_str(), column.org_table_str());
                    let (table, unique) = if table.is_empty() {
                        (None, false)
                    } else {
                        let params = (
                            schema.to_string(),
                            table.to_string(),
                            column.org_name_str().into_owned(),
                        );
                        let unique: Option<i64> = conn.exec_first(UNIQUE, params).await.map_err(failed)?;
                        (Some(format!("{schema}.{table}")), unique.is_some())
                    };
                    described.push(Described {
                        name: column.name_str().into_owned(),
                        database_type: type_name(column),
                        suggested: suggested(column),
                        table,
                        unique,
                    });
                }
                Ok(described)
            }))
        })
    }

    fn plan(&mut self, statement: &Statement, values: &[Value]) -> Result<Verdict, String> {
        let sql = format!("EXPLAIN FORMAT=JSON {}", statement.sql);
        let limit = limit(&statement.sql);
        let values = values.to_vec();
        self.shell.call(move |conn| {
            let (sql, values) = (sql.clone(), values.clone());
            Box::pin(read_only(conn, async move |conn: &mut Conn| {
                let prepared = conn.prep(&sql).await.map_err(failed)?;
                let found: Option<mysql_async::Row> =
                    conn.exec_first(&prepared, params(&values)?).await.map_err(failed)?;
                let plan = match found.map(mysql_async::Row::unwrap).as_deref() {
                    Some([MyValue::Bytes(text)]) => match super::values::json(text) {
                        Ok(Value::Json(plan)) => plan,
                        _ => return Err("EXPLAIN returned a plan that is not JSON".to_string().into()),
                    },
                    _ => return Err("EXPLAIN returned no plan".to_string().into()),
                };
                let rows = limit.map_or(returned(&plan), |limit| returned(&plan).min(limit));
                Ok(verdict(&scans(&plan), indexes(&plan), rows))
            }))
        })
    }
}

/// Whether a unique index or primary key of table `?.?` covers its column `?` alone. A prefix of the column that is
/// unique leaves the whole column unique too.
const UNIQUE: &str = "SELECT 1 FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
GROUP BY INDEX_NAME
HAVING COUNT(*) = 1 AND MAX(COLUMN_NAME) = ?
LIMIT 1";

/// The database's own words for `e`: a server's message as it wrote it, else the cause beneath mysql_async's own
/// wrappers, which only repeat what kind of error it is.
fn message(e: &mysql_async::Error) -> String {
    match e {
        mysql_async::Error::Server(s) => s.message.clone(),
        other => {
            let mut cause: &dyn std::error::Error = other;
            while let Some(inner) = cause.source() {
                cause = inner;
            }
            cause.to_string()
        }
    }
}

/// The query inside smeltr's own statement (`super::sql::statement`), which reads it as `q`; the whole statement when
/// it holds none. MariaDB names `q` as the table of every column read through it.
fn inner(sql: &str) -> &str {
    sql.strip_prefix("SELECT * FROM (\n")
        .and_then(|rest| rest.rfind("\n) AS q").map(|end| &rest[..end]))
        .unwrap_or(sql)
}

/// The statement's own `LIMIT`, which smeltr writes last, alone on its line.
fn limit(sql: &str) -> Option<u64> {
    sql.rsplit_once("\nLIMIT ").and_then(|(_, n)| n.trim().parse().ok())
}

fn params(values: &[Value]) -> Result<Params, String> {
    if values.is_empty() {
        return Ok(Params::Empty);
    }
    values
        .iter()
        .enumerate()
        .map(|(i, value)| encode(i + 1, value))
        .collect::<Result<_, _>>()
        .map(Params::Positional)
}

/// `value` bound to placeholder `number`. MySQL converts a bound value to whatever the placeholder is compared with,
/// so each value is sent as the type closest to its own.
fn encode(number: usize, value: &Value) -> Result<MyValue, String> {
    Ok(match value {
        Value::Null => MyValue::NULL,
        Value::Bool(b) => MyValue::Int(i64::from(*b)),
        Value::Int(i) => MyValue::Int(*i),
        Value::Float(f) => MyValue::Double(*f),
        Value::Str(s) => MyValue::Bytes(s.as_bytes().to_vec()),
        Value::Bytes(b) => MyValue::Bytes(b.clone()),
        Value::Time(t) => {
            date_value(*t).ok_or_else(|| format!("placeholder {number} is a time before year 0 or after 9999"))?
        }
        Value::Duration(d) => time_value(*d),
        Value::Json(j) => {
            let mut text = Vec::new();
            super::values::json_text(j, &mut text);
            MyValue::Bytes(text)
        }
        Value::Array(_) => return Err(format!("placeholder {number} is an array, and MySQL has no array type")),
    })
}

/// A time as MySQL's date and time, in UTC: the session's zone.
fn date_value(micros: i64) -> Option<MyValue> {
    let (days, of_day) = (micros.div_euclid(MICROS_PER_DAY), micros.rem_euclid(MICROS_PER_DAY));
    let (year, month, day) = civil_from_days(days);
    let year = u16::try_from(year).ok().filter(|y| *y <= 9999)?;
    let seconds = of_day / MICROS_PER_SECOND;
    Some(MyValue::Date(
        year,
        month as u8,
        day as u8,
        (seconds / 3600) as u8,
        (seconds / 60 % 60) as u8,
        (seconds % 60) as u8,
        (of_day % MICROS_PER_SECOND) as u32,
    ))
}

/// A duration as MySQL's TIME: a sign, then days, hours, minutes, seconds and microseconds.
fn time_value(micros: i64) -> MyValue {
    let magnitude = micros.unsigned_abs();
    let seconds = magnitude / MICROS_PER_SECOND as u64;
    MyValue::Time(
        micros < 0,
        (seconds / 86_400) as u32,
        (seconds / 3600 % 24) as u8,
        (seconds / 60 % 60) as u8,
        (seconds % 60) as u8,
        (magnitude % MICROS_PER_SECOND as u64) as u32,
    )
}

fn read_row(columns: &[Column], row: Vec<MyValue>) -> Result<Vec<Value>, String> {
    columns
        .iter()
        .zip(row)
        .map(|(column, value)| {
            decode(column, value)
                .map_err(|cause| format!("column `{}` ({}): {cause}", column.name_str(), type_name(column)))
        })
        .collect()
}

/// One cell read as a [`Value`], by the column's type.
fn decode(column: &Column, value: MyValue) -> Result<Value, String> {
    use ColumnType::*;
    if !readable(column) {
        return Err(format!("smeltr does not read the type {}", type_name(column)));
    }
    Ok(match value {
        MyValue::NULL => Value::Null,
        // BOOL is TINYINT(1), which holds any TINYINT: only 0 and 1 are a boolean.
        MyValue::Int(i @ (0 | 1)) if boolean(column) => Value::Bool(i == 1),
        MyValue::UInt(u @ (0 | 1)) if boolean(column) => Value::Bool(u == 1),
        MyValue::Int(i) => Value::Int(i),
        MyValue::UInt(u) => unsigned(u),
        MyValue::Float(f) => Value::Float(f.into()),
        MyValue::Double(f) => Value::Float(f),
        MyValue::Date(year, month, day, hour, minute, second, micros) => {
            datetime(year, month, day, hour, minute, second, micros)
        }
        MyValue::Time(negative, days, hours, minutes, seconds, micros) => {
            let magnitude =
                ((i64::from(days) * 24 + i64::from(hours)) * 60 + i64::from(minutes)) * 60 + i64::from(seconds);
            let magnitude = magnitude * MICROS_PER_SECOND + i64::from(micros);
            Value::Duration(if negative { -magnitude } else { magnitude })
        }
        MyValue::Bytes(raw) => match column.column_type() {
            MYSQL_TYPE_JSON => super::values::json(&raw)?,
            MYSQL_TYPE_BIT => {
                if raw.len() > 8 {
                    return Err(format!("{} bytes of bits where at most 8 were expected", raw.len()));
                }
                unsigned(raw.iter().fold(0u64, |n, b| n << 8 | u64::from(*b)))
            }
            // Text the server sends for a number it holds exactly.
            MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => Value::Str(utf8(raw)?),
            _ if bytes(column) => Value::Bytes(raw),
            _ => Value::Str(utf8(raw)?),
        },
    })
}

/// An unsigned integer: an integer while i64 holds it, else its decimal text, which keeps it whole.
fn unsigned(u: u64) -> Value {
    i64::try_from(u).map_or_else(|_| Value::Str(u.to_string()), Value::Int)
}

fn utf8(raw: Vec<u8>) -> Result<String, String> {
    String::from_utf8(raw).map_err(|e| e.to_string())
}

/// A date and time as microseconds since the Unix epoch, read as UTC: a TIMESTAMP is sent in the session's zone,
/// which is UTC, and a DATETIME holds no zone. A date with a zero month or day, MySQL's zero date among them, names
/// no day; it reads as null, an unknown time.
fn datetime(year: u16, month: u8, day: u8, hour: u8, minute: u8, second: u8, micros: u32) -> Value {
    if month == 0 || day == 0 {
        return Value::Null;
    }
    let days = days_from_civil(i64::from(year), u32::from(month), u32::from(day));
    let seconds = (i64::from(hour) * 60 + i64::from(minute)) * 60 + i64::from(second);
    Value::Time(days * MICROS_PER_DAY + seconds * MICROS_PER_SECOND + i64::from(micros))
}

/// MySQL's BOOL: a TINYINT(1).
fn boolean(column: &Column) -> bool {
    column.column_type() == ColumnType::MYSQL_TYPE_TINY && column.column_length() == 1
}

/// Whether a column of text types holds bytes: BINARY, VARBINARY and the BLOBs share their types with the text ones,
/// told apart by the binary character set.
fn bytes(column: &Column) -> bool {
    use ColumnType::*;
    column.character_set() == BINARY_CHARSET
        && matches!(
            column.column_type(),
            MYSQL_TYPE_VARCHAR
                | MYSQL_TYPE_VAR_STRING
                | MYSQL_TYPE_STRING
                | MYSQL_TYPE_TINY_BLOB
                | MYSQL_TYPE_MEDIUM_BLOB
                | MYSQL_TYPE_LONG_BLOB
                | MYSQL_TYPE_BLOB
        )
}

/// Whether [`decode`] reads the column.
fn readable(column: &Column) -> bool {
    use ColumnType::*;
    !matches!(
        column.column_type(),
        MYSQL_TYPE_GEOMETRY | MYSQL_TYPE_VECTOR | MYSQL_TYPE_UNKNOWN | MYSQL_TYPE_TYPED_ARRAY
    )
}

/// What a column of this type is declared as by default. MariaDB's JSON is LONGTEXT under another name, so it is
/// text there.
fn suggested(column: &Column) -> Option<DataType> {
    use ColumnType::*;
    if !readable(column) {
        // `cast_unreadable` reads it as its text.
        return Some(DataType::Str);
    }
    Some(match column.column_type() {
        _ if boolean(column) => DataType::Bool,
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG | MYSQL_TYPE_LONGLONG
        | MYSQL_TYPE_YEAR | MYSQL_TYPE_BIT => DataType::I64,
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => DataType::F64,
        MYSQL_TYPE_DATE
        | MYSQL_TYPE_NEWDATE
        | MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_TIMESTAMP2 => DataType::DateTime,
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => DataType::Duration,
        MYSQL_TYPE_JSON => DataType::Variant,
        _ if bytes(column) => DataType::Bytes,
        _ => DataType::Str,
    })
}

/// The database's name for the column's type, as near as a result's columns tell it: they share one type between
/// TEXT and BLOB, say, and leave the character set to tell them apart.
fn type_name(column: &Column) -> String {
    use ColumnType::*;
    let flags = column.flags();
    let binary = column.character_set() == BINARY_CHARSET;
    let name = match column.column_type() {
        _ if boolean(column) => "tinyint(1)",
        MYSQL_TYPE_TINY => "tinyint",
        MYSQL_TYPE_SHORT => "smallint",
        MYSQL_TYPE_INT24 => "mediumint",
        MYSQL_TYPE_LONG => "int",
        MYSQL_TYPE_LONGLONG => "bigint",
        MYSQL_TYPE_FLOAT => "float",
        MYSQL_TYPE_DOUBLE => "double",
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => "decimal",
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => "date",
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => "datetime",
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => "timestamp",
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => "time",
        MYSQL_TYPE_YEAR => "year",
        MYSQL_TYPE_BIT => "bit",
        MYSQL_TYPE_JSON => "json",
        MYSQL_TYPE_GEOMETRY => "geometry",
        MYSQL_TYPE_VECTOR => "vector",
        MYSQL_TYPE_NULL => "null",
        MYSQL_TYPE_ENUM => "enum",
        MYSQL_TYPE_SET => "set",
        MYSQL_TYPE_STRING if flags.contains(ColumnFlags::ENUM_FLAG) => "enum",
        MYSQL_TYPE_STRING if flags.contains(ColumnFlags::SET_FLAG) => "set",
        MYSQL_TYPE_STRING if binary => "binary",
        MYSQL_TYPE_STRING => "char",
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING if binary => "varbinary",
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => "varchar",
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB if binary => "blob",
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => "text",
        MYSQL_TYPE_UNKNOWN | MYSQL_TYPE_TYPED_ARRAY => "unknown",
    };
    let numeric = matches!(
        column.column_type(),
        MYSQL_TYPE_TINY
            | MYSQL_TYPE_SHORT
            | MYSQL_TYPE_INT24
            | MYSQL_TYPE_LONG
            | MYSQL_TYPE_LONGLONG
            | MYSQL_TYPE_FLOAT
            | MYSQL_TYPE_DOUBLE
            | MYSQL_TYPE_DECIMAL
            | MYSQL_TYPE_NEWDECIMAL
    );
    if numeric && !boolean(column) && flags.contains(ColumnFlags::UNSIGNED_FLAG) {
        format!("{name} unsigned")
    } else {
        name.to_string()
    }
}

/// `sql` reading each column of a type smeltr cannot read as its text, or None when it reads every column. A
/// query's own types are the user's to choose, and `SELECT *` takes whatever the table holds; text keeps the value.
fn cast_unreadable(sql: &str, columns: &[Column]) -> Option<(String, Vec<String>)> {
    let mut names = std::collections::HashSet::new();
    // Two columns of one name cannot be named apart; the poll refuses them, saying so, without the cast.
    if columns.iter().all(readable) || !columns.iter().all(|c| names.insert(c.name_str())) {
        return None;
    }
    let rest = sql.strip_prefix("SELECT *")?;
    let list: Vec<String> = columns
        .iter()
        .map(|c| {
            let name = quote(Dialect::MySql, &c.name_str());
            if readable(c) {
                format!("q.{name}")
            } else if c.column_type() == ColumnType::MYSQL_TYPE_GEOMETRY {
                // Its bytes are the server's own storage; its text is the WKT anyone reads.
                format!("ST_AsText(q.{name}) AS {name}")
            } else {
                format!("CAST(q.{name} AS CHAR) AS {name}")
            }
        })
        .collect();
    let cast = columns
        .iter()
        .filter(|c| !readable(c))
        .map(|c| c.name_str().into_owned())
        .collect();
    Some((format!("SELECT {}{rest}", list.join(", ")), cast))
}

/// One read of a whole table in a plan.
#[derive(Debug, PartialEq)]
struct Scan {
    /// The table as the query names it: its alias, when it has one. EXPLAIN gives no other.
    name: String,
    rows: u64,
}

/// A number in a plan. MySQL writes some as text.
fn number(value: Option<&[u8]>) -> Option<f64> {
    match variant::read(value?) {
        variant::Value::I64(i) => Some(i as f64),
        variant::Value::F64(f) => Some(f),
        variant::Value::Str(s) => s.parse().ok(),
        _ => None,
    }
}

/// The text at `key` in the plan object `node`.
fn text_at<'p>(node: &'p [u8], key: &str) -> Option<&'p str> {
    match variant::read(variant::member(node, key)?) {
        variant::Value::Str(s) => Some(s),
        _ => None,
    }
}

/// The rows a table read examines: `rows_examined_per_scan` in MySQL's plan, `rows` in MariaDB's.
fn examined(table: &[u8]) -> Option<f64> {
    number(variant::member(table, "rows_examined_per_scan")).or_else(|| number(variant::member(table, "rows")))
}

/// Whether a table in a plan is one the statement made rather than one the database holds: a derived table, a CTE,
/// a union's result.
fn made(table: &[u8]) -> bool {
    variant::member(table, "materialized_from_subquery").is_some()
        || variant::member(table, "materialized").is_some()
        || text_at(table, "table_name").is_none_or(|n| n.starts_with('<'))
}

/// The table read the plan object `map` holds, if any.
fn table(map: &[u8]) -> Option<&[u8]> {
    variant::member(map, "table").filter(|t| variant::member(t, "table_name").is_some())
}

/// Every table read in `EXPLAIN FORMAT=JSON` output, at any depth, in plan order. MySQL and MariaDB both write each
/// as a `"table"` object, nested in join, sort, grouping and subquery objects that differ between them.
fn tables(explained: &[u8]) -> Vec<&[u8]> {
    fn walk<'p>(node: &'p [u8], found: &mut Vec<&'p [u8]>) {
        match variant::read(node) {
            variant::Value::Object(entries) => {
                found.extend(table(node));
                for (_, child) in entries {
                    walk(child, found);
                }
            }
            variant::Value::Array(items) => items.for_each(|item| walk(item, found)),
            _ => {}
        }
    }
    let mut found = Vec::new();
    walk(explained, &mut found);
    found
}

/// Every read of a whole table the database holds, with its estimate of the rows it reads.
fn scans(explained: &[u8]) -> Vec<Scan> {
    tables(explained)
        .into_iter()
        .filter(|t| text_at(t, "access_type") == Some("ALL") && !made(t))
        .map(|t| Scan {
            name: text_at(t, "table_name").unwrap_or_default().to_string(),
            rows: examined(t).map_or(0, |r| r as u64),
        })
        .collect()
}

/// Each index the plan reads through, by name, in plan order, once.
fn indexes(explained: &[u8]) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    for key in tables(explained).into_iter().filter_map(|t| text_at(t, "key")) {
        if !found.iter().any(|f| f == key) {
            found.push(key.to_string());
        }
    }
    found
}

/// The database's estimate of the rows the statement returns, before its `LIMIT`. MySQL writes what each join
/// produces on its last table; MariaDB writes each table's rows and the share its filter keeps, multiplied.
fn returned(explained: &[u8]) -> u64 {
    /// The tables the block itself joins, leaving out those of its subqueries, which are blocks of their own.
    fn joined<'p>(node: &'p [u8], found: &mut Vec<&'p [u8]>) {
        match variant::read(node) {
            variant::Value::Object(entries) if variant::member(node, "query_block").is_none() => {
                found.extend(table(node));
                for (_, child) in entries {
                    joined(child, found);
                }
            }
            variant::Value::Array(items) => items.for_each(|item| joined(item, found)),
            _ => {}
        }
    }
    fn block(query_block: &[u8]) -> f64 {
        if let Some(union) = variant::member(query_block, "union_result") {
            let specifications = variant::member(union, "query_specifications").map(variant::read);
            let Some(variant::Value::Array(specifications)) = specifications else {
                return 0.0;
            };
            return specifications
                .filter_map(|s| variant::member(s, "query_block"))
                .map(block)
                .sum();
        }
        let mut found = Vec::new();
        joined(query_block, &mut found);
        if let Some(rows) = found
            .last()
            .and_then(|t| number(variant::member(t, "rows_produced_per_join")))
        {
            return rows;
        }
        found
            .iter()
            .map(|t| examined(t).unwrap_or(1.0) * number(variant::member(t, "filtered")).unwrap_or(100.0) / 100.0)
            .product()
    }
    variant::member(explained, "query_block").map_or(0, |b| block(b).round() as u64)
}

/// The largest scan past [`FULL_SCAN_ROWS`], or `Indexed` through `indexes`, returning about `rows`.
fn verdict(scans: &[Scan], indexes: Vec<String>, rows: u64) -> Verdict {
    match scans.iter().filter(|s| s.rows > FULL_SCAN_ROWS).max_by_key(|s| s.rows) {
        Some(scan) => Verdict::FullScan {
            relation: scan.name.clone(),
            rows: scan.rows,
        },
        None => Verdict::Indexed { indexes, rows },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `explained` as a plan reads, in `core::variant`'s encoding.
    fn plan(explained: serde_json::Value) -> Vec<u8> {
        match crate::io::values::json(&serde_json::to_vec(&explained).unwrap()).unwrap() {
            Value::Json(plan) => plan,
            other => panic!("not JSON: {other:?}"),
        }
    }

    fn column(ty: ColumnType) -> Column {
        Column::new(ty).with_name(b"c").with_character_set(BINARY_CHARSET)
    }

    fn text(ty: ColumnType) -> Column {
        column(ty).with_character_set(45)
    }

    #[test]
    fn cells_decode_as_the_postgresql_driver_decodes_the_same_type() {
        use ColumnType::*;
        let flag = column(MYSQL_TYPE_TINY).with_column_length(1);
        assert_eq!(decode(&flag, MyValue::Int(1)).unwrap(), Value::Bool(true));
        assert_eq!(decode(&flag, MyValue::Int(0)).unwrap(), Value::Bool(false));
        let small = column(MYSQL_TYPE_TINY).with_column_length(4);
        assert_eq!(decode(&small, MyValue::Int(7)).unwrap(), Value::Int(7));
        let big = column(MYSQL_TYPE_LONGLONG).with_flags(ColumnFlags::UNSIGNED_FLAG);
        assert_eq!(decode(&big, MyValue::UInt(5)).unwrap(), Value::Int(5));
        assert_eq!(
            decode(&big, MyValue::UInt(u64::MAX)).unwrap(),
            Value::Str("18446744073709551615".into()),
            "past i64, its decimal text"
        );
        assert_eq!(
            decode(&column(MYSQL_TYPE_FLOAT), MyValue::Float(0.5)).unwrap(),
            Value::Float(0.5)
        );
        assert_eq!(
            decode(&column(MYSQL_TYPE_NEWDECIMAL), MyValue::Bytes(b"12.50".to_vec())).unwrap(),
            Value::Str("12.50".into())
        );
        assert_eq!(
            decode(&text(MYSQL_TYPE_JSON), MyValue::Bytes(br#"{"a": [1]}"#.to_vec())).unwrap(),
            crate::io::values::json(br#"{"a":[1]}"#).unwrap()
        );
        assert_eq!(
            decode(&column(MYSQL_TYPE_BLOB), MyValue::Bytes(vec![0xde, 0xad])).unwrap(),
            Value::Bytes(vec![0xde, 0xad])
        );
        assert_eq!(
            decode(&text(MYSQL_TYPE_BLOB), MyValue::Bytes(b"words".to_vec())).unwrap(),
            Value::Str("words".into()),
            "TEXT is a BLOB in a character set"
        );
        assert_eq!(
            decode(&column(MYSQL_TYPE_BIT), MyValue::Bytes(vec![0x01, 0x02])).unwrap(),
            Value::Int(258)
        );
        assert_eq!(
            decode(&column(MYSQL_TYPE_YEAR), MyValue::UInt(2024)).unwrap(),
            Value::Int(2024)
        );
        assert_eq!(decode(&column(MYSQL_TYPE_LONG), MyValue::NULL).unwrap(), Value::Null);
        assert!(decode(&text(MYSQL_TYPE_VAR_STRING), MyValue::Bytes(vec![0xff])).is_err());
        let err = decode(&column(MYSQL_TYPE_GEOMETRY), MyValue::Bytes(vec![0])).unwrap_err();
        assert!(err.contains("geometry"), "{err}");
    }

    #[test]
    fn times_read_as_utc_and_a_zero_date_as_null() {
        use ColumnType::*;
        // 2026-10-10 is 20_736 days after 1970-01-01.
        assert_eq!(
            decode(&column(MYSQL_TYPE_DATE), MyValue::Date(2026, 10, 10, 0, 0, 0, 0)).unwrap(),
            Value::Time(1_791_590_400_000_000)
        );
        assert_eq!(
            decode(&column(MYSQL_TYPE_DATETIME), MyValue::Date(1970, 1, 1, 1, 2, 3, 4)).unwrap(),
            Value::Time(3_723_000_004)
        );
        assert_eq!(
            decode(
                &column(MYSQL_TYPE_TIMESTAMP),
                MyValue::Date(1969, 12, 31, 23, 59, 59, 0)
            )
            .unwrap(),
            Value::Time(-1_000_000)
        );
        assert_eq!(
            decode(&column(MYSQL_TYPE_DATETIME), MyValue::Date(0, 0, 0, 0, 0, 0, 0)).unwrap(),
            Value::Null
        );
        assert_eq!(
            decode(&column(MYSQL_TYPE_TIME), MyValue::Time(true, 1, 2, 3, 4, 5)).unwrap(),
            Value::Duration(-(((24 + 2) * 60 + 3) * 60 + 4) * MICROS_PER_SECOND - 5)
        );
        for micros in [
            0,
            -1,
            1_791_590_400_123_456,
            -62_167_219_200_000_000,
            253_402_300_799_999_999,
        ] {
            let MyValue::Date(y, mo, d, h, mi, s, us) = date_value(micros).unwrap() else {
                panic!("a date");
            };
            assert_eq!(datetime(y, mo, d, h, mi, s, us), Value::Time(micros), "{micros}");
        }
        assert_eq!(date_value(253_402_300_800_000_000), None, "year 10000");
        assert_eq!(date_value(-62_167_219_200_000_001), None, "before year 0");
        for micros in [
            0,
            5,
            -5,
            3 * MICROS_PER_DAY + 7_000_001,
            -(838 * 3600 * MICROS_PER_SECOND),
        ] {
            let MyValue::Time(negative, days, h, m, s, us) = time_value(micros) else {
                panic!("a time");
            };
            assert_eq!(
                decode(&column(MYSQL_TYPE_TIME), MyValue::Time(negative, days, h, m, s, us)).unwrap(),
                Value::Duration(micros)
            );
        }
    }

    #[test]
    fn values_bind_as_the_type_nearest_their_own() {
        assert_eq!(encode(1, &Value::Bool(true)).unwrap(), MyValue::Int(1));
        assert_eq!(
            encode(1, &Value::Str("a".into())).unwrap(),
            MyValue::Bytes(b"a".to_vec())
        );
        assert_eq!(
            encode(1, &Value::Time(1_791_590_400_000_001)).unwrap(),
            MyValue::Date(2026, 10, 10, 0, 0, 0, 1)
        );
        assert_eq!(
            encode(1, &crate::io::values::json(b"[1]").unwrap()).unwrap(),
            MyValue::Bytes(b"[1]".to_vec())
        );
        let err = encode(2, &Value::Array(Vec::new())).unwrap_err();
        assert_eq!(err, "placeholder 2 is an array, and MySQL has no array type");
        assert!(matches!(params(&[]).unwrap(), Params::Empty));
    }

    #[test]
    fn types_suggest_what_a_record_declares() {
        use ColumnType::*;
        let unsigned = column(MYSQL_TYPE_LONGLONG).with_flags(ColumnFlags::UNSIGNED_FLAG);
        assert_eq!(suggested(&unsigned), Some(DataType::I64));
        assert_eq!(type_name(&unsigned), "bigint unsigned");
        let flag = column(MYSQL_TYPE_TINY).with_column_length(1);
        assert_eq!(
            (suggested(&flag), type_name(&flag)),
            (Some(DataType::Bool), "tinyint(1)".into())
        );
        assert_eq!(suggested(&column(MYSQL_TYPE_NEWDECIMAL)), Some(DataType::Str));
        assert_eq!(suggested(&column(MYSQL_TYPE_DATETIME)), Some(DataType::DateTime));
        assert_eq!(suggested(&column(MYSQL_TYPE_TIME)), Some(DataType::Duration));
        assert_eq!(suggested(&text(MYSQL_TYPE_JSON)), Some(DataType::Variant));
        assert_eq!(suggested(&column(MYSQL_TYPE_YEAR)), Some(DataType::I64));
        assert_eq!(suggested(&column(MYSQL_TYPE_BIT)), Some(DataType::I64));
        let varbinary = column(MYSQL_TYPE_VAR_STRING);
        assert_eq!(
            (suggested(&varbinary), type_name(&varbinary)),
            (Some(DataType::Bytes), "varbinary".into())
        );
        let varchar = text(MYSQL_TYPE_VAR_STRING);
        assert_eq!(
            (suggested(&varchar), type_name(&varchar)),
            (Some(DataType::Str), "varchar".into())
        );
        let choice = text(MYSQL_TYPE_STRING).with_flags(ColumnFlags::ENUM_FLAG);
        assert_eq!(
            (suggested(&choice), type_name(&choice)),
            (Some(DataType::Str), "enum".into())
        );
        assert_eq!(type_name(&text(MYSQL_TYPE_BLOB)), "text");
        assert_eq!(suggested(&column(MYSQL_TYPE_GEOMETRY)), Some(DataType::Str));
    }

    #[test]
    fn a_column_smeltr_cannot_read_is_read_as_its_text() {
        use ColumnType::*;
        let sql = "SELECT * FROM (\nSELECT * FROM t\n) AS q\nLIMIT 10";
        let columns = [
            column(MYSQL_TYPE_LONG).with_name(b"id"),
            column(MYSQL_TYPE_GEOMETRY).with_name(b"spot"),
            column(MYSQL_TYPE_UNKNOWN).with_name(b"odd"),
        ];
        let (cast, names) = cast_unreadable(sql, &columns).unwrap();
        assert_eq!(
            cast,
            "SELECT q.`id`, ST_AsText(q.`spot`) AS `spot`, CAST(q.`odd` AS CHAR) AS `odd` FROM (\nSELECT * FROM t\n) AS q\nLIMIT 10"
        );
        assert_eq!(names, ["spot", "odd"]);
        assert_eq!(cast_unreadable(sql, &columns[..1]), None);
    }

    /// Read from what `sql::statement` writes, so a change to its shape fails here rather than in a describe.
    #[test]
    fn the_query_inside_a_statement_and_its_limit_are_found() {
        let query = super::super::sql::Query::parse("SELECT a FROM t WHERE (x) AS q", Dialect::MySql).unwrap();
        let position = ["a".to_string()];
        let statement = super::super::sql::statement(
            &query,
            &super::super::sql::Poll {
                position: &position,
                after: true,
                start: false,
                settle_secs: None,
                batch: 500,
                fields: &[],
            },
        );
        let sql = statement.sql.as_str();
        assert_eq!(inner(sql), "SELECT a FROM t WHERE (x) AS q");
        assert_eq!(limit(sql), Some(500));
        assert_eq!(inner("SELECT 1"), "SELECT 1");
        assert_eq!(limit("SELECT 1"), None);
    }

    #[test]
    fn a_mysql_plan_reading_a_whole_table_is_a_full_scan() {
        let explained = plan(serde_json::json!({"query_block": {"select_id": 1, "table": {
            "table_name": "plan_big", "access_type": "ALL", "rows_examined_per_scan": 999_504,
            "rows_produced_per_join": 99_950, "filtered": "10.00"}}}));
        assert_eq!(
            verdict(&scans(&explained), indexes(&explained), returned(&explained)),
            Verdict::FullScan {
                relation: "plan_big".into(),
                rows: 999_504
            }
        );
        assert_eq!(returned(&explained), 99_950);
    }

    #[test]
    fn a_mysql_plan_names_its_indexes_and_leaves_a_derived_table_out() {
        let explained = plan(serde_json::json!({"query_block": {"select_id": 1, "table": {
            "table_name": "q", "access_type": "ALL", "rows_examined_per_scan": 333_135, "rows_produced_per_join": 333_135,
            "materialized_from_subquery": {"query_block": {"union_result": {"table_name": "<union2,3>", "access_type": "ALL",
                "query_specifications": [{"query_block": {"select_id": 2, "nested_loop": [
                    {"table": {"table_name": "a", "access_type": "range", "key": "a_at", "rows_examined_per_scan": 500,
                               "rows_produced_per_join": 500}},
                    {"table": {"table_name": "b", "access_type": "eq_ref", "key": "PRIMARY",
                               "rows_examined_per_scan": 1, "rows_produced_per_join": 500}}]}},
                    {"query_block": {"select_id": 3, "message": "No tables used"}}]}}}}}}));
        assert!(scans(&explained).is_empty(), "q is the statement's own");
        assert_eq!(
            verdict(&scans(&explained), indexes(&explained), returned(&explained)),
            Verdict::Indexed {
                indexes: vec!["a_at".into(), "PRIMARY".into()],
                rows: 333_135
            }
        );
        let ordered = plan(
            serde_json::json!({"query_block": {"ordering_operation": {"using_filesort": false, "table": {
            "table_name": "plan_big", "access_type": "ref", "key": "plan_big_indexed",
            "rows_examined_per_scan": 1, "rows_produced_per_join": 1}}}}),
        );
        assert_eq!(
            verdict(&scans(&ordered), indexes(&ordered), returned(&ordered)),
            Verdict::Indexed {
                indexes: vec!["plan_big_indexed".into()],
                rows: 1
            }
        );
    }

    #[test]
    fn a_mariadb_plan_is_judged_as_a_mysql_one() {
        let scan = plan(
            serde_json::json!({"query_block": {"select_id": 1, "cost": 160.37, "nested_loop": [{"table": {
            "table_name": "plan_big", "access_type": "ALL", "loops": 1, "rows": 967_853, "cost": 160.37,
            "filtered": 100, "attached_condition": "plan_big.plain = 5"}}]}}),
        );
        assert_eq!(
            verdict(&scans(&scan), indexes(&scan), returned(&scan)),
            Verdict::FullScan {
                relation: "plan_big".into(),
                rows: 967_853
            }
        );
        let derived = plan(
            serde_json::json!({"query_block": {"select_id": 1, "nested_loop": [{"table": {
            "table_name": "<derived2>", "access_type": "ALL", "rows": 200, "filtered": 50,
            "materialized": {"query_block": {"select_id": 2, "nested_loop": [
                {"table": {"table_name": "a", "access_type": "ref", "key": "a_at", "rows": 40, "filtered": 100}},
                {"table": {"table_name": "b", "access_type": "eq_ref", "key": "PRIMARY", "rows": 1, "filtered": 100}}
            ]}}}}]}}),
        );
        assert!(scans(&derived).is_empty());
        assert_eq!(
            verdict(&scans(&derived), indexes(&derived), returned(&derived)),
            Verdict::Indexed {
                indexes: vec!["a_at".into(), "PRIMARY".into()],
                rows: 100
            },
            "the outer block's rows, past its filter"
        );
        let small = plan(serde_json::json!({"query_block": {"nested_loop": [{"table": {
            "table_name": "few", "access_type": "ALL", "rows": 40, "filtered": 100}}]}}));
        assert_eq!(
            verdict(&scans(&small), indexes(&small), returned(&small)),
            Verdict::Indexed {
                indexes: Vec::new(),
                rows: 40
            },
            "a small table read whole is no full scan"
        );
    }
}
