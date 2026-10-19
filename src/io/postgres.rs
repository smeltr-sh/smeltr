//! The PostgreSQL [`Driver`]: an SQL source's statements, run read-only on one connection, values in PostgreSQL's
//! binary format.
//!
//! ```text
//!   Statement + values ──► BEGIN READ ONLY ──► prepare ──► bind each value as its placeholder's type ──► rows
//!                                                                                                       │
//!                                   Value per cell, decoded by the column's type  ◄──── COMMIT ◄────────┘
//! ```
//!
//! tokio-postgres is async; the connection and the runtime it is driven on are a [`Shell`]'s.

use std::error::Error as StdError;
use std::fmt::{self, Write as _};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use tokio_postgres::config::SslMode;
// What `to_sql` writes into. tokio-postgres re-exports the `bytes` type here for its own `to_sql_checked!`.
use futures_util::StreamExt as _;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, Column, Config, NoTls, Row};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::IoError;
use super::sql::{Dialect, Statement, quote};
use super::sql_source::{
    CALL_TIMEOUT, CONNECT_TIMEOUT, Connection, Described, Driver, FULL_SCAN_ROWS, Failure, MAX_RESULT_BYTES, Rows,
    STATEMENT_MARGIN, Shell, Tls, Verdict, held, too_large,
};
use super::values::Value;
use crate::core::column::DataType;
use crate::core::variant;
use crate::ops::civil::MICROS_PER_DAY;
use crate::plan::topology::{HeldSecrets, Identity, SecretRef};

type BoxError = Box<dyn StdError + Sync + Send>;

/// Microseconds from the Unix epoch to PostgreSQL's, 2000-01-01T00:00:00Z.
const EPOCH_OFFSET: i64 = 946_684_800_000_000;
/// An interval's month has no fixed length. It is read as 30 days, as PostgreSQL's own justify_days() counts it.
const DAYS_PER_MONTH: i64 = 30;

/// Where a PostgreSQL source connects, and as whom.
#[derive(Clone, Debug)]
pub struct PostgresConnection {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<SecretRef>,
    pub tls: Tls,
    /// The certificate presented and the authorities trusted, under `Tls::Require`.
    pub identity: Option<Arc<Identity>>,
}

pub struct PostgresDriver {
    shell: Shell<Client>,
}

/// What `connect` takes: the config, password resolved, and the TLS it connects under (`None` under `Tls::Disable`).
#[derive(Clone)]
pub struct Opening {
    config: Config,
    tls: Option<rustls::ClientConfig>,
}

fn failed(e: tokio_postgres::Error) -> Failure {
    // A FATAL or PANIC error ends the session: the server closes the connection after sending it.
    let fatal = e
        .as_db_error()
        .is_some_and(|db| matches!(db.severity(), "FATAL" | "PANIC"));
    Failure {
        text: message(&e),
        broken: fatal || e.is_closed(),
        found_closed: e.is_closed(),
    }
}

impl PostgresDriver {
    /// Resolves the password from `secrets` now (an absent secret is an IoError, never an unauthenticated try);
    /// connects lazily on first use and again after a broken connection.
    pub fn new(connection: PostgresConnection, secrets: &HeldSecrets) -> Result<PostgresDriver, IoError> {
        let mut config = Config::new();
        config
            .host(&connection.host)
            .port(connection.port)
            .dbname(&connection.database)
            .user(&connection.user)
            .application_name("smeltr")
            .connect_timeout(CONNECT_TIMEOUT);
        if let Some(reference) = &connection.password {
            config.password(super::held(secrets, reference, "the PostgreSQL source", "password")?);
        }
        let tls = match connection.tls {
            Tls::Disable => {
                config.ssl_mode(SslMode::Disable);
                None
            }
            Tls::Require => {
                config.ssl_mode(SslMode::Require);
                Some(super::objects::tls(connection.identity.as_deref(), secrets)?)
            }
        };
        Ok(PostgresDriver {
            shell: Shell::new(Opening { config, tls })?,
        })
    }
}

impl Connection for Client {
    const NAME: &'static str = "PostgreSQL";
    type Opening = Opening;

    async fn open(opening: Opening) -> Result<Client, Failure> {
        Ok(connect(opening.config, opening.tls).await?)
    }

    fn closed(&self) -> bool {
        self.is_closed()
    }
}

async fn connect(config: Config, tls: Option<rustls::ClientConfig>) -> Result<Client, String> {
    let client = match tls {
        None => {
            let (client, connection) = config.connect(NoTls).await.map_err(|e| message(&e))?;
            tokio::spawn(connection);
            client
        }
        Some(tls) => {
            let (client, connection) = config
                .connect(MakeRustlsConnect::new(tls))
                .await
                .map_err(|e| message(&e))?;
            tokio::spawn(connection);
            client
        }
    };
    // smeltr reads a `timestamp without time zone` as UTC, and the wait for late commits compares such a column with
    // `now()` in the session's zone; UTC makes the two agree. A call that times out here drops the connection, and
    // the server stops its statement only on a timeout of its own, a little earlier. A shorter one the database
    // already sets stays.
    let timeout_ms = (CALL_TIMEOUT - STATEMENT_MARGIN).as_millis();
    client
        .batch_execute(&format!(
            "SET TIME ZONE 'UTC'; SELECT set_config('statement_timeout', '{timeout_ms}', false) FROM pg_catalog.pg_settings \
             WHERE name = 'statement_timeout' AND (setting::bigint = 0 OR setting::bigint > {timeout_ms})"
        ))
        .await
        .map_err(|e| message(&e))?;
    Ok(client)
}

impl Driver for PostgresDriver {
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn run(&mut self, statement: &Statement, values: &[Value]) -> Result<Rows, String> {
        let sql = statement.sql.clone();
        let values = values.to_vec();
        self.shell.call(move |client| {
            let (sql, values) = (sql.clone(), values.clone());
            Box::pin(async move {
                let tx = client
                    .build_transaction()
                    .read_only(true)
                    .start()
                    .await
                    .map_err(failed)?;
                // Prepared through the extended protocol, which takes exactly one statement: a second one smuggled
                // into the query is refused here, before anything runs.
                let mut prepared = tx.prepare(&sql).await.map_err(failed)?;
                let cast = cast_unreadable(&sql, prepared.columns());
                if let Some((cast_sql, _)) = &cast {
                    prepared = tx.prepare(cast_sql).await.map_err(failed)?;
                }
                let cast = cast.map(|(_, names)| names);
                let params = params(&values);
                let found = tx.query_raw(&prepared, bound(&params)).await.map_err(failed)?;
                let mut found = std::pin::pin!(found);
                let mut rows = Vec::new();
                let mut bytes = 0;
                while let Some(row) = found.next().await {
                    let row = read_row(&row.map_err(failed)?)?;
                    bytes += held(&row);
                    if bytes > MAX_RESULT_BYTES {
                        return Err(Failure::from(too_large()));
                    }
                    rows.push(row);
                }
                let columns = prepared.columns().iter().map(|c| c.name().to_string()).collect();
                tx.commit().await.map_err(failed)?;
                Ok(Rows {
                    columns,
                    rows,
                    as_text: cast.into_iter().flatten().collect(),
                })
            })
        })
    }

    fn describe(&mut self, statement: &Statement) -> Result<Vec<Described>, String> {
        let sql = statement.sql.clone();
        self.shell.call(move |client| {
            let sql = sql.clone();
            Box::pin(async move {
                let tx = client
                    .build_transaction()
                    .read_only(true)
                    .start()
                    .await
                    .map_err(failed)?;
                let prepared = tx.prepare(&sql).await.map_err(failed)?;
                let mut described = Vec::with_capacity(prepared.columns().len());
                for column in prepared.columns() {
                    let (table, unique) = match (column.table_oid(), column.column_id()) {
                        (Some(oid), Some(attnum)) if oid != 0 && attnum > 0 => {
                            match tx.query_opt(TABLE_OF, &[&oid, &attnum]).await.map_err(failed)? {
                                Some(row) => {
                                    let schema: String = row.get(0);
                                    let name: String = row.get(1);
                                    (Some(format!("{schema}.{name}")), row.get(2))
                                }
                                None => (None, false),
                            }
                        }
                        _ => (None, false),
                    };
                    described.push(Described {
                        name: column.name().to_string(),
                        database_type: column.type_().name().to_string(),
                        suggested: suggested(column.type_()),
                        table,
                        unique,
                    });
                }
                tx.commit().await.map_err(failed)?;
                Ok(described)
            })
        })
    }

    fn plan(&mut self, statement: &Statement, values: &[Value]) -> Result<Verdict, String> {
        // VERBOSE adds each scan's schema, so a relation is named as `describe` names it.
        let sql = format!("EXPLAIN (VERBOSE, FORMAT JSON) {}", statement.sql);
        let values = values.to_vec();
        self.shell.call(move |client| {
            let (sql, values) = (sql.clone(), values.clone());
            Box::pin(async move {
                let tx = client
                    .build_transaction()
                    .read_only(true)
                    .start()
                    .await
                    .map_err(failed)?;
                let prepared = tx.prepare(&sql).await.map_err(failed)?;
                let params = params(&values);
                let found = tx.query(&prepared, &bound(&params)).await.map_err(failed)?;
                let plan = match found.first().map(read_row).transpose()?.as_deref() {
                    Some([Value::Json(plan)]) => plan.clone(),
                    _ => return Err("EXPLAIN returned no plan".to_string().into()),
                };
                let mut scans = scans(&plan);
                // A scan's own estimate is what passes its filter; the relation's size is what it reads.
                for scan in &mut scans {
                    if let Some(schema) = &scan.schema {
                        let row = tx
                            .query_opt(RELTUPLES, &[schema as &(dyn ToSql + Sync), &scan.name])
                            .await
                            .map_err(failed)?;
                        // -1 is a relation never analyzed.
                        let tuples: f64 = row.map_or(-1.0, |r| r.get(0));
                        if tuples > 0.0 {
                            scan.rows = scan.rows.max(tuples as u64);
                        }
                    }
                }
                tx.commit().await.map_err(failed)?;
                Ok(verdict(&scans, indexes(&plan), returned(&plan)))
            })
        })
    }
}

/// The schema and name of the table with oid `$1`, and whether a unique index or primary key covers its column
/// `$2` alone, for every row: a partial index does not.
const TABLE_OF: &str = "SELECT n.nspname::text, c.relname::text, EXISTS (
    SELECT 1 FROM pg_index i
    WHERE i.indrelid = c.oid AND (i.indisunique OR i.indisprimary) AND i.indisvalid
      AND i.indnkeyatts = 1 AND i.indkey[0] = $2 AND i.indpred IS NULL)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = $1";

const RELTUPLES: &str = "SELECT c.reltuples::float8 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2";

/// The database's own words for `e`: its message, then its detail and hint when it gave them.
fn message(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut text = db.message().to_string();
        for part in [db.detail(), db.hint()].into_iter().flatten() {
            text.push_str("; ");
            text.push_str(part);
        }
        return text;
    }
    match e.source().and_then(|s| s.downcast_ref::<Refusal>()) {
        Some(refusal) => refusal.0.clone(),
        None => e.to_string(),
    }
}

/// Why a value could not be bound or read, in words that already name what is wrong.
#[derive(Debug)]
struct Refusal(String);

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for Refusal {}

fn refuse(message: String) -> BoxError {
    Box::new(Refusal(message))
}

fn params(values: &[Value]) -> Vec<Param<'_>> {
    values
        .iter()
        .enumerate()
        .map(|(i, value)| Param { number: i + 1, value })
        .collect()
}

fn bound<'a>(params: &'a [Param<'_>]) -> Vec<&'a (dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}

fn read_row(row: &Row) -> Result<Vec<Value>, String> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(i, column)| {
            row.try_get::<_, Cell>(i)
                .map(|c| c.0)
                .map_err(|e| unreadable(column, &e))
        })
        .collect()
}

fn unreadable(column: &Column, e: &tokio_postgres::Error) -> String {
    let cause = e.source().map_or_else(|| e.to_string(), |s| s.to_string());
    format!("column `{}` ({}): {cause}", column.name(), column.type_().name())
}

/// What a column of `ty` is declared as by default; `None` for a type smeltr cannot read, or reads only as text
/// a record has no type for yet.
fn suggested(ty: &Type) -> Option<DataType> {
    if !readable(ty) {
        // `cast_unreadable` reads it as PostgreSQL prints it.
        return Some(DataType::Str);
    }
    match ty.kind() {
        // An element with no type of its own leaves the array a variant.
        Kind::Array(member) => Some(suggested(member).map_or(DataType::Variant, |e| DataType::Array(Box::new(e)))),
        Kind::Domain(base) => suggested(base),
        Kind::Enum(_) => Some(DataType::Str),
        _ if *ty == Type::BOOL => Some(DataType::Bool),
        _ if [Type::INT2, Type::INT4, Type::INT8, Type::OID].contains(ty) => Some(DataType::I64),
        _ if [Type::FLOAT4, Type::FLOAT8].contains(ty) => Some(DataType::F64),
        _ if text_like(ty) || [Type::NUMERIC, Type::UUID, Type::INET, Type::CIDR].contains(ty) => Some(DataType::Str),
        _ if [Type::TIMESTAMPTZ, Type::TIMESTAMP, Type::DATE].contains(ty) => Some(DataType::DateTime),
        _ if [Type::TIME, Type::INTERVAL].contains(ty) => Some(DataType::Duration),
        _ if [Type::JSON, Type::JSONB].contains(ty) => Some(DataType::Variant),
        _ if *ty == Type::BYTEA => Some(DataType::Bytes),
        _ => None,
    }
}

/// Whether [`decode`] reads `ty`.
fn readable(ty: &Type) -> bool {
    match ty.kind() {
        Kind::Array(member) => readable(member),
        Kind::Domain(base) => readable(base),
        Kind::Enum(_) => true,
        _ => {
            text_like(ty)
                || [
                    Type::BOOL,
                    Type::INT2,
                    Type::INT4,
                    Type::INT8,
                    Type::OID,
                    Type::FLOAT4,
                    Type::FLOAT8,
                    Type::NUMERIC,
                    Type::UUID,
                    Type::INET,
                    Type::CIDR,
                    Type::TIMESTAMP,
                    Type::TIMESTAMPTZ,
                    Type::DATE,
                    Type::TIME,
                    Type::INTERVAL,
                    Type::JSON,
                    Type::JSONB,
                    Type::BYTEA,
                ]
                .contains(ty)
        }
    }
}

/// `sql` reading each column of a type smeltr cannot read as its text, or None when it reads every column. A
/// query's own types are the user's to choose, and `SELECT *` takes whatever the table holds; text keeps the value.
fn cast_unreadable(sql: &str, columns: &[Column]) -> Option<(String, Vec<String>)> {
    let mut names = std::collections::HashSet::new();
    // Two columns of one name cannot be named apart; the poll refuses them, saying so, without the cast.
    if columns.iter().all(|c| readable(c.type_())) || !columns.iter().all(|c| names.insert(c.name())) {
        return None;
    }
    let rest = sql.strip_prefix("SELECT *")?;
    let list: Vec<String> = columns
        .iter()
        .map(|c| {
            let name = quote(Dialect::Postgres, c.name());
            if readable(c.type_()) {
                format!("q.{name}")
            } else {
                format!("q.{name}::text AS {name}")
            }
        })
        .collect();
    let cast = columns
        .iter()
        .filter(|c| !readable(c.type_()))
        .map(|c| c.name().to_string())
        .collect();
    Some((format!("SELECT {}{rest}", list.join(", ")), cast))
}

/// Types whose binary form is their text.
fn text_like(ty: &Type) -> bool {
    [
        Type::TEXT,
        Type::VARCHAR,
        Type::BPCHAR,
        Type::NAME,
        Type::UNKNOWN,
        Type::CHAR,
    ]
    .contains(ty)
        || (ty.name() == "citext" && matches!(ty.kind(), Kind::Simple))
}

/// One cell read as a [`Value`], by the column's type.
struct Cell(Value);

impl<'a> FromSql<'a> for Cell {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Cell, BoxError> {
        decode(ty, raw).map(Cell)
    }

    fn from_sql_null(_: &Type) -> Result<Cell, BoxError> {
        Ok(Cell(Value::Null))
    }

    /// Every type: one this cannot read fails in `from_sql`, naming the type, rather than here with tokio-postgres'
    /// message about Rust types.
    fn accepts(_: &Type) -> bool {
        true
    }
}

fn decode(ty: &Type, raw: &[u8]) -> Result<Value, BoxError> {
    match ty.kind() {
        Kind::Array(member) => return array(member, raw),
        Kind::Domain(base) => return decode(base, raw),
        Kind::Enum(_) => return Ok(Value::Str(utf8(raw)?)),
        _ => {}
    }
    Ok(if *ty == Type::BOOL {
        Value::Bool(fixed::<1>(raw)?[0] != 0)
    } else if *ty == Type::INT2 {
        Value::Int(i16::from_be_bytes(fixed(raw)?).into())
    } else if *ty == Type::INT4 {
        Value::Int(i32::from_be_bytes(fixed(raw)?).into())
    } else if *ty == Type::INT8 {
        Value::Int(i64::from_be_bytes(fixed(raw)?))
    } else if *ty == Type::OID {
        Value::Int(u32::from_be_bytes(fixed(raw)?).into())
    } else if *ty == Type::FLOAT4 {
        Value::Float(f32::from_be_bytes(fixed(raw)?).into())
    } else if *ty == Type::FLOAT8 {
        Value::Float(f64::from_be_bytes(fixed(raw)?))
    } else if *ty == Type::NUMERIC {
        Value::Str(numeric_text(raw)?)
    } else if text_like(ty) {
        Value::Str(utf8(raw)?)
    } else if *ty == Type::UUID {
        Value::Str(uuid_text(fixed(raw)?))
    } else if *ty == Type::INET || *ty == Type::CIDR {
        Value::Str(inet_text(raw)?)
    } else if *ty == Type::TIMESTAMP || *ty == Type::TIMESTAMPTZ {
        timestamp(i64::from_be_bytes(fixed(raw)?))?
    } else if *ty == Type::DATE {
        date(i32::from_be_bytes(fixed(raw)?))?
    } else if *ty == Type::TIME {
        Value::Duration(i64::from_be_bytes(fixed(raw)?))
    } else if *ty == Type::INTERVAL {
        let raw: [u8; 16] = fixed(raw)?;
        let micros = i64::from_be_bytes(raw[..8].try_into().expect("8 bytes"));
        let days = i32::from_be_bytes(raw[8..12].try_into().expect("4 bytes"));
        let months = i32::from_be_bytes(raw[12..].try_into().expect("4 bytes"));
        interval(micros, days, months)?
    } else if *ty == Type::JSON {
        crate::io::values::json(raw)?
    } else if *ty == Type::JSONB {
        match raw.split_first() {
            Some((1, text)) => crate::io::values::json(text)?,
            _ => return Err(refuse("jsonb in a version smeltr does not read".to_string())),
        }
    } else if *ty == Type::BYTEA {
        Value::Bytes(raw.to_vec())
    } else {
        return Err(refuse(format!("smeltr does not read the type {}", ty.name())));
    })
}

fn fixed<const N: usize>(raw: &[u8]) -> Result<[u8; N], BoxError> {
    raw.try_into()
        .map_err(|_| refuse(format!("{} bytes where {N} were expected", raw.len())))
}

fn utf8(raw: &[u8]) -> Result<String, BoxError> {
    Ok(std::str::from_utf8(raw)?.to_string())
}

/// A timestamp's microseconds since 2000. `infinity` and `-infinity` have no time in smeltr's terms; they read as
/// null, as an unknown time.
fn timestamp(micros: i64) -> Result<Value, BoxError> {
    if micros == i64::MAX || micros == i64::MIN {
        return Ok(Value::Null);
    }
    micros
        .checked_add(EPOCH_OFFSET)
        .map(Value::Time)
        .ok_or_else(|| refuse("a timestamp past the range smeltr holds".to_string()))
}

/// A date's days since 2000, as midnight UTC. `infinity` and `-infinity` read as null, as for a timestamp.
fn date(days: i32) -> Result<Value, BoxError> {
    if days == i32::MAX || days == i32::MIN {
        return Ok(Value::Null);
    }
    i64::from(days)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|micros| micros.checked_add(EPOCH_OFFSET))
        .map(Value::Time)
        .ok_or_else(|| refuse("a date past the range smeltr holds".to_string()))
}

/// An array of `member`s, nested one [`Value::Array`] per dimension, outermost first. The binary form is the count
/// of dimensions, a has-null flag, the element type, each dimension's length and lower bound, then every element,
/// last dimension fastest, as its length (-1 for null) and its bytes.
fn array(member: &Type, raw: &[u8]) -> Result<Value, BoxError> {
    let mut rest = raw;
    let dimensions = word(&mut rest)?;
    if dimensions < 0 {
        return Err(refuse("an array of negative dimensions".to_string()));
    }
    word(&mut rest)?;
    word(&mut rest)?;
    let mut lengths = Vec::new();
    for _ in 0..dimensions {
        let length =
            usize::try_from(word(&mut rest)?).map_err(|_| refuse("an array of negative length".to_string()))?;
        lengths.push(length);
        word(&mut rest)?;
    }
    let count = lengths
        .iter()
        .try_fold(usize::from(dimensions > 0), |n, l| n.checked_mul(*l))
        .ok_or_else(|| refuse("an array of more elements than smeltr holds".to_string()))?;
    let mut items = Vec::new();
    // Every element takes at least its length's four bytes, so a count past the buffer stops at its end.
    for _ in 0..count {
        let item = match word(&mut rest)? {
            -1 => Value::Null,
            length => {
                let length =
                    usize::try_from(length).map_err(|_| refuse("an array element of negative length".to_string()))?;
                let (item, tail) = rest.split_at_checked(length).ok_or_else(short_array)?;
                rest = tail;
                decode(member, item)?
            }
        };
        items.push(item);
    }
    if !rest.is_empty() {
        return Err(refuse("an array longer than its header says".to_string()));
    }
    // An empty array reads as one, whatever dimensions it was sent with: PostgreSQL sends none, tokio-postgres one
    // of length 0.
    if items.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }
    Ok(nest(&lengths, &mut items.into_iter()))
}

fn short_array() -> BoxError {
    refuse("an array shorter than its header says".to_string())
}

/// The next big-endian i32 of an array's binary form.
fn word(rest: &mut &[u8]) -> Result<i32, BoxError> {
    let (head, tail) = rest.split_first_chunk::<4>().ok_or_else(short_array)?;
    *rest = tail;
    Ok(i32::from_be_bytes(*head))
}

fn nest(lengths: &[usize], items: &mut impl Iterator<Item = Value>) -> Value {
    match lengths.split_first() {
        None => Value::Array(Vec::new()),
        Some((length, [])) => Value::Array(items.take(*length).collect()),
        Some((length, inner)) => Value::Array((0..*length).map(|_| nest(inner, items)).collect()),
    }
}

fn interval(micros: i64, days: i32, months: i32) -> Result<Value, BoxError> {
    let days = i64::from(months) * DAYS_PER_MONTH + i64::from(days);
    days.checked_mul(MICROS_PER_DAY)
        .and_then(|d| d.checked_add(micros))
        .map(Value::Duration)
        .ok_or_else(|| refuse("an interval past the range smeltr holds".to_string()))
}

const NUMERIC_POSITIVE: u16 = 0x0000;
const NUMERIC_NEGATIVE: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_INFINITY: u16 = 0xD000;
const NUMERIC_NEGATIVE_INFINITY: u16 = 0xF000;

/// A numeric's exact decimal text, as PostgreSQL prints it. The binary form is base-10000 digits, the weight of the
/// first (its power of 10000), a sign, and the count of decimal digits after the point.
fn numeric_text(raw: &[u8]) -> Result<String, BoxError> {
    let short = || refuse("a numeric shorter than its header says".to_string());
    let word = |at: usize| {
        raw.get(at..at + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .ok_or_else(short)
    };
    let count = usize::from(word(0)?);
    let weight = word(2)? as i16;
    let sign = word(4)?;
    let scale = usize::from(word(6)?);
    let digits = (0..count).map(|i| word(8 + 2 * i)).collect::<Result<Vec<u16>, _>>()?;
    match sign {
        NUMERIC_POSITIVE | NUMERIC_NEGATIVE => {}
        NUMERIC_NAN => return Ok("NaN".to_string()),
        NUMERIC_INFINITY => return Ok("Infinity".to_string()),
        NUMERIC_NEGATIVE_INFINITY => return Ok("-Infinity".to_string()),
        _ => return Err(refuse(format!("a numeric with the unknown sign {sign:#06x}"))),
    }
    if digits.iter().any(|d| *d >= 10_000) {
        return Err(refuse("a numeric with a digit past 9999".to_string()));
    }
    let digit = |i: i32| {
        usize::try_from(i)
            .ok()
            .and_then(|i| digits.get(i))
            .copied()
            .unwrap_or(0)
    };
    let mut text = String::new();
    if sign == NUMERIC_NEGATIVE {
        text.push('-');
    }
    if weight < 0 {
        text.push('0');
    } else {
        write!(text, "{}", digit(0)).expect("writing to a String");
        for i in 1..=i32::from(weight) {
            write!(text, "{:04}", digit(i)).expect("writing to a String");
        }
    }
    if scale > 0 {
        text.push('.');
        let mut fraction = String::with_capacity(scale + 4);
        let mut i = i32::from(weight) + 1;
        while fraction.len() < scale {
            write!(fraction, "{:04}", digit(i)).expect("writing to a String");
            i += 1;
        }
        fraction.truncate(scale);
        text.push_str(&fraction);
    }
    Ok(text)
}

/// The binary numeric for decimal text: optional sign, digits, optionally a point and more digits; or `NaN`,
/// `Infinity`, `-Infinity`.
fn numeric_bytes(text: &str) -> Result<Vec<u8>, String> {
    let header = |count: usize, weight: i16, sign: u16, scale: u16| {
        let mut out = Vec::with_capacity(8 + 2 * count);
        out.extend_from_slice(&(count as u16).to_be_bytes());
        out.extend_from_slice(&weight.to_be_bytes());
        out.extend_from_slice(&sign.to_be_bytes());
        out.extend_from_slice(&scale.to_be_bytes());
        out
    };
    match text {
        "NaN" => return Ok(header(0, 0, NUMERIC_NAN, 0)),
        "Infinity" | "+Infinity" => return Ok(header(0, 0, NUMERIC_INFINITY, 0)),
        "-Infinity" => return Ok(header(0, 0, NUMERIC_NEGATIVE_INFINITY, 0)),
        _ => {}
    }
    let not_decimal = || format!("{text:?} is not a decimal number");
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let decimal = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if (whole.is_empty() && fraction.is_empty()) || !decimal(whole) || !decimal(fraction) {
        return Err(not_decimal());
    }
    // PostgreSQL's limit on digits after the point.
    let scale = u16::try_from(fraction.len())
        .ok()
        .filter(|s| *s <= 16_383)
        .ok_or_else(not_decimal)?;
    let whole = whole.trim_start_matches('0');
    let lead = (4 - whole.len() % 4) % 4;
    let trail = (4 - fraction.len() % 4) % 4;
    let padded: String = "0".repeat(lead) + whole + fraction + &"0".repeat(trail);
    let mut groups: Vec<u16> = padded
        .as_bytes()
        .chunks(4)
        .map(|c| {
            std::str::from_utf8(c)
                .expect("ASCII digits")
                .parse()
                .expect("four digits")
        })
        .collect();
    let mut weight = i32::try_from((lead + whole.len()) / 4).map_err(|_| not_decimal())? - 1;
    let first = groups.iter().position(|g| *g != 0).unwrap_or(groups.len());
    groups.drain(..first);
    weight -= first as i32;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        return Ok(header(0, 0, NUMERIC_POSITIVE, scale));
    }
    let weight = i16::try_from(weight).map_err(|_| not_decimal())?;
    let sign = if negative { NUMERIC_NEGATIVE } else { NUMERIC_POSITIVE };
    let mut out = header(groups.len(), weight, sign, scale);
    for g in groups {
        out.extend_from_slice(&g.to_be_bytes());
    }
    Ok(out)
}

fn uuid_text(raw: [u8; 16]) -> String {
    let mut text = String::with_capacity(36);
    for (i, b) in raw.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            text.push('-');
        }
        write!(text, "{b:02x}").expect("writing to a String");
    }
    text
}

fn uuid_bytes(text: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = text.bytes().filter(|b| *b != b'-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(out)
}

/// An inet or cidr as PostgreSQL prints it: the address, and its prefix length where it is not the whole address
/// or the value is a cidr.
fn inet_text(raw: &[u8]) -> Result<String, BoxError> {
    let (family, bits, cidr, address) = match raw {
        [family, bits, cidr, _, address @ ..] => (*family, *bits, *cidr != 0, address),
        _ => return Err(refuse("an inet shorter than its header".to_string())),
    };
    let (text, full) = match (family, address.len()) {
        // PostgreSQL's own family numbers: 2 for IPv4, 3 for IPv6.
        (2, 4) => (
            Ipv4Addr::from(<[u8; 4]>::try_from(address).expect("4 bytes")).to_string(),
            32,
        ),
        (3, 16) => (
            Ipv6Addr::from(<[u8; 16]>::try_from(address).expect("16 bytes")).to_string(),
            128,
        ),
        _ => return Err(refuse(format!("an inet of the unknown family {family}"))),
    };
    Ok(if cidr || bits != full {
        format!("{text}/{bits}")
    } else {
        text
    })
}

/// The binary form of an inet or cidr as `inet_text` writes it: an address, then `/bits` when the mask is not
/// the whole address.
fn inet_bytes(text: &str, cidr: bool) -> Option<Vec<u8>> {
    let (address, bits) = match text.split_once('/') {
        Some((address, bits)) => (address, Some(bits.parse::<u8>().ok()?)),
        None => (text, None),
    };
    let (family, full, address) = match address.parse::<IpAddr>().ok()? {
        IpAddr::V4(a) => (2, 32, a.octets().to_vec()),
        IpAddr::V6(a) => (3, 128, a.octets().to_vec()),
    };
    let bits = bits.unwrap_or(full);
    (bits <= full).then(|| [&[family, bits, u8::from(cidr), address.len() as u8][..], &address].concat())
}

/// One value bound to placeholder `$number`, encoded as whatever type PostgreSQL inferred for it.
#[derive(Debug)]
struct Param<'a> {
    number: usize,
    value: &'a Value,
}

impl ToSql for Param<'_> {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        if *self.value == Value::Null {
            return Ok(IsNull::Yes);
        }
        out.extend_from_slice(&encode(self.number, ty, self.value)?);
        Ok(IsNull::No)
    }

    /// Every type: a value that cannot be the placeholder's fails in `to_sql`, naming both.
    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// `value` in the binary form of `ty`, for placeholder `$number`. Never called with `Value::Null`.
fn encode(number: usize, ty: &Type, value: &Value) -> Result<Vec<u8>, BoxError> {
    let cannot = || {
        refuse(format!(
            "${number} is a {}, and {} cannot be one",
            ty.name(),
            what(value)
        ))
    };
    match ty.kind() {
        Kind::Array(_) => {
            let Value::Array(items) = value else {
                return Err(cannot());
            };
            let items: Vec<Param> = items.iter().map(|value| Param { number, value }).collect();
            let mut out = BytesMut::new();
            items.to_sql(ty, &mut out)?;
            return Ok(out.to_vec());
        }
        Kind::Domain(base) => return encode(number, base, value),
        Kind::Enum(_) => {
            return match value {
                Value::Str(s) => Ok(s.as_bytes().to_vec()),
                _ => Err(cannot()),
            };
        }
        _ => {}
    }
    let int = |v: &Value| match v {
        Value::Int(i) => Some(*i),
        _ => None,
    };
    let bytes = if *ty == Type::BOOL {
        match value {
            Value::Bool(b) => vec![u8::from(*b)],
            _ => return Err(cannot()),
        }
    } else if *ty == Type::INT2 {
        i16::try_from(int(value).ok_or_else(cannot)?)
            .map_err(|_| cannot())?
            .to_be_bytes()
            .to_vec()
    } else if *ty == Type::INT4 {
        i32::try_from(int(value).ok_or_else(cannot)?)
            .map_err(|_| cannot())?
            .to_be_bytes()
            .to_vec()
    } else if *ty == Type::INT8 {
        int(value).ok_or_else(cannot)?.to_be_bytes().to_vec()
    } else if *ty == Type::OID {
        u32::try_from(int(value).ok_or_else(cannot)?)
            .map_err(|_| cannot())?
            .to_be_bytes()
            .to_vec()
    } else if *ty == Type::FLOAT4 {
        match value {
            Value::Float(f) => (*f as f32).to_be_bytes().to_vec(),
            Value::Int(i) => (*i as f32).to_be_bytes().to_vec(),
            _ => return Err(cannot()),
        }
    } else if *ty == Type::FLOAT8 {
        match value {
            Value::Float(f) => f.to_be_bytes().to_vec(),
            Value::Int(i) => (*i as f64).to_be_bytes().to_vec(),
            _ => return Err(cannot()),
        }
    } else if *ty == Type::NUMERIC {
        let text = match value {
            Value::Int(i) => i.to_string(),
            Value::Float(f) if f.is_nan() => "NaN".to_string(),
            Value::Float(f) if f.is_infinite() => (if *f > 0.0 { "Infinity" } else { "-Infinity" }).to_string(),
            // A float's Display is its shortest exact decimal, never an exponent.
            Value::Float(f) => f.to_string(),
            Value::Str(s) => s.clone(),
            _ => return Err(cannot()),
        };
        numeric_bytes(&text).map_err(|e| refuse(format!("${number} is a numeric, and {e}")))?
    } else if text_like(ty) {
        match value {
            Value::Str(s) => s.as_bytes().to_vec(),
            _ => return Err(cannot()),
        }
    } else if *ty == Type::TIMESTAMP || *ty == Type::TIMESTAMPTZ {
        match value {
            Value::Time(t) => t.checked_sub(EPOCH_OFFSET).ok_or_else(cannot)?.to_be_bytes().to_vec(),
            _ => return Err(cannot()),
        }
    } else if *ty == Type::DATE {
        match value {
            // The day the time falls on, in UTC.
            Value::Time(t) => {
                let days = t
                    .checked_sub(EPOCH_OFFSET)
                    .ok_or_else(cannot)?
                    .div_euclid(MICROS_PER_DAY);
                i32::try_from(days).map_err(|_| cannot())?.to_be_bytes().to_vec()
            }
            _ => return Err(cannot()),
        }
    } else if *ty == Type::TIME {
        match value {
            Value::Duration(d) => d.to_be_bytes().to_vec(),
            _ => return Err(cannot()),
        }
    } else if *ty == Type::INTERVAL {
        match value {
            Value::Duration(d) => [&d.to_be_bytes()[..], &0i32.to_be_bytes(), &0i32.to_be_bytes()].concat(),
            _ => return Err(cannot()),
        }
    } else if *ty == Type::UUID {
        match value {
            Value::Str(s) => uuid_bytes(s).ok_or_else(cannot)?.to_vec(),
            _ => return Err(cannot()),
        }
    } else if *ty == Type::INET || *ty == Type::CIDR {
        match value {
            Value::Str(s) => inet_bytes(s, *ty == Type::CIDR).ok_or_else(cannot)?,
            _ => return Err(cannot()),
        }
    } else if *ty == Type::BYTEA {
        match value {
            Value::Bytes(b) => b.clone(),
            _ => return Err(cannot()),
        }
    } else if *ty == Type::JSON || *ty == Type::JSONB {
        let mut text = Vec::new();
        match value {
            Value::Json(j) => crate::io::values::json_text(j, &mut text),
            _ => return Err(cannot()),
        }
        // jsonb's binary form is a version byte, 1, then the text.
        if *ty == Type::JSONB {
            [&[1u8][..], &text].concat()
        } else {
            text
        }
    } else {
        return Err(cannot());
    };
    Ok(bytes)
}

/// How a value reads in a message about binding it.
fn what(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => format!("the bool {b}"),
        Value::Int(i) => format!("the integer {i}"),
        Value::Float(f) => format!("the float {f}"),
        Value::Str(_) => "a string".to_string(),
        Value::Bytes(_) => "bytes".to_string(),
        Value::Time(_) => "a time".to_string(),
        Value::Duration(_) => "a duration".to_string(),
        Value::Json(_) => "JSON".to_string(),
        Value::Array(_) => "an array".to_string(),
    }
}

/// One sequential scan in a plan.
#[derive(Debug, PartialEq)]
struct Scan {
    /// Present when the plan was explained with VERBOSE.
    schema: Option<String>,
    name: String,
    rows: u64,
}

/// The text at `key` in the plan node `node`.
fn text_at<'p>(node: &'p [u8], key: &str) -> Option<&'p str> {
    match variant::read(variant::member(node, key)?) {
        variant::Value::Str(s) => Some(s),
        _ => None,
    }
}

/// The number at `key` in the plan node `node`.
fn number_at(node: &[u8], key: &str) -> Option<f64> {
    match variant::read(variant::member(node, key)?) {
        variant::Value::I64(i) => Some(i as f64),
        variant::Value::F64(f) => Some(f),
        _ => None,
    }
}

/// The elements of the list `list`, none where it is no list.
fn items(list: Option<&[u8]>) -> impl Iterator<Item = &[u8]> {
    list.and_then(|l| match variant::read(l) {
        variant::Value::Array(items) => Some(items),
        _ => None,
    })
    .into_iter()
    .flatten()
}

/// Every sequential scan in `EXPLAIN (FORMAT JSON)` output, at any depth, with the planner's row estimate.
fn scans(explained: &[u8]) -> Vec<Scan> {
    fn walk(node: &[u8], found: &mut Vec<Scan>) {
        if text_at(node, "Node Type") == Some("Seq Scan")
            && let Some(name) = text_at(node, "Relation Name")
        {
            found.push(Scan {
                schema: text_at(node, "Schema").map(str::to_string),
                name: name.to_string(),
                rows: number_at(node, "Plan Rows").map_or(0, |r| r as u64),
            });
        }
        for child in items(variant::member(node, "Plans")) {
            walk(child, found);
        }
    }
    let mut found = Vec::new();
    for statement in items(Some(explained)) {
        if let Some(plan) = variant::member(statement, "Plan") {
            walk(plan, &mut found);
        }
    }
    found
}

/// Each index `EXPLAIN (FORMAT JSON)` output reads through, by name, in plan order, once.
fn indexes(explained: &[u8]) -> Vec<String> {
    fn walk(node: &[u8], found: &mut Vec<String>) {
        let reads_index = matches!(
            text_at(node, "Node Type"),
            Some("Index Scan" | "Index Only Scan" | "Bitmap Index Scan")
        );
        if reads_index
            && let Some(name) = text_at(node, "Index Name")
            && !found.iter().any(|f| f == name)
        {
            found.push(name.to_string());
        }
        for child in items(variant::member(node, "Plans")) {
            walk(child, found);
        }
    }
    let mut found = Vec::new();
    for statement in items(Some(explained)) {
        if let Some(plan) = variant::member(statement, "Plan") {
            walk(plan, &mut found);
        }
    }
    found
}

/// The planner's estimate of the rows the statement returns: its top node's.
fn returned(explained: &[u8]) -> u64 {
    (items(Some(explained)).next())
        .and_then(|statement| variant::member(statement, "Plan"))
        .and_then(|plan| number_at(plan, "Plan Rows"))
        .map_or(0, |r| r as u64)
}

/// The largest scan past [`FULL_SCAN_ROWS`], or `Indexed` through `indexes`, returning about `rows`.
fn verdict(scans: &[Scan], indexes: Vec<String>, rows: u64) -> Verdict {
    match scans.iter().filter(|s| s.rows > FULL_SCAN_ROWS).max_by_key(|s| s.rows) {
        Some(scan) => Verdict::FullScan {
            relation: match &scan.schema {
                Some(schema) => format!("{schema}.{}", scan.name),
                None => scan.name.clone(),
            },
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

    fn numeric(text: &str) -> String {
        numeric_text(&numeric_bytes(text).unwrap()).unwrap()
    }

    #[test]
    fn numeric_reads_postgresql_binary() {
        // 12345.678: digits 1, 2345, 6780; weight 1; scale 3.
        let raw = [0, 3, 0, 1, 0, 0, 0, 3, 0, 1, 0x09, 0x29, 0x1A, 0x7C];
        assert_eq!(numeric_text(&raw).unwrap(), "12345.678");
        // -0.0001: one digit 1 at weight -1; scale 4.
        let raw = [0, 1, 0xFF, 0xFF, 0x40, 0, 0, 4, 0, 1];
        assert_eq!(numeric_text(&raw).unwrap(), "-0.0001");
        // 0.00 has no digits and keeps its scale.
        assert_eq!(numeric_text(&[0, 0, 0, 0, 0, 0, 0, 2]).unwrap(), "0.00");
        assert_eq!(numeric_text(&[0, 0, 0, 0, 0xC0, 0, 0, 0]).unwrap(), "NaN");
        assert_eq!(numeric_text(&[0, 0, 0, 0, 0xD0, 0, 0, 0]).unwrap(), "Infinity");
        assert_eq!(numeric_text(&[0, 0, 0, 0, 0xF0, 0, 0, 0]).unwrap(), "-Infinity");
        // 10000000: digit 1000 at weight 1, digit 0 trimmed.
        assert_eq!(numeric_text(&[0, 1, 0, 1, 0, 0, 0, 0, 0x03, 0xE8]).unwrap(), "10000000");
        assert!(numeric_text(&[0, 2, 0, 0, 0, 0, 0, 0, 0, 1]).is_err());
    }

    #[test]
    fn numeric_round_trips_through_its_binary_form() {
        for text in [
            "0",
            "1",
            "-1",
            "12345.678",
            "-0.0001",
            "0.00",
            "10000",
            "99999999.99990",
            "-123456789012345678901234567890.5",
            "NaN",
            "Infinity",
            "-Infinity",
        ] {
            assert_eq!(numeric(text), text);
        }
        assert_eq!(numeric("007.50"), "7.50");
        assert_eq!(numeric("+3"), "3");
        assert_eq!(numeric(".5"), "0.5");
        assert_eq!(
            numeric_bytes("-0.0001").unwrap(),
            [0, 1, 0xFF, 0xFF, 0x40, 0, 0, 4, 0, 1]
        );
        for bad in ["", "-", "1e5", "1.2.3", "abc", " 1"] {
            assert!(numeric_bytes(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn times_count_from_the_unix_epoch() {
        assert_eq!(timestamp(0).unwrap(), Value::Time(EPOCH_OFFSET));
        assert_eq!(timestamp(-EPOCH_OFFSET).unwrap(), Value::Time(0));
        assert_eq!(timestamp(i64::MAX).unwrap(), Value::Null);
        assert_eq!(timestamp(i64::MIN).unwrap(), Value::Null);
        // 2026-10-10 is 9779 days after 2000-01-01.
        assert_eq!(date(9779).unwrap(), Value::Time(1_791_590_400_000_000));
        assert_eq!(date(-10_957).unwrap(), Value::Time(0));
        assert_eq!(date(i32::MAX).unwrap(), Value::Null);
        // 1 month, 2 days, 3 seconds.
        assert_eq!(
            interval(3_000_000, 2, 1).unwrap(),
            Value::Duration(32 * MICROS_PER_DAY + 3_000_000)
        );
        assert_eq!(interval(0, -1, 0).unwrap(), Value::Duration(-MICROS_PER_DAY));
        assert!(interval(i64::MAX, 1, 0).is_err());
    }

    #[test]
    fn times_bind_as_postgresql_counts_them() {
        let encoded = encode(1, &Type::TIMESTAMPTZ, &Value::Time(EPOCH_OFFSET + 5)).unwrap();
        assert_eq!(encoded, 5i64.to_be_bytes());
        let noon = 1_791_590_400_000_000 + MICROS_PER_DAY / 2;
        assert_eq!(
            encode(1, &Type::DATE, &Value::Time(noon)).unwrap(),
            9779i32.to_be_bytes()
        );
        let before_1970 = Value::Time(-1);
        assert_eq!(
            encode(1, &Type::DATE, &before_1970).unwrap(),
            (-10_958i32).to_be_bytes()
        );
    }

    #[test]
    fn values_bind_only_as_a_type_that_holds_them() {
        assert_eq!(encode(1, &Type::INT4, &Value::Int(-7)).unwrap(), (-7i32).to_be_bytes());
        let over = encode(2, &Type::INT4, &Value::Int(3_000_000_000))
            .unwrap_err()
            .to_string();
        assert_eq!(over, "$2 is a int4, and the integer 3000000000 cannot be one");
        assert!(encode(1, &Type::INT2, &Value::Int(40_000)).is_err());
        assert!(encode(1, &Type::OID, &Value::Int(-1)).is_err());
        let text = encode(3, &Type::INT8, &Value::Str("1".into())).unwrap_err().to_string();
        assert_eq!(text, "$3 is a int8, and a string cannot be one");
        assert_eq!(
            encode(1, &Type::JSONB, &crate::io::values::json(b"[1]").unwrap()).unwrap(),
            b"\x01[1]"
        );
        let id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        let bytes = encode(1, &Type::UUID, &Value::Str(id.into())).unwrap();
        assert_eq!(uuid_text(bytes.try_into().unwrap()), id);
        assert!(encode(1, &Type::NUMERIC, &Value::Str("x".into())).is_err());
        assert_eq!(
            numeric_text(&encode(1, &Type::NUMERIC, &Value::Float(0.1)).unwrap()).unwrap(),
            "0.1"
        );
    }

    #[test]
    fn every_value_a_position_can_hold_binds_again() {
        for (ty, value) in [
            (Type::INET, Value::Str("10.1.2.3".into())),
            (Type::INET, Value::Str("10.1.2.3/8".into())),
            (Type::INET, Value::Str("::1".into())),
            (Type::CIDR, Value::Str("10.0.0.0/8".into())),
            (Type::BYTEA, Value::Bytes(vec![0x00, 0xff, 0x10])),
            (
                Type::INT4_ARRAY,
                Value::Array(vec![Value::Int(1), Value::Null, Value::Int(3)]),
            ),
            (Type::TEXT_ARRAY, Value::Array(Vec::new())),
        ] {
            let bytes = encode(1, &ty, &value).unwrap_or_else(|e| panic!("{ty}: {e}"));
            assert_eq!(decode(&ty, &bytes).unwrap(), value, "{ty}");
        }
        assert!(encode(1, &Type::INET, &Value::Str("10.1.2.3/33".into())).is_err());
        assert!(encode(1, &Type::BYTEA, &Value::Str("\\x00".into())).is_err());
    }

    #[test]
    fn cells_decode_by_their_type() {
        assert_eq!(decode(&Type::INT2, &(-3i16).to_be_bytes()).unwrap(), Value::Int(-3));
        assert_eq!(
            decode(&Type::OID, &u32::MAX.to_be_bytes()).unwrap(),
            Value::Int(u32::MAX.into())
        );
        assert_eq!(
            decode(&Type::BYTEA, &[0xde, 0xad]).unwrap(),
            Value::Bytes(vec![0xde, 0xad])
        );
        assert_eq!(
            decode(&Type::JSONB, b"\x01{\"a\":1}").unwrap(),
            crate::io::values::json(br#"{"a":1}"#).unwrap()
        );
        assert_eq!(
            decode(&Type::INET, &[2, 32, 0, 4, 10, 0, 0, 1]).unwrap(),
            Value::Str("10.0.0.1".into())
        );
        assert_eq!(
            decode(&Type::CIDR, &[2, 8, 1, 4, 10, 0, 0, 0]).unwrap(),
            Value::Str("10.0.0.0/8".into())
        );
        assert!(
            decode(&Type::POINT, &[0; 16])
                .unwrap_err()
                .to_string()
                .contains("point")
        );
        // int4[] holding 1 and null: one dimension, a null flag, the element type, then each length and value.
        let mut array = Vec::new();
        for word in [1i32, 1, 23, 2, 1, 4, 1, -1] {
            array.extend_from_slice(&word.to_be_bytes());
        }
        assert_eq!(
            decode(&Type::INT4_ARRAY, &array).unwrap(),
            Value::Array(vec![Value::Int(1), Value::Null])
        );
    }

    fn words(words: &[i32]) -> Vec<u8> {
        words.iter().flat_map(|w| w.to_be_bytes()).collect()
    }

    #[test]
    fn arrays_nest_one_array_per_dimension() {
        // int4[2][2]: two dimensions, no null, the element type, each length and lower bound, then 1, 2, 3, 4.
        let square = words(&[2, 0, 23, 2, 1, 2, 1, 4, 1, 4, 2, 4, 3, 4, 4]);
        assert_eq!(
            decode(&Type::INT4_ARRAY, &square).unwrap(),
            Value::Array(vec![
                Value::Array(vec![Value::Int(1), Value::Int(2)]),
                Value::Array(vec![Value::Int(3), Value::Int(4)]),
            ])
        );
        let holed = words(&[2, 1, 23, 1, 1, 2, 1, -1, 4, 7]);
        assert_eq!(
            decode(&Type::INT4_ARRAY, &holed).unwrap(),
            Value::Array(vec![Value::Array(vec![Value::Null, Value::Int(7)])])
        );
        for empty in [words(&[0, 0, 23]), words(&[1, 0, 23, 0, 1])] {
            assert_eq!(decode(&Type::INT4_ARRAY, &empty).unwrap(), Value::Array(Vec::new()));
        }
    }

    #[test]
    fn malformed_arrays_are_refused() {
        let square = words(&[2, 0, 23, 2, 1, 2, 1, 4, 1, 4, 2, 4, 3, 4, 4]);
        for cut in 0..square.len() {
            assert!(decode(&Type::INT4_ARRAY, &square[..cut]).is_err(), "cut at {cut}");
        }
        assert!(decode(&Type::INT4_ARRAY, &[square.clone(), vec![0]].concat()).is_err());
        assert!(decode(&Type::INT4_ARRAY, &words(&[-1, 0, 23])).is_err());
        assert!(decode(&Type::INT4_ARRAY, &words(&[1, 0, 23, -1, 1])).is_err());
        assert!(decode(&Type::INT4_ARRAY, &words(&[1, 0, 23, 1, 1, -2])).is_err());
        assert!(decode(&Type::INT4_ARRAY, &words(&[1, 0, 23, 2, 1, 4, 1])).is_err());
        assert!(decode(&Type::INT4_ARRAY, &words(&[1, 0, 23, i32::MAX, 1])).is_err());
    }

    #[test]
    fn a_date_past_the_range_held_is_refused() {
        assert!(decode(&Type::DATE, &(i32::MAX - 1).to_be_bytes()).is_err());
        assert!(decode(&Type::DATE, &(i32::MIN + 1).to_be_bytes()).is_err());
        assert_eq!(decode(&Type::DATE, &i32::MAX.to_be_bytes()).unwrap(), Value::Null);
        assert_eq!(
            decode(&Type::DATE, &0i32.to_be_bytes()).unwrap(),
            Value::Time(EPOCH_OFFSET)
        );
    }

    #[test]
    fn the_largest_seq_scan_past_the_threshold_is_the_verdict() {
        let explained = plan(serde_json::json!([{
            "Plan": {
                "Node Type": "Limit",
                "Plan Rows": 100,
                "Plans": [{
                    "Node Type": "Nested Loop",
                    "Plan Rows": 100,
                    "Plans": [
                        {"Node Type": "Seq Scan", "Relation Name": "small", "Schema": "public", "Plan Rows": 40},
                        {"Node Type": "Index Scan", "Relation Name": "huge", "Schema": "public", "Plan Rows": 9e6},
                        {"Node Type": "Seq Scan", "Relation Name": "events", "Schema": "app", "Plan Rows": 250000},
                        {"Node Type": "Seq Scan", "Relation Name": "logs", "Schema": "app", "Plan Rows": 120000}
                    ]
                }]
            }
        }]));
        let found = scans(&explained);
        assert_eq!(found.len(), 3);
        assert_eq!(
            verdict(&found, Vec::new(), 100),
            Verdict::FullScan {
                relation: "app.events".into(),
                rows: 250_000
            }
        );
        let indexed = Verdict::Indexed {
            indexes: Vec::new(),
            rows: 100,
        };
        assert_eq!(verdict(&found[..1], Vec::new(), 100), indexed);
        assert_eq!(verdict(&[], Vec::new(), 100), indexed);
    }

    #[test]
    fn an_indexed_verdict_names_its_indexes_and_the_rows_the_top_node_returns() {
        let explained = plan(serde_json::json!([{
            "Plan": {
                "Node Type": "Limit",
                "Plan Rows": 20,
                "Plans": [{
                    "Node Type": "Nested Loop",
                    "Plan Rows": 5000,
                    "Plans": [
                        {"Node Type": "Index Scan", "Index Name": "orders_pkey", "Relation Name": "orders",
                         "Schema": "app", "Plan Rows": 5000},
                        {"Node Type": "Bitmap Heap Scan", "Relation Name": "lines", "Schema": "app", "Plan Rows": 3,
                         "Plans": [{"Node Type": "Bitmap Index Scan", "Index Name": "lines_order", "Plan Rows": 3}]},
                        {"Node Type": "Index Only Scan", "Index Name": "orders_pkey", "Relation Name": "orders",
                         "Schema": "app", "Plan Rows": 1}
                    ]
                }]
            }
        }]));
        assert!(scans(&explained).is_empty());
        assert_eq!(
            verdict(&scans(&explained), indexes(&explained), returned(&explained)),
            Verdict::Indexed {
                indexes: vec!["orders_pkey".into(), "lines_order".into()],
                rows: 20
            }
        );
    }

    #[test]
    fn types_suggest_what_a_record_declares() {
        assert_eq!(suggested(&Type::INT4), Some(DataType::I64));
        assert_eq!(suggested(&Type::NUMERIC), Some(DataType::Str));
        assert_eq!(suggested(&Type::TIMESTAMP), Some(DataType::DateTime));
        assert_eq!(suggested(&Type::INTERVAL), Some(DataType::Duration));
        assert_eq!(suggested(&Type::JSONB), Some(DataType::Variant));
        assert_eq!(
            suggested(&Type::TEXT_ARRAY),
            Some(DataType::Array(Box::new(DataType::Str)))
        );
        assert_eq!(
            suggested(&Type::INT4_ARRAY),
            Some(DataType::Array(Box::new(DataType::I64)))
        );
        assert_eq!(suggested(&Type::POINT_ARRAY), Some(DataType::Str));
        assert_eq!(
            suggested(&Type::BYTEA_ARRAY),
            Some(DataType::Array(Box::new(DataType::Bytes)))
        );
        assert_eq!(suggested(&Type::BYTEA), Some(DataType::Bytes));
        assert_eq!(suggested(&Type::POINT), Some(DataType::Str));
    }
}
