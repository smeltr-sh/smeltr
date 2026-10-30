//! Identity columns: a table's columns whose values the writer generates, each on its column's grid
//! `start + k·step`, beyond the column's `delta.identity.highWaterMark`, never one twice.
//!
//! Values are reserved ahead in the log. A commit raises a column's high-water mark by a block, and the values up to
//! it are this writer's alone, since every writer gives out only values beyond the mark it read. Each row admitted
//! sets a value aside, so a row that would take one past a long's range is refused alone; a file's rows take theirs
//! as they are written.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant E as egress: admit
//!   participant W as writer thread
//!   participant S as _delta_log
//!   E->>E: set a value aside for the row
//!   E->>S: none left: a commit of its own raises the mark a block
//!   W->>W: each row written takes a value set aside
//!   W->>S: a data commit raises the mark a block when what is left runs low
//! ```

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType as Arrow, Field, Schema as ArrowSchema};
use serde_json::{Value, json};

use super::log::{Log, Metadata, Snapshot, commit_name, line, now_ms};
use super::schema::{self, StructField, StructType};
use crate::io::files::{CommitError, Retry};
use crate::io::parquet::{Columns, Computed};

/// How many values a commit reserves for a column.
pub const BLOCK: u64 = 1 << 20;

/// How few values left unclaimed make a commit reserve a block more: a data commit reserves before the rows run out.
const LOW: u64 = BLOCK / 2;

const START: &str = "delta.identity.start";
const STEP: &str = "delta.identity.step";
const HIGH_WATER_MARK: &str = "delta.identity.highWaterMark";
const ALLOW_EXPLICIT_INSERT: &str = "delta.identity.allowExplicitInsert";

/// One identity column, as its metadata defines it.
#[derive(Clone, Debug, PartialEq)]
pub struct Identity {
    pub name: String,
    pub start: i64,
    pub step: i64,
    /// Whether a writer may write the column's values itself.
    pub explicit: bool,
}

impl Identity {
    /// `field`'s identity; None when it is no identity column. Err when its metadata does not read as one.
    pub fn of(field: &StructField) -> Result<Option<Identity>, String> {
        if !field.metadata.keys().any(|k| k.starts_with("delta.identity.")) {
            return Ok(None);
        }
        let long = |key: &str| {
            (field.metadata.get(key).and_then(Value::as_i64))
                .ok_or_else(|| format!("the table's identity column {} has no {key}", field.name))
        };
        let step = long(STEP)?;
        if step == 0 {
            return Err(format!("the table's identity column {} steps by 0", field.name));
        }
        if field.data_type != schema::Type::Primitive("long".into()) {
            return Err(format!("the table's identity column {} is no long", field.name));
        }
        let explicit = match field.metadata.get(ALLOW_EXPLICIT_INSERT) {
            None => false,
            Some(v) => v.as_bool().ok_or_else(|| {
                format!(
                    "the table's identity column {} has {ALLOW_EXPLICIT_INSERT} {v}",
                    field.name
                )
            })?,
        };
        mark(field)?;
        Ok(Some(Identity {
            name: field.name.clone(),
            start: long(START)?,
            step,
            explicit,
        }))
    }

    /// Up to `n` values of the grid beyond `mark`, or from `start` when there is none. None when none is left within a
    /// long.
    fn block(&self, mark: Option<i64>, n: u64) -> Option<Block> {
        let (start, step) = (self.start as i128, self.step as i128);
        // The first k whose value is past `mark` in the step's direction: (mark - start) / step, floored, and one more.
        let first = mark.map_or(0, |m| (floor_div(m as i128 - start, step) + 1).max(0));
        let bound = if step > 0 { i64::MAX } else { i64::MIN } as i128;
        let last = floor_div(bound - start, step).min(first + n as i128 - 1);
        (last >= first).then(|| Block {
            next: start + first * step,
            left: (last - first + 1) as u64,
        })
    }
}

fn floor_div(a: i128, b: i128) -> i128 {
    let q = a / b;
    if a % b != 0 && (a < 0) != (b < 0) { q - 1 } else { q }
}

/// `field`'s high-water mark; None when it has given out no value yet.
fn mark(field: &StructField) -> Result<Option<i64>, String> {
    match field.metadata.get(HIGH_WATER_MARK) {
        None => Ok(None),
        Some(v) => (v.as_i64().map(Some))
            .ok_or_else(|| format!("the table's identity column {} has {HIGH_WATER_MARK} {v}", field.name)),
    }
}

/// The table's identity columns, each a top-level `long`. Err names one whose metadata does not read.
pub fn columns(schema: &StructType) -> Result<Vec<Identity>, String> {
    schema
        .fields
        .iter()
        .filter_map(|f| Identity::of(f).transpose())
        .collect()
}

/// Values of a column's grid reserved in the log: `next`, and each a step on from it, `left` of them.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Block {
    next: i128,
    left: u64,
}

/// What one column has reserved and not yet given out.
#[derive(Debug, Default)]
struct Reserve {
    blocks: VecDeque<Block>,
    /// Values in `blocks` no admitted row has set aside.
    free: u64,
}

/// The identity columns this writer generates, the values each has reserved, and how a row admitted reserves more.
#[derive(Debug)]
pub struct Sequences {
    columns: Vec<Identity>,
    reserves: Mutex<Vec<Reserve>>,
    /// Where a row admitted when none is left reserves a block.
    log: Log,
}

/// The values of `columns` taken for a commit: the table's metadata with their marks raised, and the blocks raised.
pub struct Raised {
    pub metadata: Metadata,
    blocks: Vec<(usize, Block)>,
}

impl Sequences {
    /// The sequences of `columns`, none reserved yet. A row admitted when none is left reserves a block in `log`,
    /// trying again until the log's stall threshold.
    pub fn new(columns: Vec<Identity>, log: Log) -> Sequences {
        Sequences {
            reserves: Mutex::new(columns.iter().map(|_| Reserve::default()).collect()),
            columns,
            log,
        }
    }

    /// Each column as a file carries it, from `table`'s physical names and ids.
    pub fn computed(self: &Arc<Self>, table: &StructType) -> Vec<Arc<dyn Computed>> {
        let names = ArrowSchema::new(
            (self.columns.iter())
                .map(|c| Field::new(&c.name, Arrow::Int64, true))
                .collect::<Vec<_>>(),
        );
        let written = schema::physical(&names, table);
        (written.fields().iter().enumerate())
            .map(|(column, field)| {
                Arc::new(Generated {
                    sequences: self.clone(),
                    column,
                    field: field.clone(),
                }) as Arc<dyn Computed>
            })
            .collect()
    }

    /// `metadata` with a block more reserved for each column with fewer than `LOW` values unclaimed. None when none
    /// has, or every one that has is out of values. Err when the table's identity columns are no longer these.
    pub fn raised(&self, metadata: &Metadata) -> Result<Option<Raised>, String> {
        let low: Vec<usize> = {
            let reserves = self.reserves.lock().unwrap_or_else(PoisonError::into_inner);
            (0..self.columns.len()).filter(|&i| reserves[i].free < LOW).collect()
        };
        if low.is_empty() {
            return Ok(None);
        }
        let mut schema = StructType::parse(&metadata.schema_string)?;
        let mut blocks = Vec::new();
        for i in low {
            let column = &self.columns[i];
            let changed = || format!("another writer changed the identity column {}", column.name);
            let field = (schema.fields.iter_mut())
                .find(|f| f.name == column.name)
                .ok_or_else(changed)?;
            if Identity::of(field)?.as_ref() != Some(column) {
                return Err(changed());
            }
            if let Some(block) = column.block(mark(field)?, BLOCK) {
                let last = block.next + (block.left as i128 - 1) * column.step as i128;
                field.metadata.insert(HIGH_WATER_MARK.into(), json!(last as i64));
                blocks.push((i, block));
            }
        }
        if blocks.is_empty() {
            return Ok(None);
        }
        let mut metadata = metadata.clone();
        metadata.schema_string = schema.to_json();
        Ok(Some(Raised { metadata, blocks }))
    }

    /// Takes the blocks of `raised`, committed: their values are this writer's to give out.
    pub fn reserved(&self, raised: Raised) {
        let mut reserves = self.reserves.lock().unwrap_or_else(PoisonError::into_inner);
        for (i, block) in raised.blocks {
            reserves[i].free += block.left;
            reserves[i].blocks.push_back(block);
        }
    }

    /// Sets a value of column `i` aside for a row, reserving a block first when none is left. Err when the column has
    /// no value left within a long, another writer changed the table's identity columns, or a block could not be
    /// reserved before the destination stalls.
    fn claim(&self, i: usize) -> Result<(), String> {
        if self.take_free(i) {
            return Ok(());
        }
        self.reserve()?;
        if self.take_free(i) {
            return Ok(());
        }
        Err(format!(
            "the identity column {} has no value left within a long beyond its high-water mark",
            self.columns[i].name
        ))
    }

    fn take_free(&self, i: usize) -> bool {
        let mut reserves = self.reserves.lock().unwrap_or_else(PoisonError::into_inner);
        let free = &mut reserves[i].free;
        *free > 0 && {
            *free -= 1;
            true
        }
    }

    /// Reserves a block for each column low on values, in a commit of its own on the table as the log has it now.
    fn reserve(&self) -> Result<(), String> {
        let mut retry = Retry::new(Instant::now(), self.log.stall_after());
        loop {
            // Rare enough to read the log whole, from its last checkpoint.
            let error = match Snapshot::load(&self.log, "") {
                Err(why) => CommitError::Transient(why),
                Ok(None) => CommitError::Transient("the table's log has no commit".to_string()),
                Ok(Some(snapshot)) => {
                    let Some(raised) = self.raised(&snapshot.metadata)? else {
                        return Ok(());
                    };
                    match self.commit(snapshot.version + 1, raised) {
                        Ok(()) => return Ok(()),
                        Err(e) => e,
                    }
                }
            };
            match retry.next(&error, Instant::now()) {
                Some(wait) => std::thread::sleep(wait),
                None => {
                    let why = match error {
                        CommitError::Conflict => "other writers committed first, every time".to_string(),
                        CommitError::Transient(why) | CommitError::Unknown(why) => why,
                    };
                    return Err(format!("no identity values could be reserved: {why}"));
                }
            }
        }
    }

    fn commit(&self, version: i64, raised: Raised) -> Result<(), CommitError> {
        let body = [
            line(
                "commitInfo",
                &json!({"timestamp": now_ms(), "operation": "CHANGE COLUMN", "engineInfo": "smeltr"}),
            ),
            line("metaData", &raised.metadata),
        ]
        .join("\n");
        match self.log.create(&commit_name(version), body.into_bytes()) {
            Ok(true) => {
                self.reserved(raised);
                Ok(())
            }
            Ok(false) => Err(CommitError::Conflict),
            // A lost commit that landed reserved values no row takes: the next reads its mark and goes beyond it.
            Err(why) => Err(CommitError::Unknown(why)),
        }
    }

    /// The next `rows` values of column `i`, each set aside by a row admitted.
    fn take(&self, i: usize, rows: usize) -> Vec<i64> {
        let step = self.columns[i].step as i128;
        let mut reserves = self.reserves.lock().unwrap_or_else(PoisonError::into_inner);
        let reserve = &mut reserves[i];
        let mut values = Vec::with_capacity(rows);
        while values.len() < rows {
            let block = (reserve.blocks.front_mut()).expect("a value set aside for each row admitted");
            let n = (block.left).min((rows - values.len()) as u64);
            values.extend((0..n as i128).map(|k| (block.next + k * step) as i64));
            block.next += n as i128 * step;
            block.left -= n;
            if block.left == 0 {
                reserve.blocks.pop_front();
            }
        }
        values
    }
}

/// An identity column this writer generates, as a computed column of its files.
#[derive(Debug)]
struct Generated {
    sequences: Arc<Sequences>,
    column: usize,
    field: Arc<Field>,
}

impl Computed for Generated {
    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }

    fn admit(&self, _columns: &Columns) -> Result<(), String> {
        self.sequences.claim(self.column)
    }

    fn fill(&self, batch: &RecordBatch) -> ArrayRef {
        Arc::new(Int64Array::from(self.sequences.take(self.column, batch.num_rows())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(start: i64, step: i64) -> Identity {
        Identity {
            name: "id".into(),
            start,
            step,
            explicit: false,
        }
    }

    fn values(block: Option<Block>, step: i64) -> Vec<i64> {
        let Some(block) = block else {
            return Vec::new();
        };
        (0..block.left as i128)
            .map(|k| (block.next + k * step as i128) as i64)
            .collect()
    }

    #[test]
    fn a_block_is_the_grids_next_values_beyond_the_mark() {
        assert_eq!(values(identity(1, 2).block(None, 3), 2), [1, 3, 5]);
        assert_eq!(values(identity(1, 2).block(Some(9), 3), 2), [11, 13, 15]);
        // A mark off the grid, or before its start.
        assert_eq!(values(identity(1, 2).block(Some(10), 2), 2), [11, 13]);
        assert_eq!(values(identity(1, 2).block(Some(-100), 2), 2), [1, 3]);
        assert_eq!(values(identity(10, -3).block(Some(4), 3), -3), [1, -2, -5]);
        assert_eq!(values(identity(10, -3).block(Some(5), 2), -3), [4, 1]);
        assert_eq!(values(identity(10, -3).block(Some(50), 2), -3), [10, 7]);
    }

    #[test]
    fn a_block_ends_at_the_last_value_a_long_holds() {
        assert_eq!(
            values(identity(1, 1).block(Some(i64::MAX - 2), 5), 1),
            [i64::MAX - 1, i64::MAX]
        );
        assert_eq!(identity(1, 1).block(Some(i64::MAX), 5), None);
        assert_eq!(values(identity(0, i64::MAX).block(None, 5), i64::MAX), [0, i64::MAX]);
        assert_eq!(values(identity(-1, i64::MIN).block(None, 5), i64::MIN), [-1]);
        assert_eq!(identity(i64::MIN, -1).block(Some(i64::MIN), 1), None);
        assert_eq!(values(identity(i64::MIN, -1).block(None, 2), -1), [i64::MIN]);
    }
}
