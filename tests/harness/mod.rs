//! What an end-to-end pipeline test needs: a plan compiled from `Step`s, a
//! batch with `_raw` seeded, and the JSON that comes back out.
//!
//! Each `tests/*.rs` file is its own crate, so this is shared the way
//! `panos` and `live` already are -- `mod harness;` at the top of the file.

#![allow(dead_code)]

#[path = "../../src/fuzz.rs"]
pub mod fuzz;

pub mod destination;
pub mod docker;
pub mod http;
pub mod receiver;
pub mod registry;

use smeltr::core::batch::{Batch, SLOT_RAW};
use smeltr::core::buffer::BufferPool;
use smeltr::core::column::{StrBuilder, VarType};
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ErrorSink;
use smeltr::core::id::{FunctionId, SlotId};
use smeltr::plan::compile::{Plan, compile};
use smeltr::plan::config::{CompileError, FunctionConfig, FunctionKind, PipelineConfig, Step};
use smeltr::plan::output::run_to_json;
use smeltr::plan::topology::SourceMeta;

pub fn func(id: FunctionId, kind: FunctionKind) -> Step {
    Step::Function(FunctionConfig::new(id, kind))
}

/// A code step: `body` in the code language.
pub fn code(id: FunctionId, body: &str) -> Step {
    func(id, FunctionKind::Code { body: body.to_string() })
}

pub fn pipeline(body: Vec<Step>) -> PipelineConfig {
    PipelineConfig {
        id: 0,
        name: "test".into(),
        body,
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }
}

/// Compile against a Kafka source, or say why it would not.
pub fn compile_ok(body: Vec<Step>) -> Plan {
    compile_ok_for(SourceMeta::KAFKA, body)
}

pub fn compile_ok_for(meta: SourceMeta, body: Vec<Step>) -> Plan {
    compile(&pipeline(body), &meta).unwrap_or_else(|e| {
        panic!(
            "compile failed: {}",
            e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
        )
    })
}

/// The errors a body is expected to fail with.
pub fn compile_err(body: Vec<Step>) -> Vec<CompileError> {
    compile(&pipeline(body), &SourceMeta::KAFKA).expect_err("expected this body to be refused")
}

/// The slot a source's declared field occupies, by name. `SourceMeta::column`
/// is name-keyed so a field can move within the list; keying a test to the
/// position instead makes a reorder silently retarget it rather than fail.
pub fn slot_of(meta: SourceMeta, name: &str) -> SlotId {
    let at = meta
        .source_fields()
        .iter()
        .position(|(n, _)| *n == name)
        .unwrap_or_else(|| panic!("{name} is not a field this source declares"));
    meta.source_slot(at)
}

pub struct Env {
    pub pool: BufferPool,
    pub errors: ErrorSink,
}

impl Env {
    pub fn new() -> Env {
        Env {
            pool: BufferPool::new(),
            errors: ErrorSink::new(),
        }
    }

    /// The serialized JSON, one row per output record.
    pub fn run(&mut self, plan: &Plan, batch: &mut Batch) -> Vec<Vec<u8>> {
        let mut ctx = Ctx::new(&mut self.pool, &mut self.errors);
        run_to_json(plan, batch, &mut ctx)
    }

    /// The same rows, parsed -- for asserting on fields rather than bytes.
    pub fn run_json(&mut self, plan: &Plan, batch: &mut Batch) -> Vec<serde_json::Value> {
        self.run(plan, batch).iter().map(|row| parsed(row)).collect()
    }

    /// Run the plan without serializing: for tests that read the batch after.
    pub fn exec(&mut self, plan: &Plan, batch: &mut Batch) {
        let mut ctx = Ctx::new(&mut self.pool, &mut self.errors);
        smeltr::plan::exec::run(plan, batch, &mut ctx);
    }
}

impl Default for Env {
    fn default() -> Env {
        Env::new()
    }
}

/// Seed `_raw` with one record's text per row.
///
/// `Str`, as `core::batch` declares it and as ingest builds it: a record is
/// decoded once from its source's encoding, so everything downstream reads
/// UTF-8. Seeding `Bytes` here would exercise an arm production never takes.
pub fn seed_raw(env: &mut Env, batch: &mut Batch, texts: &[&str]) {
    put_raw(&mut env.pool, batch, texts);
}

pub fn put_raw(pool: &mut BufferPool, batch: &mut Batch, texts: &[&str]) {
    let mut b = StrBuilder::new(pool, VarType::Str, texts.len());
    b.reserve(pool, texts.iter().map(|d| d.len()).sum());
    for d in texts {
        b.push(d.as_bytes());
    }
    let col = b.finish();
    batch.put(SLOT_RAW, col, pool);
}

/// A batch laid out for `plan` as a run hands its rows over (`Plan::delivery_layout`), with `_raw` seeded and nothing
/// else bound.
pub fn raw_batch(plan: &Plan, texts: &[&str], pool: &mut BufferPool) -> Batch {
    let mut batch = Batch::new(&plan.delivery_layout(), texts.len(), 0);
    put_raw(pool, &mut batch, texts);
    batch
}

/// `rows` rows whose `_raw` is empty -- for a pipeline that never reads it.
pub fn seed_raw_empty(env: &mut Env, batch: &mut Batch, rows: usize) {
    seed_raw(env, batch, &vec![""; rows]);
}

pub fn parsed(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
}

/// `plan` run over `texts`, its live rows cut into Parquet columns as its record, as egress cuts them for a Parquet
/// destination: a row's place in the chunk is its place among the live rows.
pub fn cut(plan: &Plan, texts: &[&str]) -> std::sync::Arc<smeltr::io::parquet::Chunk> {
    use smeltr::io::parquet::{Chunk, Schema};
    let mut env = Env::new();
    let mut batch = raw_batch(plan, texts, &mut env.pool);
    env.exec(plan, &mut batch);
    let live = batch.peek(plan.live_slot).bool().unwrap();
    let rows: Vec<u32> = (0..batch.rows() as u32)
        .filter(|&r| live.validity.is_valid(r as usize) && smeltr::core::bits::get_row(live.values, r as usize))
        .collect();
    let schema = Schema::new(&record(plan)).unwrap();
    let view = smeltr::format::record::RecordView::new(&plan.tree, |s| batch.peek(s));
    let chunk = Chunk::cut(&schema, &[(&view, &rows)], 0, &mut env.pool);
    drop(view);
    batch.reclaim(&mut env.pool);
    std::sync::Arc::new(chunk)
}

/// The record `plan` writes, its tag field the default's.
pub fn record(plan: &Plan) -> smeltr::core::column::DataType {
    smeltr::plan::dictionary::record_type(plan, smeltr::plan::config::DEFAULT_TAG_FIELD)
}
