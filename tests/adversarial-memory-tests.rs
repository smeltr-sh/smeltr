//! Records built to make a parse expand many times their own size. Each must be refused by the memory budget before
//! its parse holds much more than the budget, as the process's peak resident memory shows. Linux only: the peak is
//! read from `/proc/self/status`, and each test runs in a process of its own under nextest.
#![cfg(target_os = "linux")]

use smeltr::core::buffer::{BufferPool, MemoryBudget, catch_over_budget, quiet_refusals};
use smeltr::core::error::ErrorSink;
use smeltr::plan::config::Step;

use crate::harness;
use harness::*;

const BUDGET: usize = 16 << 20;
const RECORD: usize = 8 << 20;

/// `head`, then `item` repeated to about `len` bytes, then `tail`.
fn record(len: usize, head: &str, item: &str, tail: &str) -> String {
    // Sized once: a buffer freed while it grew would stay resident, and the parse could take it unseen.
    let mut s = String::with_capacity(len + item.len() + tail.len());
    s.push_str(head);
    while s.len() < len {
        s.push_str(item);
    }
    s.push_str(tail);
    s
}

fn kib(field: &str) -> usize {
    let status = std::fs::read_to_string("/proc/self/status").expect("Linux");
    let line = status.lines().find(|l| l.starts_with(field)).expect(field);
    line.split_whitespace()
        .nth(1)
        .and_then(|n| n.parse().ok())
        .expect("a number")
}

/// Run `step` over a record of `shape` under the budget: it must be refused, with resident memory never more than a
/// few budgets above where it started, however far the record would expand. The budget is checked after an
/// allocation, so the last one can overshoot it, a buffer that doubles holds both its blocks while it copies, and the
/// allocator takes memory in huge pages, which become resident 2 MiB at a time.
fn refused_within_budget(step: Step, shape: impl Fn(usize) -> String) {
    // As a worker does: the default hook would load debug info to print a backtrace, which is memory too.
    quiet_refusals();
    let plan = compile_ok(vec![step]);
    let mut env = Env {
        pool: BufferPool::with_budget(MemoryBudget::new(BUDGET)),
        errors: ErrorSink::new(),
    };
    let mut scratch = BufferPool::new();
    // A small record of the same shape first, so the code the parse runs is resident before the peak is measured.
    let mut batch = raw_batch(&plan, &[&shape(4096)], &mut scratch);
    env.exec(&plan, &mut batch);
    batch.reclaim(&mut env.pool);
    let mut batch = raw_batch(&plan, &[&shape(RECORD)], &mut scratch);
    let before = kib("VmRSS:");
    std::fs::write("/proc/self/clear_refs", "5").expect("the peak resets");
    let refused = catch_over_budget(|| env.exec(&plan, &mut batch)).is_err();
    let grew = kib("VmHWM:").saturating_sub(before) << 10;
    println!("peaked {} MiB above the start", grew >> 20);
    assert!(refused, "the budget refused the record");
    assert!(grew < 4 * BUDGET, "peaked {} MiB above the start", grew >> 20);
    batch.reclaim(&mut scratch);
}

/// An object copied whole, then entered by a later dotted key, which lays each of its keys out one by one.
#[test]
fn many_keys_laid_out_again() {
    refused_within_budget(code(0, "Object.assign($, json(_raw, {splitDots: true}));"), |len| {
        let mut s = String::with_capacity(len + 32);
        s.push_str("{\"a\":{");
        let mut n = 0;
        while s.len() < len {
            s.push_str(&format!("\"{n:x}\":0,"));
            n += 1;
        }
        s.push_str("\"end\":0},\"a.x\":1}");
        s
    });
}

#[test]
fn one_repeated_key() {
    refused_within_budget(code(0, "Object.assign($, json(_raw));"), |len| {
        record(len, "{", "\"a\":1,", "\"a\":1}")
    });
}

#[test]
fn a_long_array_of_small_values_copied() {
    refused_within_budget(code(0, "Object.assign($, json(_raw));"), |len| {
        record(len, "{\"a\":[", "0,", "0]}")
    });
}

#[test]
fn a_long_array_of_small_values_declared() {
    refused_within_budget(code(0, "a: array<i64>; Object.assign($, json(_raw));"), |len| {
        record(len, "{\"a\":[", "0,", "0]}")
    });
}

#[test]
fn a_key_of_many_dots() {
    refused_within_budget(code(0, "Object.assign($, json(_raw, {splitDots: true}));"), |len| {
        record(len, "{\"", "a.", "a\":1}")
    });
}

#[test]
fn many_short_pairs() {
    refused_within_budget(code(0, "Object.assign($, kv(_raw));"), |len| {
        record(len, "", "a=1 ", "a=1")
    });
}
