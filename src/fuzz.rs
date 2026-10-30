//! What every seeded fuzzer shares: a PRNG, the loop over a seed range, and the check that every option the fuzzer
//! knows was reached.
//!
//! A fuzzer runs a seed range in `cargo test`. A failure names its seed: `SMELTR_FUZZ_SEED=n` runs seed `n` alone,
//! and `SMELTR_FUZZ_SEEDS=n` runs `n` seeds rather than the fuzzer's default.
//!
//! Test-only and std-only: the crate's unit tests reach it as `crate::fuzz`, and `tests/harness` and the `smeltr`
//! binary's tests compile this same file as their own `fuzz`, so it can name nothing from any of them.

use std::collections::BTreeSet;
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// xorshift64*. A state of 0 gives 0 forever, so `seeded` is the way to start one from any seed.
pub struct Rng(pub u64);

impl Rng {
    /// A generator for `seed`, 0 included.
    pub fn seeded(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    /// In `0..n`. `n` must not be 0.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// An index into a list of `len`, or `None` when it is empty. It draws once either way, so a list whose length
    /// timing decides leaves the draws after it as they were.
    pub fn index(&mut self, len: usize) -> Option<usize> {
        let x = self.next_u64();
        (len > 0).then(|| (x % len as u64) as usize)
    }

    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }

    pub fn chance(&mut self, one_in: usize) -> bool {
        self.below(one_in) == 0
    }
}

/// `SMELTR_FUZZ_SEED` is set: one seed is replayed, so what holds only over a range is not checked.
pub fn replaying() -> bool {
    env_u64("SMELTR_FUZZ_SEED").is_some()
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|s| s.parse().ok())
}

/// The seeds to run: `default`, or what `SMELTR_FUZZ_SEED` or `SMELTR_FUZZ_SEEDS` asks for in its place.
pub fn seeds(default: Range<u64>) -> Range<u64> {
    if let Some(n) = env_u64("SMELTR_FUZZ_SEED") {
        return n..n + 1;
    }
    match env_u64("SMELTR_FUZZ_SEEDS") {
        Some(n) => default.start..default.start + n,
        None => default,
    }
}

/// Runs `check` on each of `seeds(default)` and fails naming every seed that returned an error or panicked, not only
/// the first.
pub fn run(default: Range<u64>, mut check: impl FnMut(u64) -> Result<(), String>) {
    let seeds = seeds(default);
    let total = seeds.end - seeds.start;
    let mut failures = Vec::new();
    for seed in seeds {
        match catch_unwind(AssertUnwindSafe(|| check(seed))) {
            Ok(Ok(())) => {}
            Ok(Err(why)) => failures.push(format!("seed {seed}: {why}")),
            Err(panic) => {
                let why = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("");
                failures.push(format!("seed {seed} panicked: {why}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {total} seeds failed:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

/// The options a fuzzer knows, and which of them its seeds reached.
pub struct Coverage {
    expected: BTreeSet<String>,
    reached: BTreeSet<String>,
}

impl Coverage {
    pub fn new<S: Into<String>>(expected: impl IntoIterator<Item = S>) -> Coverage {
        Coverage {
            expected: expected.into_iter().map(Into::into).collect(),
            reached: BTreeSet::new(),
        }
    }

    pub fn reach(&mut self, name: impl Into<String>) {
        self.reached.insert(name.into());
    }

    /// The options no seed reached, in order.
    pub fn missed(&self) -> Vec<&str> {
        self.expected.difference(&self.reached).map(String::as_str).collect()
    }

    /// Fails naming each option no seed reached. A replayed seed is not expected to reach them all.
    pub fn assert_complete(&self) {
        if replaying() {
            return;
        }
        let missed = self.missed();
        assert!(missed.is_empty(), "no seed reached {}", missed.join(", "));
    }
}
