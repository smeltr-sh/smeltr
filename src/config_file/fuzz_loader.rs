//! What the config fuzzers share: random values for a field, the loader's verdict on one bundle, and the seeded run
//! that holds each verdict to the rules. A fuzzer supplies its value pools, its [`Fuzzed`] case, the bundle around
//! it and what it checks of what the loader built; `fuzz_delta` shows the shape.

use std::collections::BTreeSet;
use std::ops::Range;

use serde_json::{Value, json};

use super::{FileConfig, LoadError, LoadedSystem, ValidationError, load_str, validation_errors};
use crate::fuzz::{Coverage, Rng};

/// Each pool pairs a value, as JSON text, with whether the loader must refuse it at its own field.
pub type Pool = &'static [(&'static str, bool)];

pub fn parse(json: &str) -> Value {
    serde_json::from_str(json).unwrap()
}

/// Text pasted from up to seven of `pieces`, the fuzzer's own cuts of what breaks its fields' parsers.
pub fn random_text(rng: &mut Rng, pieces: &[&str]) -> String {
    (0..rng.below(8)).map(|_| *rng.pick(pieces)).collect()
}

/// A value of any JSON type, to stand where a field expects another.
pub fn random_value(rng: &mut Rng, pieces: &[&str]) -> Value {
    match rng.below(9) {
        0 => Value::from(rng.next_u64() as i64),
        1 => Value::from(rng.next_u64()),
        2 => json!(f64::from(rng.below(1000) as u32) / 7.0),
        3 => Value::Bool(rng.chance(2)),
        4 => Value::Null,
        5 => json!([random_text(rng, pieces)]),
        6 => json!({ "secret": random_text(rng, pieces), "version": rng.next_u64() as i64 }),
        7 => json!({ (random_text(rng, pieces)): random_text(rng, pieces) }),
        _ => Value::from(random_text(rng, pieces)),
    }
}

/// A number at or past the edges of every bound: a count, a port, a byte count, a negative, or a size in words.
pub fn random_number(rng: &mut Rng, pieces: &[&str]) -> Value {
    match rng.below(8) {
        0 => Value::from(0),
        1 => Value::from(u64::MAX),
        2 => Value::from(rng.next_u64()),
        3 => Value::from(rng.below(300)),
        4 => Value::from(rng.below(70_000)),
        5 => Value::from(rng.next_u64() >> rng.below(64)),
        6 => Value::from(-(rng.below(100) as i64)),
        _ => Value::from(random_text(rng, pieces)),
    }
}

/// A field's value: one of its known values, or, one time in `density`, one of the field's type off the pool, and
/// now and then one of any type.
pub fn garbage_value(rng: &mut Rng, pieces: &[&str], pool: Pool, density: usize) -> Value {
    if !rng.chance(density) {
        return parse(rng.pick(pool).0);
    }
    if rng.chance(10) {
        return random_value(rng, pieces);
    }
    match parse(pool[0].0) {
        Value::String(_) => Value::from(random_text(rng, pieces)),
        Value::Object(_) => json!({ "secret": random_text(rng, pieces), "version": random_number(rng, pieces) }),
        _ => random_number(rng, pieces),
    }
}

/// One generated item, and what the loader must make of it.
pub trait Fuzzed {
    /// The section of the file the item is put in, and its id there.
    const SECTION: &'static str;
    const ID: &'static str;
    /// What a failure calls the item.
    const NOUN: &'static str;

    fn item(&self) -> &Value;

    /// The fields the loader must refuse. `None` for an item of random values, where only the loader's conduct is
    /// checked.
    fn refused(&self) -> Option<&BTreeSet<&'static str>>;

    /// What a worker is handed for the accepted item, held to the rules it was accepted under.
    fn built(&self, system: &LoadedSystem) -> Result<(), String>;
}

/// What the loader made of one bundle.
#[derive(Clone, Copy, PartialEq)]
pub enum Verdict {
    /// Refused by its JSON shape, before any rule.
    Shape,
    Refused,
    Accepted,
}

/// What the loader made of one bundle, or why it is wrong.
pub fn check<C: Fuzzed>(case: &C, json: &str) -> Result<Verdict, String> {
    let file: FileConfig = match serde_json::from_str(json) {
        Ok(file) => file,
        // A value of the wrong type is refused by its shape. A known case has none.
        Err(_) if case.refused().is_none() => return Ok(Verdict::Shape),
        Err(e) => return Err(format!("a well-formed case did not parse: {e}")),
    };
    let errors = validation_errors(file);
    check_refusals::<C>(&errors)?;
    if let Some(expected) = case.refused() {
        let actual: BTreeSet<_> = errors.iter().filter_map(|e| e.field).collect();
        if &actual != expected {
            return Err(format!(
                "refused at {actual:?}, the rules refuse at {expected:?}; the loader said {:?}",
                messages(&errors)
            ));
        }
    }
    let loaded = load_str(json);
    match (&loaded, errors.is_empty()) {
        (Ok(_), true) | (Err(LoadError::Validation(_)), false) => {}
        _ => {
            return Err(format!(
                "load_str and validation_errors disagree: {:?} against {:?}",
                loaded.as_ref().err().map(ToString::to_string),
                messages(&errors)
            ));
        }
    }
    match loaded {
        Ok(system) => case.built(&system).map(|()| Verdict::Accepted),
        Err(_) => Ok(Verdict::Refused),
    }
}

fn messages(errors: &[ValidationError]) -> Vec<String> {
    errors.iter().map(|e| format!("{:?}: {}", e.field, e.message)).collect()
}

/// Every refusal names the generated item and one of its fields: nothing else in the bundle is wrong.
fn check_refusals<C: Fuzzed>(errors: &[ValidationError]) -> Result<(), String> {
    for e in errors {
        let on_item = e
            .item
            .as_ref()
            .is_some_and(|i| i.section == C::SECTION && i.id == C::ID);
        if !on_item || e.field.is_none() {
            return Err(format!(
                "a refusal names no field of the {}: item {:?}, field {:?}: {}",
                C::NOUN,
                e.item,
                e.field,
                e.message
            ));
        }
    }
    Ok(())
}

/// Runs `generate` over the seeds, and fails naming each seed whose case the loader got wrong, with its item. Every
/// option and `expected` must be reached between the seeds: `reached` marks what one case and its verdict reach
/// besides the fields it was refused at, which are marked here.
pub fn run<C: Fuzzed>(
    seeds: Range<u64>,
    generate: fn(&mut Rng) -> C,
    options: &[&str],
    expected: Vec<String>,
    bundle: impl Fn(&C) -> String,
    reached: impl Fn(&C, Verdict, &mut Coverage),
) {
    let mut coverage = Coverage::new(options.iter().map(|o| o.to_string()).chain(expected));
    crate::fuzz::run(seeds, |seed| {
        let case = generate(&mut Rng::seeded(seed));
        let json = bundle(&case);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(&case, &json)));
        let verdict = match outcome {
            Ok(Ok(verdict)) => verdict,
            Ok(Err(why)) => return Err(format!("{why}\n{}: {}", C::NOUN, case.item())),
            Err(_) => return Err(format!("the loader panicked\n{}: {}", C::NOUN, case.item())),
        };
        for field in case.refused().iter().flat_map(|r| r.iter()) {
            coverage.reach(format!("refused at {field}"));
        }
        reached(&case, verdict, &mut coverage);
        Ok(())
    });
    coverage.assert_complete();
}
