// The design document and doc comments name `plan::plan::compile` and `plan::plan::liveness`; the module keeps its
// name.
#![allow(clippy::module_inception)]

pub mod binding;
pub mod config;
pub mod dictionary;
pub mod dot;
pub mod exec;
pub mod lower;
pub mod output;
pub mod parse_call;
pub mod plan;
pub mod resolve;
pub mod topology;
pub mod tree;
