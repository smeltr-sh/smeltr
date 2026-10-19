//! Every reference pipeline maps its records to the documents it should,
//! and `log_run` is what `bench/bench-log.sh` times. A benchmark over a
//! pipeline that produces the wrong document measures nothing, so the
//! mapping checks run with the ordinary tests.

mod panos;
mod reference;

use reference::Pipeline;

const PANOS: Pipeline = Pipeline {
    name: "panos",
    line: panos::line,
    bundle: || panos::bundle(panos::Stage::Ecs),
    check: |rows, docs, kept| {
        assert_eq!(docs, rows, "every row should reach the destination");
        assert!(kept[0].pointer("/source/ip").is_some(), "source.ip");
    },
};

/// Every pipeline the benchmark log times, in the order it logs them.
const ALL: &[&Pipeline] = &[
    &reference::json_app::PIPELINE,
    &reference::k8s::PIPELINE,
    &reference::fortigate::PIPELINE,
    &PANOS,
    &reference::nginx::PIPELINE,
    &reference::cloudtrail::PIPELINE,
    &reference::asa::PIPELINE,
];

fn maps(p: &Pipeline) {
    let (r, sink) = reference::run(&(p.bundle)(), p.line, 2_000, 500, 8);
    let kept: Vec<serde_json::Value> = sink
        .kept()
        .iter()
        .map(|d| serde_json::from_slice(d).expect("the destination gets JSON"))
        .collect();
    assert!(!kept.is_empty(), "{}: no documents", p.name);
    (p.check)(r.rows, r.docs, &kept);
}

#[test]
fn json_app_maps() {
    maps(&reference::json_app::PIPELINE);
}

#[test]
fn k8s_maps() {
    maps(&reference::k8s::PIPELINE);
}

#[test]
fn fortigate_maps() {
    maps(&reference::fortigate::PIPELINE);
}

#[test]
fn panos_maps() {
    maps(&PANOS);
}

#[test]
fn nginx_maps() {
    maps(&reference::nginx::PIPELINE);
}

#[test]
fn cloudtrail_maps() {
    maps(&reference::cloudtrail::PIPELINE);
}

#[test]
fn asa_maps() {
    maps(&reference::asa::PIPELINE);
}

/// One run of the pipeline `BENCH_PIPELINE` names, printed as the line the
/// script parses, on a line of its own: the harness's `test log_run ...`
/// leaves the cursor mid-line. Unset, it prints the names instead, so the script runs
/// each in a process of its own and `perf stat` counts one pipeline.
#[test]
#[ignore = "a measurement, not an assertion: `just bench-log`"]
fn log_run() {
    let Ok(name) = std::env::var("BENCH_PIPELINE") else {
        let names: Vec<&str> = ALL.iter().map(|p| p.name).collect();
        println!("\nNAMES {}", names.join(" "));
        return;
    };
    let p = ALL.iter().find(|p| p.name == name).expect("a pipeline of that name");
    let r = reference::run(&(p.bundle)(), p.line, 400_000, 5_000, 0).0;
    println!(
        "\nBENCH rows={} docs={} bytes_in={} bytes_out={} secs={:.6}",
        r.rows,
        r.docs,
        r.bytes_in,
        r.bytes_out,
        r.wall.as_secs_f64()
    );
}
