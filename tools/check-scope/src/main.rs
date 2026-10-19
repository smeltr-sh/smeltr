//! `just check`: the checks the files changed since origin/main can break, each a recipe in the justfile, run side
//! by side. `just check-all` runs every one and holds a merge to main, so a choice here that misses a test costs a
//! round trip, never what reaches main.
//!
//! - Documents are read only by `tools/citations`.
//! - A `src/` file picks its module's unit tests, the integration modules that name its module or share a word of
//!   its name (`src/io/kafka.rs` picks `kafka_tests`), and the tests whose source names its path. One under
//!   `src/bin/` picks the binaries' tests and the tests that launch the binary.
//! - A `tests/` file picks its integration module or its own test target. One the targets share picks all of them.
//! - The GUI picks its own checks. The site picks its own and the tests that hold it to the engine, except what the
//!   GUI or `build.rs` names, which runs everything.
//! - A crate under `tools/` picks its own tests.
//! - `Cargo.toml`, `Cargo.lock`, `deny.toml` and `build.rs` pick every Rust test; all but `build.rs` add `cargo deny`.
//! - Any other file picks the Rust tests whose source names it. One nothing names, or no merge base, runs everything:
//!   a path not placed costs time, never coverage.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitCode};
use std::{env, fs};

fn main() -> ExitCode {
    let root = env::current_dir().expect("the working directory");
    let plan = match changed(&root) {
        Some(paths) if !paths.is_empty() => Tree::read(&root).plan(&paths),
        Some(_) => Plan::all("nothing changed since origin/main"),
        None => Plan::all("no merge base with origin/main"),
    };
    run(&plan)
}

/// The files changed since this branch left origin/main, committed or not, or None without a merge base.
fn changed(root: &Path) -> Option<Vec<String>> {
    let git = |args: &[&str]| {
        let out = Command::new("git").args(args).current_dir(root).output().ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    };
    let base = git(&["merge-base", "HEAD", "origin/main"])?;
    let diff = git(&["diff", "--name-only", "--no-renames", "-z", base.trim()])?;
    let new = git(&["ls-files", "--others", "--exclude-standard", "-z"])?;
    let paths: BTreeSet<String> = diff
        .split('\0')
        .chain(new.split('\0'))
        .filter(|p| !p.is_empty())
        .map(Into::into)
        .collect();
    Some(paths.into_iter().collect())
}

/// What to run, and why.
#[derive(Default, Debug)]
struct Plan {
    all: bool,
    rust_all: bool,
    /// nextest filter terms, one per module picked.
    tests: BTreeSet<String>,
    packages: BTreeSet<String>,
    clippy: bool,
    frontend: bool,
    website: bool,
    docs: bool,
    deny: bool,
    why: Vec<String>,
}

impl Plan {
    fn all(why: &str) -> Plan {
        Plan {
            all: true,
            why: vec![why.into()],
            ..Plan::default()
        }
    }

    /// The `just` invocations, each run beside the others. Each recipe that updates stable waits on it, so the
    /// update runs once before them all rather than in each.
    fn jobs(&self) -> Vec<Vec<String>> {
        let job = |args: &[&str]| args.iter().map(|a| a.to_string()).collect::<Vec<_>>();
        if self.all {
            return vec![job(&["check-all"])];
        }
        let mut jobs = Vec::new();
        if self.rust_all {
            jobs.push(job(&["check-rust"]));
        } else {
            jobs.push(job(&["fmt-check"]));
            if !self.tests.is_empty() {
                let terms: Vec<String> = self.tests.iter().map(|t| format!("({t})")).collect();
                jobs.push(job(&["check-tests", &terms.join(" | ")]));
            }
        }
        for p in &self.packages {
            jobs.push(job(&["check-package", p]));
        }
        let flags = [
            (self.clippy, "clippy"),
            (self.frontend, "check-frontend"),
            (self.website, "website"),
            (self.docs, "check-docs"),
            (self.deny, "deny"),
        ];
        jobs.extend(flags.iter().filter(|(on, _)| *on).map(|(_, r)| job(&[r])));
        jobs
    }
}

/// What a Rust source file's tests are, as nextest names them.
#[derive(Debug, PartialEq)]
enum Owner {
    /// The library's tests under a module path.
    Unit(String),
    /// An integration module, a file under `tests/` that `tests/integration.rs` includes.
    Integration(String),
    /// A test target of its own in `Cargo.toml`, by name.
    Target(String),
    /// A target `Cargo.toml` builds but nextest never runs (`test = false`): clippy builds it.
    Built,
    /// The binaries' tests.
    Bins,
    /// Every test target: a file they share.
    AllTests,
    /// Every Rust test.
    Rust,
}

impl Owner {
    /// The nextest filter term, or None for every Rust test and for a target nextest never runs.
    fn term(&self) -> Option<String> {
        Some(match self {
            Owner::Unit(m) => format!("binary_id(=smeltr) & test(/^{m}::/)"),
            Owner::Integration(m) => format!("binary_id(=smeltr::integration) & test(/^{m}::/)"),
            Owner::Target(name) => format!("binary_id(=smeltr::{name})"),
            Owner::Bins => "kind(bin)".into(),
            Owner::AllTests => "kind(test)".into(),
            Owner::Built | Owner::Rust => return None,
        })
    }
}

/// The sources the plan reads: every `.rs` file under `src/` and `tests/`, and the site paths the GUI and
/// `build.rs` name.
struct Tree {
    sources: BTreeMap<String, String>,
    /// Files a `#[path]` attribute places, by the module path it gives them.
    placed: BTreeMap<String, String>,
    /// The integration modules, by file.
    integration: BTreeMap<String, String>,
    /// The test targets of their own, by file: each one's name, or None where nextest never runs it.
    targets: BTreeMap<String, Option<String>>,
    shared_site: BTreeSet<String>,
    /// The GUI's files that name a `website/` path: what can break a build that carries only the files it names.
    site_importers: BTreeSet<String>,
    /// The crates under `tools/`, by directory, which is each one's package name.
    crates: BTreeSet<String>,
}

impl Tree {
    fn read(root: &Path) -> Tree {
        let mut files = BTreeMap::new();
        for dir in ["src", "tests", "gui-frontend/src"] {
            walk(root, &root.join(dir), &mut files);
        }
        for entry in fs::read_dir(root.join("tools")).into_iter().flatten().flatten() {
            let manifest = entry.path().join("Cargo.toml");
            if manifest.exists() {
                files.insert(
                    format!("tools/{}/Cargo.toml", entry.file_name().to_string_lossy()),
                    String::new(),
                );
            }
        }
        for file in ["Cargo.toml", "build.rs", "gui-frontend/vite.config.ts"] {
            if let Ok(text) = fs::read_to_string(root.join(file)) {
                files.insert(file.to_string(), text);
            }
        }
        Tree::new(files)
    }

    /// `files` maps a path from the repository root, `/`-separated, to its text.
    fn new(files: BTreeMap<String, String>) -> Tree {
        let mut shared_site = BTreeSet::new();
        let mut site_importers = BTreeSet::new();
        for (path, text) in &files {
            if path == "build.rs" || path.starts_with("gui-frontend/") {
                let names = site_names(text);
                if path != "build.rs" && !names.is_empty() {
                    site_importers.insert(path.clone());
                }
                shared_site.extend(names);
            }
        }
        let targets = files.get("Cargo.toml").map(|t| test_targets(t)).unwrap_or_default();
        let crates = files
            .keys()
            .filter_map(|p| p.strip_prefix("tools/")?.strip_suffix("/Cargo.toml"))
            .map(String::from)
            .collect();
        let sources: BTreeMap<String, String> = files
            .into_iter()
            .filter(|(p, _)| p.ends_with(".rs") && (p.starts_with("src/") || p.starts_with("tests/")))
            .collect();
        let mut placed = BTreeMap::new();
        let mut integration = BTreeMap::new();
        for (path, text) in &sources {
            let module = if path == "tests/integration.rs" {
                Some(String::new())
            } else {
                module_of(path)
            };
            let Some(module) = module else { continue };
            let dir = Path::new(path).parent().unwrap_or(Path::new(""));
            for (target, name) in path_mods(text) {
                let file = normalize(&dir.join(target));
                if path == "tests/integration.rs" {
                    integration.insert(file, name);
                } else {
                    placed.insert(file, format!("{module}::{name}"));
                }
            }
        }
        Tree {
            sources,
            placed,
            integration,
            targets,
            shared_site,
            site_importers,
            crates,
        }
    }

    fn owner(&self, path: &str) -> Owner {
        if let Some(m) = self.integration.get(path) {
            return Owner::Integration(m.clone());
        }
        if let Some(target) = self.targets.get(path) {
            return target.clone().map_or(Owner::Built, Owner::Target);
        }
        if path.starts_with("tests/") {
            return Owner::AllTests;
        }
        if path.starts_with("src/bin/") {
            return Owner::Bins;
        }
        match self.placed.get(path).cloned().or_else(|| module_of(path)) {
            Some(m) => Owner::Unit(m),
            None => Owner::Rust,
        }
    }

    fn plan(&self, changed: &[String]) -> Plan {
        let mut plan = Plan::default();
        for path in changed {
            if !self.place(path, &mut plan) {
                return Plan::all(&format!("{path} needs the whole check"));
            }
            if plan.all {
                return plan;
            }
        }
        if !plan.tests.is_empty() || !plan.packages.is_empty() {
            plan.clippy = true;
        }
        plan
    }

    /// Adds what `path` can break to `plan`; false when nothing here places it.
    fn place(&self, path: &str, plan: &mut Plan) -> bool {
        let why = |plan: &mut Plan, what: &str| plan.why.push(format!("{path}: {what}"));
        if path.starts_with("website/") {
            if self.shared_site.iter().any(|n| path.starts_with(n.as_str())) {
                plan.all = true;
                plan.why
                    .push(format!("{path}: the GUI or build.rs reads it, so everything"));
                return true;
            }
            plan.website = true;
            plan.docs = true;
            plan.tests
                .insert(Owner::Integration("website_reference_tests".into()).term().unwrap());
            why(plan, "the site and the tests that hold it to the engine");
            return true;
        }
        if path.starts_with("docs/") || path.ends_with(".md") || path == "LICENSE" {
            plan.docs = true;
            why(plan, "the citation checks");
            return true;
        }
        if path.starts_with("gui-frontend/") {
            plan.frontend = true;
            plan.docs = true;
            let mut readers = self.readers(path);
            if self.site_importers.contains(path) {
                readers.push(Owner::Integration("gui_build_inputs_tests".into()));
            }
            why(plan, &format!("the GUI's checks{}", named(&readers)));
            self.add(plan, readers);
            return true;
        }
        if let Some(name) = self.crates.iter().find(|c| path.starts_with(&format!("tools/{c}/"))) {
            plan.packages.insert(name.clone());
            why(plan, &format!("the {name} crate's tests"));
            return true;
        }
        if matches!(path, "Cargo.toml" | "Cargo.lock" | "deny.toml" | "build.rs") {
            plan.rust_all = true;
            plan.clippy = true;
            plan.deny |= path != "build.rs";
            why(plan, "every Rust test");
            return true;
        }
        if path.ends_with(".rs") && (path.starts_with("src/") || path.starts_with("tests/")) {
            let mut owners = vec![self.owner(path)];
            match &owners[0] {
                Owner::Unit(m) => owners.extend(self.users(m)),
                Owner::Bins => owners.extend(self.readers("CARGO_BIN_EXE_smeltr")),
                _ => {}
            }
            if path.starts_with("src/") {
                owners.extend(self.readers(path));
            }
            why(plan, &format!("its tests{}", named(&owners)));
            self.add(plan, owners);
            return true;
        }
        let readers = self.readers(path);
        if readers.is_empty() {
            return false;
        }
        why(plan, &format!("the tests that read it{}", named(&readers)));
        self.add(plan, readers);
        true
    }

    fn add(&self, plan: &mut Plan, owners: Vec<Owner>) {
        for owner in owners {
            match owner.term() {
                Some(t) => {
                    plan.tests.insert(t);
                }
                None if owner == Owner::Built => plan.clippy = true,
                None => plan.rust_all = true,
            }
        }
    }

    /// The owners of the other sources that name `text`.
    fn readers(&self, text: &str) -> Vec<Owner> {
        self.sources
            .iter()
            .filter(|(p, source)| *p != text && source.contains(text))
            .map(|(p, _)| self.owner(p))
            .collect()
    }

    /// The integration modules that name `module` or one of its ancestors below the crate's top level, or whose name
    /// begins with a word of it.
    fn users(&self, module: &str) -> Vec<Owner> {
        let segs: Vec<&str> = module.split("::").collect();
        let mut names: Vec<String> = (2..=segs.len())
            .map(|n| format!("smeltr::{}", segs[..n].join("::")))
            .collect();
        if segs.len() == 1 {
            names.push(format!("smeltr::{module}"));
        }
        let words: Vec<String> = segs[if segs.len() == 1 { 0 } else { 1 }..]
            .iter()
            .filter(|s| !matches!(**s, "mod" | "tests" | "fuzz"))
            .map(|s| format!("{s}_"))
            .collect();
        let mut out = Vec::new();
        for (file, m) in &self.integration {
            let text = self.sources.get(file).map(String::as_str).unwrap_or("");
            if names.iter().any(|n| names_module(text, n)) || words.iter().any(|w| m.starts_with(w.as_str())) {
                out.push(Owner::Integration(m.clone()));
            }
        }
        out
    }
}

/// `" for a, b"`, naming the modules picked, or nothing.
fn named(owners: &[Owner]) -> String {
    let names: Vec<String> = owners
        .iter()
        .map(|o| match o {
            Owner::Unit(m) | Owner::Integration(m) | Owner::Target(m) => m.clone(),
            Owner::Built => "clippy's build of it".into(),
            Owner::Bins => "the binaries".into(),
            Owner::AllTests => "every test target".into(),
            Owner::Rust => "every Rust test".into(),
        })
        .collect();
    if names.is_empty() {
        String::new()
    } else {
        format!(", {}", names.join(", "))
    }
}

/// Whether `text` names `path` as a whole path, not a prefix of a longer one.
fn names_module(text: &str, path: &str) -> bool {
    text.match_indices(path)
        .any(|(i, _)| !text[i + path.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_'))
}

/// The module a file under `src/` declares by where it sits, or None for the crate roots and anything else.
fn module_of(path: &str) -> Option<String> {
    let rest = path.strip_prefix("src/")?.strip_suffix(".rs")?;
    if matches!(rest, "lib" | "main") || rest.starts_with("bin/") {
        return None;
    }
    Some(rest.strip_suffix("/mod").unwrap_or(rest).replace('/', "::"))
}

/// Each `[[test]]` target in a manifest but `tests/integration.rs`, by path: its name, or None with `test = false`.
fn test_targets(manifest: &str) -> BTreeMap<String, Option<String>> {
    let mut out = BTreeMap::new();
    for block in manifest.split("[[test]]").skip(1) {
        let block = block.split("\n[").next().unwrap_or("");
        let field = |key: &str| {
            block.lines().find_map(|l| {
                let v = l.trim().strip_prefix(key)?.trim_start().strip_prefix('=')?.trim();
                Some(v.trim_matches('"').to_string())
            })
        };
        let (Some(name), Some(path)) = (field("name"), field("path")) else {
            continue;
        };
        if path != "tests/integration.rs" {
            out.insert(path, (field("test").as_deref() != Some("false")).then_some(name));
        }
    }
    out
}

/// Each `#[path = "..."]` and the module it names on the line that follows.
fn path_mods(text: &str) -> Vec<(String, String)> {
    let lines: Vec<&str> = text.lines().map(str::trim).collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let Some(target) = line.strip_prefix("#[path = \"").and_then(|r| r.strip_suffix("\"]")) else {
            continue;
        };
        let next = lines.get(i + 1).copied().unwrap_or("");
        let decl = next.strip_prefix("pub ").unwrap_or(next);
        if let Some(name) = decl.strip_prefix("mod ").and_then(|r| r.strip_suffix(';')) {
            out.push((target.to_string(), name.trim().to_string()));
        }
    }
    out
}

/// `a/b/../c` as `a/c`, `/`-separated.
fn normalize(path: &Path) -> String {
    let mut parts: Vec<String> = Vec::new();
    for c in path.components() {
        match c {
            Component::ParentDir => {
                parts.pop();
            }
            Component::Normal(p) => parts.push(p.to_string_lossy().into_owned()),
            _ => {}
        }
    }
    parts.join("/")
}

/// Every `website/...` path `text` names.
fn site_names(text: &str) -> Vec<String> {
    let ok = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '/' | '-');
    text.match_indices("website/")
        .map(|(i, _)| {
            let name: String = text[i..].chars().take_while(|c| ok(*c)).collect();
            name.trim_end_matches(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .to_string()
        })
        .filter(|n| n.len() > "website/".len())
        .collect()
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, out);
        } else if let Ok(text) = fs::read_to_string(&path) {
            let rel: PathBuf = path.strip_prefix(root).unwrap_or(&path).to_path_buf();
            out.insert(normalize(&rel), text);
        }
    }
}

fn run(plan: &Plan) -> ExitCode {
    for why in &plan.why {
        eprintln!("check: {why}");
    }
    let jobs = plan.jobs();
    let names: Vec<String> = jobs.iter().map(|j| j[0].clone()).collect();
    eprintln!("check: running {}", names.join(", "));
    let stable = jobs.iter().any(|j| {
        matches!(
            j[0].as_str(),
            "check-all" | "check-rust" | "check-tests" | "clippy" | "deny" | "check-package"
        )
    });
    if stable && !Command::new("just").arg("stable").status().is_ok_and(|s| s.success()) {
        return ExitCode::FAILURE;
    }
    let children: Vec<_> = jobs
        .iter()
        .map(|job| {
            (
                job[0].clone(),
                Command::new("just").args(job).env("SMELTR_STABLE_UPDATED", "1").spawn(),
            )
        })
        .collect();
    let mut failed = Vec::new();
    for (name, child) in children {
        if !child.and_then(|mut c| c.wait()).is_ok_and(|s| s.success()) {
            failed.push(name);
        }
    }
    if failed.is_empty() {
        eprintln!("check: passed");
        ExitCode::SUCCESS
    } else {
        eprintln!("check: failed: {}", failed.join(", "));
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(files: &[(&str, &str)]) -> Tree {
        Tree::new(files.iter().map(|(p, t)| (p.to_string(), t.to_string())).collect())
    }

    fn sample() -> Tree {
        tree(&[
            (
                "tests/integration.rs",
                "mod harness;\n#[path = \"kafka-tests.rs\"]\nmod kafka_tests;\n#[path = \"gate-tests.rs\"]\nmod gate_tests;\n#[path = \"csv-tests.rs\"]\nmod csv_tests;\n",
            ),
            ("tests/kafka-tests.rs", "use smeltr::io::kafka::Consumer;\n"),
            (
                "tests/gate-tests.rs",
                "env!(\"CARGO_BIN_EXE_smeltr\"); read(\"src/config_file/dto.rs\")",
            ),
            (
                "Cargo.toml",
                "[[test]]\nname = \"integration\"\npath = \"tests/integration.rs\"\n\n[[test]]\nname = \"reference-pipelines\"\npath = \"tests/reference-pipelines.rs\"\n\n[[test]]\nname = \"tick-profile\"\npath = \"tests/tick-profile.rs\"\ntest = false\n\n[dependencies]\n",
            ),
            ("tests/reference-pipelines.rs", "mod reference;\n"),
            ("tests/tick-profile.rs", ""),
            ("src/config_file/dto.rs", ""),
            ("src/bin/smeltr.rs", ""),
            (
                "tests/csv-tests.rs",
                "use smeltr::format::csv::Parser;\nuse smeltr::io::kafkaesque;\n",
            ),
            ("tests/harness/mod.rs", ""),
            ("src/io/kafka.rs", ""),
            ("src/io/iceberg/table.rs", "#[path = \"fuzz.rs\"]\nmod fuzz;\n"),
            (
                "src/ops/stdfuncs.rs",
                "include_str!(\"../../gui-frontend/src/stdlib.ts\")",
            ),
            ("build.rs", "watch(\"website/src/lib/highlight\")"),
            (
                "gui-frontend/src/main.ts",
                "import '../../website/src/lib/foundry/palette.css';",
            ),
            ("tools/citations/Cargo.toml", ""),
        ])
    }

    fn plan(changed: &[&str]) -> Plan {
        sample().plan(&changed.iter().map(|p| p.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn documents_alone_run_the_citation_checks() {
        let p = plan(&["docs/a.md", "README.md", "LICENSE"]);
        assert!(p.docs && !p.all && !p.clippy && p.tests.is_empty());
        assert_eq!(
            p.jobs(),
            [vec!["fmt-check".to_string()], vec!["check-docs".to_string()]]
        );
    }

    #[test]
    fn a_source_file_picks_its_module_and_the_integration_modules_that_use_it() {
        let p = plan(&["src/io/kafka.rs"]);
        assert_eq!(
            p.tests.into_iter().collect::<Vec<_>>(),
            [
                "binary_id(=smeltr) & test(/^io::kafka::/)",
                "binary_id(=smeltr::integration) & test(/^kafka_tests::/)",
            ]
        );
        assert!(p.clippy && !p.rust_all && !p.deny);
    }

    #[test]
    fn a_source_file_picks_the_tests_that_read_it() {
        assert!(
            plan(&["src/config_file/dto.rs"])
                .tests
                .contains("binary_id(=smeltr::integration) & test(/^gate_tests::/)")
        );
    }

    #[test]
    fn a_binary_picks_the_tests_that_launch_it() {
        let p = plan(&["src/bin/smeltr.rs"]);
        assert!(p.tests.contains("kind(bin)"));
        assert!(
            p.tests
                .contains("binary_id(=smeltr::integration) & test(/^gate_tests::/)")
        );
    }

    #[test]
    fn a_test_target_of_its_own_picks_its_binary() {
        let p = plan(&["tests/reference-pipelines.rs"]);
        assert_eq!(
            p.tests.into_iter().collect::<Vec<_>>(),
            ["binary_id(=smeltr::reference-pipelines)"]
        );
        let p = plan(&["tests/tick-profile.rs"]);
        assert!(p.clippy && p.tests.is_empty() && !p.rust_all && !p.all);
        assert!(plan(&["tests/reference/mod.rs"]).tests.contains("kind(test)"));
    }

    #[test]
    fn a_module_path_is_named_whole() {
        assert!(names_module("use smeltr::io::kafka::X;", "smeltr::io::kafka"));
        assert!(!names_module("use smeltr::io::kafkaesque;", "smeltr::io::kafka"));
    }

    #[test]
    fn a_path_attribute_places_its_file() {
        assert_eq!(
            sample().owner("src/io/iceberg/fuzz.rs"),
            Owner::Unit("io::iceberg::table::fuzz".into())
        );
    }

    #[test]
    fn a_test_file_picks_its_module_and_a_shared_one_picks_them_all() {
        assert_eq!(
            sample().owner("tests/csv-tests.rs"),
            Owner::Integration("csv_tests".into())
        );
        assert_eq!(sample().owner("tests/harness/mod.rs"), Owner::AllTests);
        assert_eq!(sample().owner("src/lib.rs"), Owner::Rust);
    }

    #[test]
    fn the_gui_picks_its_checks_and_the_tests_that_read_it() {
        let p = plan(&["gui-frontend/src/stdlib.ts"]);
        assert!(p.frontend && p.docs && !p.all);
        assert!(p.tests.contains("binary_id(=smeltr) & test(/^ops::stdfuncs::/)"));
        assert!(plan(&["gui-frontend/src/App.svelte"]).tests.is_empty());
    }

    #[test]
    fn a_gui_file_that_imports_from_the_site_runs_the_build_inputs_test() {
        let p = plan(&["gui-frontend/src/main.ts"]);
        assert!(p.frontend && !p.all);
        assert!(
            p.tests
                .contains("binary_id(=smeltr::integration) & test(/^gui_build_inputs_tests::/)")
        );
    }

    #[test]
    fn the_site_runs_everything_only_where_the_gui_or_build_rs_reads_it() {
        assert!(plan(&["website/src/lib/highlight/x.ts"]).all);
        let p = plan(&["website/src/routes/a.svelte"]);
        assert!(p.website && p.docs && !p.all);
        assert!(
            p.tests
                .contains("binary_id(=smeltr::integration) & test(/^website_reference_tests::/)")
        );
    }

    #[test]
    fn the_manifests_run_every_rust_test_and_deny() {
        let p = plan(&["Cargo.lock"]);
        assert!(p.rust_all && p.deny && p.clippy);
        assert_eq!(p.jobs()[0], ["check-rust"]);
    }

    #[test]
    fn a_tool_picks_its_own_tests() {
        let p = plan(&["tools/citations/src/lib.rs"]);
        assert_eq!(p.packages.into_iter().collect::<Vec<_>>(), ["citations"]);
        assert!(p.clippy && p.tests.is_empty() && !p.all);
    }

    #[test]
    fn a_file_nothing_places_runs_everything() {
        assert!(plan(&["src/io/kafka.rs", "justfile"]).all);
        assert!(plan(&["compose.live.yaml"]).all);
    }
}
