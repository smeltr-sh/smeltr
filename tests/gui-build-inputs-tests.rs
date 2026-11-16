//! The website files the GUI imports reach every build of it. Three lists name them by hand: Vite's `fs.allow`
//! (`gui-frontend/vite.config.ts`), what the `Dockerfile` copies into the frontend stage, and what `build.rs` watches
//! to tell that `dist` is stale; and `.dockerignore` must not keep them out of the image build. A file missing from one
//! builds everywhere else and breaks that one build.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use regex::Regex;

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &str) -> String {
    std::fs::read_to_string(root().join(path)).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

/// `path` with its `..` resolved, `/`-separated.
fn normalize(path: &Path) -> String {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            c => out.push(c),
        }
    }
    out.to_string_lossy().replace('\\', "/")
}

/// Every `website/` file the GUI's own sources import, from the repository root. Its tests run only from the
/// checkout, so their imports are left out.
fn imports() -> BTreeSet<String> {
    let quoted = Regex::new(r#"['"]((?:\.\./)+website/[^'"]+)['"]"#).unwrap();
    let mut out = BTreeSet::new();
    let mut dirs = vec![root().join("gui-frontend/src")];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if path.is_dir() {
                dirs.push(path);
            } else if !name.contains(".test.") {
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                for m in quoted.captures_iter(&text) {
                    let rel = dir.strip_prefix(root()).unwrap().join(&m[1]);
                    out.insert(normalize(&rel));
                }
            }
        }
    }
    out
}

/// The `website/` paths `text` names in quotes, with the prefix before `website/` dropped.
fn quoted_site_paths(text: &str) -> BTreeSet<String> {
    let quoted = Regex::new(r#"['"](?:\.\./)*(website/[^'"]+)['"]"#).unwrap();
    quoted
        .captures_iter(text)
        .map(|m| m[1].trim_end_matches('/').to_string())
        .collect()
}

/// What `.dockerignore` keeps out of `website/`.
fn dockerignored() -> Vec<String> {
    read(".dockerignore")
        .lines()
        .map(|l| l.trim().trim_end_matches('/'))
        .filter(|l| *l == "website" || l.starts_with("website/"))
        .map(str::to_string)
        .collect()
}

/// What the `Dockerfile`'s frontend stage copies from `website/`, each checked to land where the GUI's relative imports
/// look for it.
fn dockerfile() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut frontend = false;
    for line in read("Dockerfile").lines() {
        if line.starts_with("FROM ") {
            frontend = line.ends_with(" AS frontend");
        }
        let Some(args) = line.trim().strip_prefix("COPY ").filter(|_| frontend) else {
            continue;
        };
        let args: Vec<&str> = args.split_whitespace().collect();
        let Some((dest, sources)) = args.split_last() else {
            continue;
        };
        for src in sources.iter().filter(|s| s.starts_with("website/")) {
            // A directory source copies its contents into `dest`; a file source into a directory `dest` keeps its name.
            let landed = if src.ends_with('/') || !dest.ends_with('/') {
                dest.to_string()
            } else {
                format!("{dest}{}", src.rsplit('/').next().unwrap())
            };
            let src = src.trim_end_matches('/');
            assert_eq!(
                landed.trim_end_matches('/'),
                format!("/src/{src}"),
                "the Dockerfile copies {src} where the GUI does not look for it"
            );
            out.insert(src.to_string());
        }
    }
    out
}

#[test]
fn every_build_of_the_gui_carries_the_website_files_it_imports() {
    let imports = imports();
    assert!(!imports.is_empty(), "found no website imports in gui-frontend/src");
    let lists = [
        (
            "gui-frontend/vite.config.ts fs.allow",
            quoted_site_paths(&read("gui-frontend/vite.config.ts")),
        ),
        ("the Dockerfile's COPY", dockerfile()),
        ("build.rs watch", quoted_site_paths(&read("build.rs"))),
    ];
    let mut missing = Vec::new();
    let ignored = dockerignored();
    for import in &imports {
        if let Some(e) = ignored
            .iter()
            .find(|e| import == *e || import.starts_with(&format!("{e}/")))
        {
            missing.push(format!(
                "{import} is kept out of the image build by .dockerignore's {e}"
            ));
        }
        for (name, list) in &lists {
            if !list.iter().any(|e| import == e || import.starts_with(&format!("{e}/"))) {
                missing.push(format!("{import} is missing from {name}"));
            }
        }
    }
    assert!(missing.is_empty(), "{}", missing.join("\n"));
}
