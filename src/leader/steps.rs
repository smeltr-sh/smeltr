//! The forward steps a leader's store takes from one build to the next, and which builds may open it after them.
//!
//! A store's version is how many of its steps it has had, so a rebuild that adds no step leaves it alone. Each step
//! is additive, which a build without it can still use, or breaking. The store keeps `min_reader`, the fewest steps
//! a build must know to use it, and only a breaking step raises it. That is what lets an operator roll back to an
//! older build whenever nothing since has broken the store (`docs/architecture/control-plane.md`, Upgrades).

use std::path::Path;

use rusqlite::{Connection, OptionalExtension};

/// Bring the SQLite file `conn` up to `steps`, one transaction per step, after checking this build may use it.
/// `store` names the file in every refusal.
///
/// A step is appended and never edited: a file that has had it keeps its number forever. A file stopped anywhere in
/// a step is left at the version before it, and the next start runs that step again.
pub fn upgrade_sqlite(conn: &mut Connection, store: &str, steps: &[&str]) -> Result<(), String> {
    let applied: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|e| format!("reading {store}'s version: {e}"))?;
    let applied = usize::try_from(applied).unwrap_or(usize::MAX);
    // Read failures are reported as they are: "move it aside" is only ever said of a file known to be pre-release.
    let read = |e: rusqlite::Error| format!("reading {store}'s min_reader: {e}");
    let pre_release = || {
        format!(
            "{store} was written by a smeltr from before the first release, which this one does not \
                 upgrade. Move it aside and start the leader again."
        )
    };
    let min_reader = if applied == 0 {
        // A file at version 0 is new only while it is empty: a pre-release one has tables and never set a version.
        let tables: i64 = conn
            .query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table'", [], |row| {
                row.get(0)
            })
            .map_err(read)?;
        if tables > 0 {
            return Err(pre_release());
        }
        0
    } else {
        let kept: bool = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'min_reader'",
                [],
                |row| row.get(0),
            )
            .map_err(read)?;
        let min_reader = if kept {
            conn.query_row("SELECT steps FROM min_reader", [], |row| row.get::<_, i64>(0))
                .optional()
                .map_err(read)?
        } else {
            None
        };
        min_reader.ok_or_else(pre_release)?
    };
    let min_reader =
        usize::try_from(min_reader).map_err(|_| format!("{store} records a min_reader of {min_reader}"))?;
    check(store, min_reader, steps.len())?;

    let failed = |e: rusqlite::Error| format!("upgrading {store}: {e}");
    for (i, step) in steps.iter().enumerate().skip(applied) {
        let version = i + 1;
        let tx = conn.transaction().map_err(failed)?;
        if i == 0 {
            tx.execute_batch("CREATE TABLE min_reader (steps INTEGER NOT NULL); INSERT INTO min_reader VALUES (0);")
                .map_err(failed)?;
        }
        tx.execute_batch(step).map_err(failed)?;
        // The first step is the floor: no build without it has ever read this file.
        if i == 0 || !is_additive(step) {
            tx.execute("UPDATE min_reader SET steps = ?1", [version as i64])
                .map_err(failed)?;
        }
        // `user_version` takes no parameter, and the value is this loop's own index rather than anything a caller
        // supplied.
        tx.pragma_update(None, "user_version", version as i64).map_err(failed)?;
        tx.commit().map_err(failed)?;
    }
    Ok(())
}

/// Whether a build that knows `known` steps may use a store whose `min_reader` is as given.
pub fn check(store: &str, min_reader: usize, known: usize) -> Result<(), String> {
    if min_reader > known {
        return Err(format!(
            "{store} needs a smeltr that knows {min_reader} of its steps, and this one knows {known}. A newer build \
             changed it in a way this one cannot read; run that build, or restore a backup taken before the upgrade."
        ));
    }
    Ok(())
}

/// Whether a build without `step` can still use the file after it: every statement adds a table, a plain index, or
/// a column that a row written without it can leave empty, and none of them constrains what an older build writes
/// or deletes. Anything else -- a drop, a rename, a rewrite of rows, a unique index, a check or a foreign key -- is
/// breaking, so a step is never trusted as additive on a guess.
///
/// A change to what stored values mean, with no change to the schema, is a step too: one made only of a comment
/// saying what changed, which counts as breaking.
pub fn is_additive(step: &str) -> bool {
    // What the statements say with comments dropped and every quoted string or name emptied, so neither can hide
    // or fake a keyword or a `;`.
    let mut code = String::new();
    let mut chars = step.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '-' if chars.peek() == Some(&'-') => {
                chars.by_ref().take_while(|&c| c != '\n').for_each(drop);
                code.push(' ');
            }
            '\'' | '"' | '`' | '[' => {
                let close = if c == '[' { ']' } else { c };
                chars.by_ref().take_while(|&q| q != close).for_each(drop);
                code.push_str("''");
            }
            '/' if chars.peek() == Some(&'*') => return false,
            _ => code.push(c),
        }
    }
    let statements: Vec<String> = code
        .split(';')
        .map(|statement| {
            statement
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_uppercase()
        })
        .filter(|statement| !statement.is_empty())
        .collect();
    let constrains = |statement: &str| ["CHECK", "REFERENCES", "UNIQUE"].iter().any(|c| statement.contains(c));
    !statements.is_empty()
        && statements.iter().all(|statement| {
            (statement.starts_with("CREATE TABLE ") && !statement.contains("REFERENCES"))
                || statement.starts_with("CREATE INDEX ")
                || (statement.starts_with("ALTER TABLE ")
                    && statement.contains(" ADD COLUMN ")
                    && !constrains(statement)
                    && (!statement.contains("NOT NULL") || statement.split(' ').any(|word| word == "DEFAULT")))
        })
}

/// One step of the config repository. Nothing like SQL says what it does, so its author says whether a build
/// without it can still use the repository after it.
pub struct RepoStep {
    /// The text of the file the step lives in, by `include_str!`, which `released-steps.txt` holds a hash of: a
    /// released step's code cannot change without the hash changing too.
    pub source: &'static str,
    /// Safe to run twice: a step and the version that records it cannot be written together, so a leader stopped
    /// between them runs the step again.
    pub run: fn(&Path) -> Result<(), String>,
    pub additive: bool,
}

/// Every step the config repository has taken, oldest first, each in a file of its own. Append; never edit one that
/// has shipped.
pub const REPO_STEPS: &[RepoStep] = &[];

/// Where the repository keeps how many steps it has had and its `min_reader`: its own git config, which is the
/// leader's alone and never part of history a remote could hold.
const REPO_VERSION_KEY: &str = "smeltr.steps";
const REPO_MIN_READER_KEY: &str = "smeltr.minReader";

/// Record that the repository just created at `repo_path` has every step there is.
pub fn stamp_new_repo(repo_path: &Path) -> Result<(), String> {
    let min_reader = REPO_STEPS.iter().rposition(|step| !step.additive).map_or(0, |i| i + 1);
    set_repo_key(repo_path, REPO_MIN_READER_KEY, min_reader)?;
    set_repo_key(repo_path, REPO_VERSION_KEY, REPO_STEPS.len())
}

/// Bring the config repository up to [`REPO_STEPS`], after checking this build may use it. A repository not created
/// yet has nothing to upgrade.
pub fn upgrade_repo(repo_path: &Path) -> Result<(), String> {
    upgrade_repo_with(repo_path, REPO_STEPS)
}

fn upgrade_repo_with(repo_path: &Path, steps: &[RepoStep]) -> Result<(), String> {
    if !super::git::repo_ready(repo_path) {
        return Ok(());
    }
    let store = repo_path.display().to_string();
    let repo = gix::open(repo_path).map_err(|e| format!("opening {store}: {e}"))?;
    // The repository's own file alone: a key in the user's or the system's git config, or the environment, would
    // otherwise stand in for one the repository never recorded.
    let config = gix::config::File::from_path_no_includes(repo_path.join("config"), gix::config::Source::Local)
        .map_err(|e| format!("reading {store}'s config: {e}"))?;
    // A value that is there and unreadable is reported as it is: only a repository with none is pre-release.
    let read = |key: &str| -> Result<Option<usize>, String> {
        let unreadable = || format!("{store} holds an unreadable {key}");
        config
            .integer(key)
            .map_err(|_| unreadable())?
            .map(|n| usize::try_from(n).map_err(|_| unreadable()))
            .transpose()
    };
    let (Some(applied), Some(min_reader)) = (read(REPO_VERSION_KEY)?, read(REPO_MIN_READER_KEY)?) else {
        // Created but stopped before it was stamped: it holds no ref at all, so it is as new as it looks.
        let empty = repo
            .references()
            .map_err(|e| e.to_string())
            .and_then(|refs| Ok(refs.all().map_err(|e| e.to_string())?.next().is_none()))
            .map_err(|e| format!("reading {store}'s refs: {e}"))?;
        if empty {
            return stamp_new_repo(repo_path);
        }
        return Err(format!(
            "{store} was written by a smeltr from before the first release, which this one does not upgrade. Move \
             it aside and start the leader again."
        ));
    };
    check(&store, min_reader, steps.len())?;
    for (i, step) in steps.iter().enumerate().skip(applied) {
        (step.run)(repo_path).map_err(|e| format!("upgrading {store}: {e}"))?;
        if !step.additive {
            set_repo_key(repo_path, REPO_MIN_READER_KEY, i + 1)?;
        }
        set_repo_key(repo_path, REPO_VERSION_KEY, i + 1)?;
    }
    Ok(())
}

fn set_repo_key(repo_path: &Path, key: &str, value: usize) -> Result<(), String> {
    let status = std::process::Command::new("git")
        .arg("--git-dir")
        .arg(repo_path)
        .args(["config", key, &value.to_string()])
        .status()
        .map_err(|e| format!("running git to set {key}: {e}"))?;
    if !status.success() {
        return Err(format!("git could not set {key} in {}: {status}", repo_path.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "CREATE TABLE t (a TEXT NOT NULL);";
    const ADD: &str = "-- a note; with a semicolon\nALTER TABLE t ADD COLUMN b INTEGER;";
    const BREAK: &str = "ALTER TABLE t RENAME COLUMN a TO c;";

    fn version_and_min_reader(conn: &Connection) -> (i64, i64) {
        let version = conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap();
        let min_reader = conn
            .query_row("SELECT steps FROM min_reader", [], |row| row.get(0))
            .unwrap();
        (version, min_reader)
    }

    #[test]
    fn additive_steps_are_told_from_breaking_ones() {
        assert!(is_additive(BASE));
        assert!(is_additive(ADD));
        assert!(is_additive("CREATE INDEX i ON t (a);"));
        assert!(is_additive("ALTER TABLE t ADD COLUMN k TEXT NOT NULL DEFAULT 'x';"));
        assert!(!is_additive(BREAK));
        assert!(!is_additive("ALTER TABLE t ADD COLUMN k TEXT NOT NULL;"));
        assert!(!is_additive("ALTER TABLE defaults ADD COLUMN k TEXT NOT NULL;"));
        assert!(!is_additive("CREATE UNIQUE INDEX i ON t (a);"));
        assert!(!is_additive("DELETE FROM t;"));
        assert!(!is_additive("CREATE TABLE u (x INTEGER); DROP TABLE t;"));
        assert!(!is_additive("-- users.auth_source may now hold 'oidc'"));
        assert!(!is_additive("CREATE TABLE u (x TEXT DEFAULT '--'); DROP TABLE t;"));
        assert!(!is_additive("ALTER TABLE t ADD COLUMN k TEXT CHECK (k <> 'x');"));
        assert!(!is_additive("ALTER TABLE t ADD COLUMN k TEXT REFERENCES u (id);"));
        assert!(!is_additive("CREATE TABLE u (id TEXT REFERENCES t (a));"));
        assert!(!is_additive("CREATE TABLE u (x TEXT DEFAULT ';DROP'); DROP TABLE t;"));
        assert!(is_additive(
            "ALTER TABLE t ADD COLUMN k TEXT DEFAULT 'a -- b; DROP TABLE t';"
        ));
        assert!(!is_additive("/* note */ CREATE TABLE u (x INTEGER);"));
        assert!(!is_additive("CREATE TABLE [u--x] (a INTEGER); DROP TABLE t;"));
    }

    #[test]
    fn only_a_breaking_step_raises_min_reader() {
        let mut conn = Connection::open_in_memory().unwrap();
        upgrade_sqlite(&mut conn, "t.db", &[BASE, ADD]).unwrap();
        assert_eq!(version_and_min_reader(&conn), (2, 1));
        upgrade_sqlite(&mut conn, "t.db", &[BASE, ADD, BREAK]).unwrap();
        assert_eq!(version_and_min_reader(&conn), (3, 3));
    }

    #[test]
    fn an_older_build_opens_a_store_only_additive_steps_have_passed() {
        let mut conn = Connection::open_in_memory().unwrap();
        upgrade_sqlite(&mut conn, "t.db", &[BASE, ADD]).unwrap();
        upgrade_sqlite(&mut conn, "t.db", &[BASE]).unwrap();
        assert_eq!(version_and_min_reader(&conn), (2, 1), "an older build moves nothing");

        upgrade_sqlite(&mut conn, "t.db", &[BASE, ADD, BREAK]).unwrap();
        let refused = upgrade_sqlite(&mut conn, "t.db", &[BASE, ADD]).unwrap_err();
        assert!(refused.contains("knows 3") && refused.contains("knows 2"), "{refused}");
    }

    #[test]
    fn a_store_from_before_the_first_release_is_refused() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(BASE).unwrap();
        conn.pragma_update(None, "user_version", 4).unwrap();
        let refused = upgrade_sqlite(&mut conn, "t.db", &[BASE]).unwrap_err();
        assert!(refused.contains("before the first release"), "{refused}");

        let mut unversioned = Connection::open_in_memory().unwrap();
        unversioned.execute_batch(BASE).unwrap();
        let refused = upgrade_sqlite(&mut unversioned, "t.db", &[BASE]).unwrap_err();
        assert!(refused.contains("before the first release"), "{refused}");
    }

    #[test]
    fn a_step_that_fails_leaves_the_version_and_the_file_as_they_were() {
        let mut conn = Connection::open_in_memory().unwrap();
        upgrade_sqlite(&mut conn, "t.db", &[BASE]).unwrap();
        let failing = "ALTER TABLE t ADD COLUMN z INTEGER; ALTER TABLE nowhere ADD COLUMN y INTEGER;";
        assert!(upgrade_sqlite(&mut conn, "t.db", &[BASE, failing]).is_err());
        assert_eq!(version_and_min_reader(&conn), (1, 1));
        let columns: i64 = conn
            .query_row("SELECT count(*) FROM pragma_table_info('t')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(columns, 1, "the failed step left nothing behind");
    }

    fn repo() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-repo-steps-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("config.git");
        crate::leader::git::init_bare_repo(&path).unwrap();
        path
    }

    fn repo_keys(path: &Path) -> (i64, i64) {
        let repo = gix::open(path).unwrap();
        let config = repo.config_snapshot();
        (
            config.integer(REPO_VERSION_KEY).unwrap(),
            config.integer(REPO_MIN_READER_KEY).unwrap(),
        )
    }

    /// The repository as a build from before the first release left it: no steps recorded.
    fn unstamp(path: &Path) {
        let config = path.join("config");
        let kept: String = std::fs::read_to_string(&config)
            .unwrap()
            .lines()
            .take_while(|line| !line.starts_with("[smeltr]"))
            .map(|line| format!("{line}\n"))
            .collect();
        std::fs::write(&config, kept).unwrap();
    }

    fn touch(path: &Path) -> Result<(), String> {
        let mark = path.join("touched");
        let count = std::fs::read_to_string(&mark).unwrap_or_default().len();
        std::fs::write(&mark, "x".repeat(count + 1)).map_err(|e| e.to_string())
    }

    const ADDITIVE: RepoStep = RepoStep {
        source: "touch",
        run: touch,
        additive: true,
    };
    const BREAKING: RepoStep = RepoStep {
        source: "touch",
        run: touch,
        additive: false,
    };

    #[test]
    fn a_new_repository_has_every_step_and_opens() {
        let path = repo();
        assert_eq!(repo_keys(&path), (REPO_STEPS.len() as i64, 0));
        upgrade_repo(&path).unwrap();
    }

    #[test]
    fn repository_steps_run_once_and_only_a_breaking_one_raises_min_reader() {
        let path = repo();
        upgrade_repo_with(&path, &[ADDITIVE]).unwrap();
        upgrade_repo_with(&path, &[ADDITIVE]).unwrap();
        assert_eq!(repo_keys(&path), (1, 0));
        assert_eq!(
            std::fs::read_to_string(path.join("touched")).unwrap(),
            "x",
            "the step ran once"
        );

        upgrade_repo_with(&path, &[]).expect("an older build opens a repository only additive steps have passed");
        upgrade_repo_with(&path, &[ADDITIVE, BREAKING]).unwrap();
        assert_eq!(repo_keys(&path), (2, 2));
        let refused = upgrade_repo_with(&path, &[ADDITIVE]).unwrap_err();
        assert!(refused.contains("knows 2") && refused.contains("knows 1"), "{refused}");
    }

    #[test]
    fn a_repository_from_before_the_first_release_is_refused() {
        let path = repo();
        crate::leader::git::ensure_main_commit(&path).unwrap();
        unstamp(&path);

        let refused = upgrade_repo(&path).unwrap_err();
        assert!(refused.contains("before the first release"), "{refused}");
    }

    #[test]
    fn an_unreadable_stamp_is_reported_as_such() {
        let path = repo();
        set_repo_key(&path, REPO_VERSION_KEY, 0).unwrap();
        let config = path.join("config");
        let text = std::fs::read_to_string(&config)
            .unwrap()
            .replace("steps = 0", "steps = many");
        std::fs::write(&config, text).unwrap();
        let refused = upgrade_repo(&path).unwrap_err();
        assert!(refused.contains("unreadable"), "{refused}");
    }

    #[test]
    fn a_repository_stopped_before_its_stamp_is_stamped_when_it_holds_nothing() {
        let path = repo();
        unstamp(&path);

        upgrade_repo(&path).unwrap();
        assert_eq!(repo_keys(&path), (REPO_STEPS.len() as i64, 0));
    }
}
