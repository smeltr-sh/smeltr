//! One leader per data directory.
//!
//! Two leaders on one directory share a SQLite file, a git repository and a set of refs, and race on all three:
//! the symptom is a worker group that vanishes, or an account created by one process and invisible to the other.
//! Nothing detected it before, so the first sign was the damage.
//!
//! The lock is an advisory lock the operating system holds on an open file, released when the process exits --
//! including when it crashes, which is what a lock file holding a pid cannot manage. It grants nothing and
//! protects nothing from a process that never asks; it stops the ordinary mistake of starting a second leader.

use std::path::Path;

/// The lock, for as long as it is held. Dropping it releases the file and lets another leader start, so the leader
/// keeps it for the life of the process.
pub struct DataDirLock {
    // Held for its side effect: closing the file is what releases the lock. Nothing reads or writes it.
    _file: std::fs::File,
}

/// An advisory lock on one file, held until dropped.
pub struct Held {
    _file: std::fs::File,
}

/// Take an advisory lock on `path`, creating it if it is not there. `None` is a lock somebody else holds.
///
/// For a caller that owns a different invariant from this module's and says so in its own words. What both get
/// from the operating system is the same: a lock released when the holding process exits, crash included, which
/// is what a file holding a pid cannot manage.
pub fn hold(path: &Path) -> std::io::Result<Option<Held>> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(Held { _file: file })),
        // Somebody has it, which is an answer. Anything else is the lock service failing -- a filesystem that
        // does not offer one, or one out of room -- and answering `None` there would say a holder exists that
        // does not, refusing every retry for a reason nobody could find.
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(e)) => Err(e),
    }
}

/// The file inside the data directory whose lock stands for the directory. Its contents are never read.
const LOCK_FILE: &str = "leader.lock";

/// Take the lock, or say who has it.
///
/// `Err` carries a sentence for an operator rather than an error to match on: there is one thing to do about a
/// held lock, and it is to stop the other leader.
pub fn acquire(data_dir: &Path) -> Result<DataDirLock, String> {
    let path = data_dir.join(LOCK_FILE);
    match hold(&path).map_err(|e| format!("{}: {e}", path.display()))? {
        Some(Held { _file }) => Ok(DataDirLock { _file }),
        None => Err(format!(
            "another smeltr leader is already using {}. One leader owns a data directory: two share a database \
             and a git repository and corrupt both. Stop the running one, or point this process at its own \
             SMELTR_LEADER_DATA_DIR.",
            data_dir.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a runtime holds so a replacement cannot read the same sources beside it: the second asker is refused
    /// while the first still has it, and gets it once the first is gone.
    /// A lock nobody holds and a lock service that failed are different answers. Saying the first when the second
    /// happened invents a holder, and every retry is then refused for a reason no operator can find.
    #[test]
    fn a_lock_that_cannot_be_taken_at_all_is_an_error_rather_than_a_holder() {
        let missing = std::env::temp_dir().join("smeltr-no-such-dir-for-locks").join("x.lock");
        let answered = hold(&missing);
        assert!(
            answered.is_err(),
            "a directory that is not there read as another process holding the lock"
        );
    }

    #[test]
    fn a_held_lock_refuses_a_second_holder_until_it_is_released() {
        let dir = std::env::temp_dir().join(format!("smeltr-hold-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.lock");

        let first = hold(&path).unwrap().expect("nothing holds it yet");
        assert!(hold(&path).unwrap().is_none(), "two holders at once");
        drop(first);
        assert!(hold(&path).unwrap().is_some(), "a released lock was not offered again");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn scratch(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-lock-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Releasing on drop is what lets a restarted leader start: the file outlives the process that made it, and a
    /// lock that outlived it too would need clearing by hand after every crash.
    #[test]
    fn dropping_the_lock_frees_the_directory() {
        let dir = scratch("released");
        drop(acquire(&dir).unwrap());
        assert!(acquire(&dir).is_ok());
    }

    /// An operator starting a second leader reads this refusal and nothing else, so it names the directory and the
    /// two ways out. A second lock on the same file conflicts within one process too, so one process can show it.
    #[test]
    fn a_second_leader_is_told_which_directory_and_what_to_do() {
        let dir = scratch("second");
        let _held = acquire(&dir).unwrap();

        let refused = acquire(&dir)
            .err()
            .expect("a second leader on one directory is refused");
        assert!(refused.contains(&dir.display().to_string()), "{refused}");
        assert!(refused.to_lowercase().contains("stop"), "{refused}");
        assert!(refused.contains("SMELTR_LEADER_DATA_DIR"), "{refused}");
    }
}
