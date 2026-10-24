//! Files only their owner may read: the leader's secrets encryption key, and the private keys behind its certificate
//! authority.
//!
//! On Unix that is a file mode. On Windows it is the parent directory's ACL; see [`check`].

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// A directory only its owner may enter. Succeeds on one that already exists, whatever its mode.
#[cfg(unix)]
pub fn create_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    fs::DirBuilder::new().recursive(true).mode(0o700).create(path)
}

/// Inherits the parent's ACL, which is the whole of its protection on Windows. See [`check`].
#[cfg(not(unix))]
pub fn create_dir(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)
}

/// `create_new` so two callers racing to write the first one cannot both win.
#[cfg(unix)]
pub fn create(path: &Path) -> std::io::Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

/// `create_new` still holds the race; the mode does not exist to set. See [`check`].
#[cfg(not(unix))]
pub fn create(path: &Path) -> std::io::Result<fs::File> {
    fs::OpenOptions::new().write(true).create_new(true).open(path)
}

/// Puts `contents` at `path`, owner-only, replacing whatever is there.
///
/// Written beside and renamed over, so an interrupted write leaves the previous file rather than half of this one,
/// and the mode travels with the rename: the contents are never briefly readable under the real name.
pub fn replace(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let mut beside = path.as_os_str().to_owned();
    beside.push(".new");
    let beside = PathBuf::from(beside);

    // One left by an interrupted write, which is the only way one exists: the leader holds its data directory
    // alone (`leader::lock`), so nobody else is mid-write here.
    let _ = fs::remove_file(&beside);

    let mut file = create(&beside)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&beside, path)?;
    sync_parent(path)
}

/// Persists the directory entry a file was just created or renamed under.
///
/// A file's own sync says nothing about whether its name is durable. Without this a crash can leave contents that
/// reached the disk under a name that never did, and a caller that writes two files in an order it relies on gets
/// no ordering at all.
#[cfg(unix)]
pub fn sync_parent(path: &Path) -> std::io::Result<()> {
    match path.parent() {
        Some(dir) => fs::File::open(dir)?.sync_all(),
        None => Ok(()),
    }
}

/// **On Windows nothing is synced.**
///
/// A directory cannot be opened as a file, so `std::fs` offers no handle to sync one through. Durability of a name
/// there is whatever the filesystem gives without asking.
#[cfg(not(unix))]
pub fn sync_parent(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Refuses a file anyone but its owner can read. `what` names what would leak, for the operator reading it.
#[cfg(unix)]
pub fn check(path: &Path, what: &str) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(format!(
            "{} is mode {:03o}, which lets someone other than its owner read {what}. `chmod 600` it.",
            path.display(),
            mode & 0o777
        ));
    }
    Ok(())
}

/// **On Windows only existence is checked.**
///
/// A private key's access is whatever its directory's ACL grants. `std::fs` cannot read a DACL, so there is
/// nothing here to check it against, and a key readable by every local account would pass unremarked. The fix is
/// the same one `sqlite_store::make_private` needs and is not written yet. A file that is not there at all is
/// still refused: that is not an ACL question, and a caller reading `Ok` from a key it cannot open is a bug no
/// platform should have.
#[cfg(not(unix))]
pub fn check(path: &Path, _what: &str) -> Result<(), String> {
    fs::metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-private-file-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        create_dir(&dir).expect("a scratch directory");
        dir
    }

    #[test]
    fn replacing_a_file_keeps_it_owner_only() {
        let dir = scratch("replace");
        let path = dir.join("key.pem");

        replace(&path, b"first").expect("write it");
        replace(&path, b"second").expect("write it again");

        assert_eq!(fs::read(&path).unwrap(), b"second");
        assert!(check(&path, "a key").is_ok(), "a replaced file is still owner-only");
    }

    #[test]
    fn replacing_a_file_leaves_nothing_beside_it() {
        let dir = scratch("beside");
        let path = dir.join("key.pem");

        replace(&path, b"once").expect("write it");

        let left: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(left, vec!["key.pem".to_string()]);
    }

    /// A crash can leave one behind. If it blocked the next write, that key could never be written again.
    #[test]
    fn a_leftover_from_an_interrupted_write_does_not_block_the_next() {
        let dir = scratch("interrupted");
        let path = dir.join("key.pem");
        replace(&path, b"previous").expect("write it");

        // What an interrupted `replace` leaves behind: the temporary file, written and never renamed.
        let mut beside = path.as_os_str().to_owned();
        beside.push(".new");
        fs::write(PathBuf::from(beside), b"half").expect("plant one");

        replace(&path, b"next").expect("the leftover does not block the next write");
        assert_eq!(fs::read(&path).unwrap(), b"next");
    }

    /// A process that dies inside `replace` leaves the old key or the new one, whole. What a reader finds under the
    /// name at any moment is what such a death would leave, so every read must be the old contents or the new.
    ///
    /// A reader sees the page cache. The syncs that carry the file through a power loss go untested here.
    ///
    /// Unix only, where a rename over a name is atomic.
    #[cfg(unix)]
    #[test]
    fn the_name_always_holds_a_whole_file_while_it_is_replaced() {
        let dir = scratch("whole");
        let path = dir.join("key.pem");
        let (old, new) = (vec![b'o'; 1 << 20], vec![b'n'; 1 << 20]);
        replace(&path, &old).expect("write it");

        std::thread::scope(|s| {
            let writer = s.spawn(|| -> std::io::Result<()> {
                for i in 0..20 {
                    replace(&path, if i % 2 == 0 { &new } else { &old })?;
                }
                Ok(())
            });
            // Until the writer ends, by return or by panic: a writer that panics must not leave this spinning.
            while !writer.is_finished() {
                let read = fs::read(&path).expect("the name always holds a file");
                assert!(read == old || read == new, "a torn read of {} bytes", read.len());
            }
            writer.join().unwrap().expect("replace it");
        });
    }

    #[cfg(unix)]
    #[test]
    fn a_readable_key_is_refused_and_says_what_to_do() {
        use std::os::unix::fs::PermissionsExt;

        let dir = scratch("mode");
        let path = dir.join("key.pem");
        replace(&path, b"secret").expect("write it");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("open it up");

        let refused = check(&path, "the authority's private key").expect_err("mode 644 is refused");
        assert!(refused.contains("644"), "{refused}");
        assert!(refused.contains("chmod 600"), "{refused}");
    }
}
