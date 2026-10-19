//! The lookup store: where a lookup file's bytes live, beside the repository whose commits name them.
//!
//! A commit holds a [`Pointer`] at a lookup file's path, never the bytes. A lookup file can be gigabytes and
//! replaced daily, and git keeps every version it is ever given on every host that clones it. The store keeps a file
//! under its SHA-256, so a name is never reused for other bytes, and a plan maps the file rather than reading it: a
//! host holds one copy in its page cache however many plans, previews and deployments name it.
//!
//! The leader's store holds what uploads put there, kept by `leader::lookups`' retention. A worker's holds what its
//! own deployment names, fetched from the leader and checked against the pointer.

use std::io::Write;
use std::path::{Path, PathBuf};

use super::LookupError;

/// The store's folder inside a repository's git directory. Git leaves a folder it does not know alone, and neither
/// clones nor fetches carry it.
pub const STORE_DIR: &str = "lookup-store";

/// What a commit holds at a lookup file's path: the Git LFS pointer format, so that git tooling shows it as what it
/// is. Smeltr does not speak the LFS protocol.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Pointer {
    /// The SHA-256 of the bytes, in lowercase hex.
    pub oid: String,
    pub size: u64,
}

impl Pointer {
    /// The pointer `text` is, or `None` when it is not one.
    pub fn parse(text: &[u8]) -> Option<Pointer> {
        let text = std::str::from_utf8(text).ok()?;
        let mut lines = text.lines();
        lines.next()?.strip_prefix("version https://git-lfs.github.com/spec/")?;
        let oid = lines.next()?.strip_prefix("oid sha256:")?;
        let size = lines.next()?.strip_prefix("size ")?.parse().ok()?;
        is_oid(oid).then(|| Pointer {
            oid: oid.to_string(),
            size,
        })
    }

    pub fn text(&self) -> String {
        format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize {}\n",
            self.oid, self.size
        )
    }
}

/// Whether `oid` is a SHA-256 in lowercase hex: what may name a file in the store, and nothing else may.
pub fn is_oid(oid: &str) -> bool {
    oid.len() == 64 && oid.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Where the store of the repository at `repo` keeps `oid`.
pub fn path_of(repo: &Path, oid: &str) -> PathBuf {
    repo.join(STORE_DIR).join(oid)
}

/// The file at `path`, mapped read-only. On Linux its pages are read in now, so the first batch to reach it takes no
/// faults; elsewhere they fault in as rows reach them.
pub fn map(path: &Path) -> std::io::Result<memmap2::Mmap> {
    let file = std::fs::File::open(path)?;
    // SAFETY: a mapped file must not change while mapped. A store file is written under a temporary name and renamed
    // to its hash, and nothing writes one again; a directory bundle's file is its author's, as any input file is.
    unsafe { memmap2::MmapOptions::new().populate().map(&file) }
}

/// The file at `path`, copied: for a file its author may change while a plan holds it, which a map must not see.
pub fn copy(path: &Path) -> std::io::Result<memmap2::Mmap> {
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() {
        // Windows refuses to make an empty anonymous map read-only.
        // SAFETY: a map of length zero reads no byte of the file, whatever is written to it later.
        return unsafe { memmap2::MmapOptions::new().len(0).map(&std::fs::File::open(path)?) };
    }
    let mut copy = memmap2::MmapMut::map_anon(bytes.len())?;
    copy.copy_from_slice(&bytes);
    copy.make_read_only()
}

/// The pointer `blob`, the file committed at `named`'s path, is. A lookup file is committed as a pointer, by an upload
/// or by git-lfs: committed as its bytes it would reach every clone of the repository, every worker's included.
pub fn pointer_in(blob: &[u8], named: &str) -> Result<Pointer, LookupError> {
    Pointer::parse(blob).ok_or_else(|| {
        LookupError(format!(
            "lookup file `{named}` is committed as its bytes, not as a Git LFS pointer. Commit it with git-lfs, or \
             upload it through Smeltr"
        ))
    })
}

/// The largest lookup file the store takes, uploaded or fetched.
pub const MAX_FILE_BYTES: u64 = 2 << 30;

/// The ones of `pointers` that `repo`'s store lacks.
pub fn missing<'a>(repo: &Path, pointers: impl IntoIterator<Item = &'a Pointer>) -> Vec<&'a Pointer> {
    pointers
        .into_iter()
        .filter(|pointer| !path_of(repo, &pointer.oid).exists())
        .collect()
}

/// Fetch into `repo`'s store every one of `pointers` it lacks, each read from what `fetch` answers for it and checked
/// against its pointer.
pub fn fetch_missing<'a, R: std::io::Read>(
    repo: &Path,
    pointers: impl IntoIterator<Item = &'a Pointer>,
    mut fetch: impl FnMut(&Pointer) -> Result<R, String>,
) -> Result<(), String> {
    for pointer in missing(repo, pointers) {
        let failed = |e: String| format!("could not fetch lookup file sha256 {}: {e}", pointer.oid);
        let mut body = fetch(pointer).map_err(failed)?;
        let mut incoming = Incoming::new(repo).map_err(|e| failed(e.to_string()))?;
        // Bounded by the pointer: bytes past its size cannot hash to it, and would only fill the disk first.
        std::io::copy(&mut std::io::Read::take(&mut body, pointer.size + 1), &mut incoming)
            .map_err(|e| failed(e.to_string()))?;
        incoming.finish(Some(pointer)).map_err(|e| failed(e.0))?;
    }
    Ok(())
}

/// A file arriving in the store: written under a temporary name, hashed as it is written, and given its name only
/// once it is whole. One that is dropped unfinished leaves nothing behind.
pub struct Incoming {
    dir: PathBuf,
    temp: PathBuf,
    file: Option<std::fs::File>,
    digest: aws_lc_rs::digest::Context,
    size: u64,
}

impl Incoming {
    pub fn new(repo: &Path) -> std::io::Result<Incoming> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = repo.join(STORE_DIR);
        std::fs::create_dir_all(&dir)?;
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp = dir.join(format!("{}{n}", incoming_prefix(std::process::id())));
        Ok(Incoming {
            file: Some(std::fs::File::create(&temp)?),
            dir,
            temp,
            digest: aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256),
            size: 0,
        })
    }

    /// Name the file by its hash. `expected` is the pointer it was fetched for, refused if the bytes are not those.
    /// Bytes the store already holds are the same file, so the copy just written is let go.
    pub fn finish(mut self, expected: Option<&Pointer>) -> Result<Pointer, LookupError> {
        let failed = |e: std::io::Error| LookupError(format!("couldn't store a lookup file: {e}"));
        let file = self.file.take().expect("finished once");
        file.sync_all().map_err(failed)?;
        drop(file);
        let digest = std::mem::replace(
            &mut self.digest,
            aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256),
        );
        let oid: String = digest.finish().as_ref().iter().map(|b| format!("{b:02x}")).collect();
        let pointer = Pointer { oid, size: self.size };
        if let Some(expected) = expected
            && expected != &pointer
        {
            return Err(LookupError(format!(
                "a lookup file fetched as sha256 {} ({} bytes) arrived as sha256 {} ({} bytes)",
                expected.oid, expected.size, pointer.oid, pointer.size
            )));
        }
        let at = self.dir.join(&pointer.oid);
        if at.exists() {
            return Ok(pointer);
        }
        match std::fs::rename(&self.temp, &at) {
            Ok(()) => Ok(pointer),
            // A concurrent arrival of the same bytes got there first, and Windows will not replace a file it maps.
            Err(_) if at.exists() => Ok(pointer),
            Err(e) => Err(failed(e)),
        }
    }
}

impl Incoming {
    /// Name the file `name`, for a file derived from a stored one whose name says what from, and map it. A file of
    /// that name is replaced: it is being written again because it did not read back.
    pub fn finish_as(mut self, name: &str) -> std::io::Result<memmap2::Mmap> {
        drop(self.file.take());
        let at = self.dir.join(name);
        std::fs::rename(&self.temp, &at)?;
        map(&at)
    }
}

/// `bytes` put in a store as the file at `at`, a path inside it, and mapped.
pub fn derive(at: &Path, bytes: &[u8]) -> std::io::Result<memmap2::Mmap> {
    let (Some(repo), Some(name)) = (
        at.parent().and_then(Path::parent),
        at.file_name().and_then(|n| n.to_str()),
    ) else {
        return Err(std::io::Error::other("a derived file is named inside a store"));
    };
    let mut incoming = Incoming::new(repo)?;
    incoming.write_all(bytes)?;
    incoming.finish_as(name)
}

/// How the temporary files of the process `pid` are named.
pub fn incoming_prefix(pid: u32) -> String {
    format!(".incoming-{pid}-")
}

impl Write for Incoming {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.file.as_mut().expect("written before finish").write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.size += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.as_mut().expect("written before finish").flush()
    }
}

impl Drop for Incoming {
    fn drop(&mut self) {
        self.file.take();
        let _ = std::fs::remove_file(&self.temp);
    }
}

/// Remove every file in `repo`'s store but those `keep` names. A file that will not go is left for the next call:
/// Windows refuses to delete a file something still maps.
pub fn keep_only(repo: &Path, keep: &std::collections::HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(repo.join(STORE_DIR)) else {
        return;
    };
    for entry in entries.flatten() {
        // A file derived from a stored one is named for it, `<oid>.<what>`, and goes with it.
        let oid = |name: &str| name.split('.').next().map(str::to_string);
        if !entry
            .file_name()
            .to_str()
            .and_then(oid)
            .is_some_and(|oid| keep.contains(&oid))
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Remove `oid` from `repo`'s store, and then every file derived from it. Derived files outlive a refused
/// delete of the stored one, so it can be tried again; a derived file that will not go is an orphan, which
/// `is_orphan` names.
pub fn remove(repo: &Path, oid: &str) -> std::io::Result<()> {
    std::fs::remove_file(path_of(repo, oid))?;
    let derived = format!("{oid}.");
    for entry in std::fs::read_dir(repo.join(STORE_DIR))?.flatten() {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(&derived))
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(())
}

/// Hash every stored file in `repo`'s store and remove, with what was derived from it, each whose bytes are not
/// those its name says, or cannot be read. Answers each oid found damaged and whether it went; `fetch_missing` fetches
/// one that went again. Reads every byte the store holds, so it is for a host's start, before anything maps a file.
pub fn remove_damaged(repo: &Path) -> Vec<(String, std::io::Result<()>)> {
    let Ok(entries) = std::fs::read_dir(repo.join(STORE_DIR)) else {
        return Vec::new();
    };
    let mut damaged = Vec::new();
    for entry in entries.flatten() {
        let Some(oid) = entry
            .file_name()
            .to_str()
            .filter(|name| is_oid(name))
            .map(str::to_string)
        else {
            continue;
        };
        if hash_of(&entry.path()).is_ok_and(|hash| hash == oid) {
            continue;
        }
        let removed = remove(repo, &oid);
        damaged.push((oid, removed));
    }
    damaged
}

/// The SHA-256 of the file at `path`, in lowercase hex.
fn hash_of(path: &Path) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    let mut buf = vec![0; 1 << 20];
    loop {
        match std::io::Read::read(&mut file, &mut buf)? {
            0 => break,
            n => digest.update(&buf[..n]),
        }
    }
    Ok(digest.finish().as_ref().iter().map(|b| format!("{b:02x}")).collect())
}

/// Whether `name` in `repo`'s store is derived from a stored file that is gone.
pub fn is_orphan(repo: &Path, name: &str) -> bool {
    name.split_once('.')
        .is_some_and(|(oid, _)| is_oid(oid) && !path_of(repo, oid).exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-store-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn put(repo: &Path, bytes: &[u8], expected: Option<&Pointer>) -> Result<Pointer, LookupError> {
        let mut incoming = Incoming::new(repo).unwrap();
        incoming.write_all(bytes).unwrap();
        incoming.finish(expected)
    }

    #[test]
    fn a_pointer_reads_back_as_itself_and_nothing_else_reads_as_one() {
        let pointer = Pointer {
            oid: "a".repeat(64),
            size: 12,
        };
        assert_eq!(Pointer::parse(pointer.text().as_bytes()), Some(pointer));
        assert_eq!(Pointer::parse(b"ip,city\n10.0.0.0/8,lan\n"), None);
        let upper = format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize 1\n",
            "A".repeat(64)
        );
        assert_eq!(Pointer::parse(upper.as_bytes()), None, "an oid is lowercase hex");
    }

    #[test]
    fn a_stored_file_is_named_by_its_hash_and_maps_back_whole() {
        let repo = repo("named");
        let pointer = put(&repo, b"abc", None).unwrap();
        assert_eq!(
            pointer.oid,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(&map(&path_of(&repo, &pointer.oid)).unwrap()[..], b"abc");
        assert_eq!(
            std::fs::read_dir(repo.join(STORE_DIR)).unwrap().count(),
            1,
            "no temporary file is left"
        );
        std::fs::remove_dir_all(&repo).ok();
    }

    #[test]
    fn bytes_that_are_not_what_was_fetched_are_refused_and_not_kept() {
        let repo = repo("refused");
        let wanted = put(&repo, b"abc", None).unwrap();
        std::fs::remove_file(path_of(&repo, &wanted.oid)).unwrap();
        let err = put(&repo, b"abd", Some(&wanted)).unwrap_err();
        assert!(err.0.contains(&wanted.oid), "{err}");
        assert_eq!(std::fs::read_dir(repo.join(STORE_DIR)).unwrap().count(), 0);
        std::fs::remove_dir_all(&repo).ok();
    }

    #[test]
    fn a_file_committed_as_its_bytes_is_refused() {
        let pointer = Pointer {
            oid: "a".repeat(64),
            size: 3,
        };
        assert_eq!(pointer_in(pointer.text().as_bytes(), "t.csv").unwrap(), pointer);
        let err = pointer_in(b"ip,city\n", "t.csv").unwrap_err();
        assert!(err.0.contains("`t.csv`") && err.0.contains("git-lfs"), "{err}");
    }

    #[test]
    fn a_copy_holds_the_file_as_it_was() {
        let dir = repo("copy");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("t.csv");
        std::fs::write(&file, b"a,b\n").unwrap();
        let copy = copy(&file).unwrap();
        std::fs::write(&file, b"").unwrap();
        assert_eq!(&copy[..], b"a,b\n");
        assert!(super::copy(&file).unwrap().is_empty(), "an empty file copies");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_stored_file_whose_bytes_are_not_its_name_is_removed_with_what_was_derived_from_it() {
        let repo = repo("damaged");
        let good = put(&repo, b"a,b\n", None).unwrap();
        let bad = put(&repo, b"c,d\n", None).unwrap();
        let derived = path_of(&repo, &format!("{}.index-0", bad.oid));
        std::fs::write(&derived, b"").unwrap();
        let mut bytes = std::fs::read(path_of(&repo, &bad.oid)).unwrap();
        bytes[0] ^= 1;
        std::fs::write(path_of(&repo, &bad.oid), bytes).unwrap();

        let damaged = remove_damaged(&repo);
        assert_eq!(damaged.len(), 1);
        assert_eq!(damaged[0].0, bad.oid);
        assert!(damaged[0].1.is_ok());
        assert!(path_of(&repo, &good.oid).exists());
        assert!(!path_of(&repo, &bad.oid).exists());
        assert!(!derived.exists());
        assert_eq!(
            missing(&repo, [&good, &bad]),
            vec![&bad],
            "what was removed is fetched again"
        );
        assert!(
            remove_damaged(&repo).is_empty(),
            "a store that hashes true is left as it is"
        );
        std::fs::remove_dir_all(&repo).ok();
    }

    #[test]
    fn keep_only_removes_what_it_is_not_given() {
        let repo = repo("keep");
        let a = put(&repo, b"a", None).unwrap();
        let b = put(&repo, b"b", None).unwrap();
        let derived = |oid: &str| path_of(&repo, &format!("{oid}.index-0"));
        std::fs::write(derived(&a.oid), b"").unwrap();
        std::fs::write(derived(&b.oid), b"").unwrap();
        keep_only(&repo, &[a.oid.clone()].into());
        assert!(path_of(&repo, &a.oid).exists());
        assert!(derived(&a.oid).exists(), "a file derived from a kept one is kept");
        assert!(!path_of(&repo, &b.oid).exists());
        assert!(!derived(&b.oid).exists());
        std::fs::remove_dir_all(&repo).ok();
    }
}
