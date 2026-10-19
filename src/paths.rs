//! Absolute paths other programs accept. On Windows `std::fs::canonicalize` returns a verbatim path, `\\?\C:\...`,
//! which git reads as a UNC host named `?` and refuses, and which reads badly in a committed bundle.

use std::path::{Path, PathBuf};

/// `canonicalize`, minus the Windows verbatim prefix. Identical to `canonicalize` everywhere else.
pub fn canonicalize_portable(path: &Path) -> std::io::Result<PathBuf> {
    let real = path.canonicalize()?;
    Ok(match real.to_str().and_then(strip_verbatim) {
        Some(stripped) => PathBuf::from(stripped),
        None => real,
    })
}

/// `\\?\C:\x` -> `C:\x`, `\\?\UNC\server\share` -> `\\server\share`. `None` for anything else, including a verbatim
/// path in neither shape, which has no shorter spelling.
fn strip_verbatim(s: &str) -> Option<String> {
    let rest = s.strip_prefix(r"\\?\")?;
    if let Some(unc) = rest.strip_prefix(r"UNC\") {
        return Some(format!(r"\\{unc}"));
    }
    // `\\?\Volume{...}` is also verbatim and has no drive letter.
    let b = rest.as_bytes();
    (b.first().is_some_and(u8::is_ascii_alphabetic) && b.get(1) == Some(&b':') && b.get(2) == Some(&b'\\'))
        .then(|| rest.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_verbatim_drive_path_loses_its_prefix() {
        assert_eq!(
            strip_verbatim(r"\\?\C:\Users\John\git\smeltr").as_deref(),
            Some(r"C:\Users\John\git\smeltr")
        );
        assert_eq!(strip_verbatim(r"\\?\D:\").as_deref(), Some(r"D:\"));
    }

    #[test]
    fn a_verbatim_unc_path_becomes_an_ordinary_unc_path() {
        assert_eq!(
            strip_verbatim(r"\\?\UNC\fileserver\configs\smeltr").as_deref(),
            Some(r"\\fileserver\configs\smeltr")
        );
    }

    #[test]
    fn a_path_with_no_shorter_spelling_is_left_alone() {
        assert_eq!(
            strip_verbatim(r"\\?\Volume{b75e2c83-0000-0000-0000-602f00000000}\data"),
            None
        );
        assert_eq!(strip_verbatim(r"\\?\C-no-colon\x"), None);
    }

    #[test]
    fn an_ordinary_path_is_unchanged() {
        assert_eq!(strip_verbatim("/home/user/smeltr"), None);
        assert_eq!(strip_verbatim(r"C:\Users\John"), None);
        assert_eq!(strip_verbatim(r"\\fileserver\share"), None);
        assert_eq!(strip_verbatim(""), None);
    }

    #[test]
    fn it_resolves_a_real_directory() {
        let dir = std::env::temp_dir();
        let ours = canonicalize_portable(&dir).unwrap();
        assert!(ours.is_absolute());
        assert!(!ours.to_string_lossy().starts_with(r"\\?\"));
    }
}
