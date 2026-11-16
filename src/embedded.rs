//! The frontend builds baked into the binary by build.rs, each named by its directory in `tools/frontends.json`.

include!(concat!(env!("OUT_DIR"), "/embedded.rs"));

/// The file at `path` in the output of the frontend built in `dir`. Found by a scan, which is fast enough for the few
/// hundred files of the GUI.
pub fn file(dir: &str, path: &str) -> Option<&'static [u8]> {
    files(dir).find(|(name, _)| *name == path).map(|(_, bytes)| bytes)
}

/// Every file in the output of the frontend built in `dir`, by its path there.
pub fn files(dir: &str) -> impl Iterator<Item = (&'static str, &'static [u8])> {
    FILES
        .iter()
        .filter(move |(d, _, _)| *d == dir)
        .map(|(_, path, bytes)| (*path, *bytes))
}
