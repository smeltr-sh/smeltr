//! The frontend builds baked into the binary by build.rs, each by its name in `tools/frontends.json`.

include!(concat!(env!("OUT_DIR"), "/embedded.rs"));

/// The file at `path` in the output of the frontend `name`. Found by a scan, which is fast enough for the few
/// hundred files of the GUI.
pub fn file(name: &str, path: &str) -> Option<&'static [u8]> {
    files(name).find(|(file, _)| *file == path).map(|(_, bytes)| bytes)
}

/// Every file in the output of the frontend `name`, by its path there.
pub fn files(name: &str) -> impl Iterator<Item = (&'static str, &'static [u8])> {
    FILES
        .iter()
        .filter(move |(n, _, _)| *n == name)
        .map(|(_, path, bytes)| (*path, *bytes))
}
