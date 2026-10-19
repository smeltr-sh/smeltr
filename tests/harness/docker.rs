//! What a live test that runs a sender in a container needs: the container and its scratch directory removed
//! however the test ends, a panic hook restored the same way, and the container's logs for a failure's message.

/// A container, removed when dropped, so a failing test leaves none running.
pub struct Container(pub String);

impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .output();
    }
}

/// A directory, removed when dropped, so a failing seed leaves none behind.
pub struct Scratch(pub std::path::PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Makes `dir` and everything directly in it readable by any user, for a container that runs as a user of its own.
/// Does nothing on Windows, which has no such modes.
pub fn share(dir: &std::path::Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |path: &std::path::Path| if path.is_dir() { 0o755 } else { 0o644 };
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode(&path))).unwrap();
        }
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// What a panic hook is, as `std::panic::set_hook` takes it.
pub type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>;

/// A panic hook, the one before it restored when dropped, however the test ends.
pub struct Hook(Option<PanicHook>);

impl Hook {
    pub fn set(hook: PanicHook) -> Hook {
        let previous = std::panic::take_hook();
        std::panic::set_hook(hook);
        Hook(Some(previous))
    }
}

impl Drop for Hook {
    fn drop(&mut self) {
        // The hook cannot be changed while a panic unwinds, and the test is failing anyway.
        if std::thread::panicking() {
            return;
        }
        if let Some(previous) = self.0.take() {
            std::panic::set_hook(previous);
        }
    }
}

/// The logs of container `name`, both streams.
pub fn container_logs(name: &str) -> String {
    let logs = std::process::Command::new("docker")
        .args(["logs", name])
        .output()
        .unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    )
}
