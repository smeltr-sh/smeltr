//! The locks smeltr takes. A thread that panics while it holds one leaves the lock poisoned; these locks hand the
//! data over all the same. What a panic means is decided once, by the thread it happened on: a caught panic fails
//! its one batch or request, a panic on a thread started with [`spawn`] ends the process, so nothing runs on beside
//! a dead thread, and a panic in a task run with [`keep_running`] starts that task again.

use std::sync::PoisonError;
pub use std::sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard, WaitTimeoutResult};
use std::thread::JoinHandle;
use std::time::Duration;

/// A mutex whose `lock` always returns the guard.
#[derive(Debug, Default)]
pub struct Mutex<T: ?Sized>(std::sync::Mutex<T>);

impl<T> Mutex<T> {
    pub const fn new(value: T) -> Self {
        Self(std::sync::Mutex::new(value))
    }

    pub fn into_inner(self) -> T {
        self.0.into_inner().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: ?Sized> Mutex<T> {
    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The guard, or `None` while another thread holds the lock.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        match self.0.try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }
}

/// A condition variable for a [`Mutex`]. Its waits always return the guard.
#[derive(Debug, Default)]
pub struct Condvar(std::sync::Condvar);

impl Condvar {
    pub const fn new() -> Self {
        Self(std::sync::Condvar::new())
    }

    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.0.wait(guard).unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        within: Duration,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
        self.0
            .wait_timeout(guard, within)
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn wait_timeout_while<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        within: Duration,
        waiting: impl FnMut(&mut T) -> bool,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
        self.0
            .wait_timeout_while(guard, within, waiting)
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub fn notify_one(&self) {
        self.0.notify_one();
    }

    pub(crate) fn notify_all(&self) {
        self.0.notify_all();
    }
}

/// A reader-writer lock whose `read` and `write` always return the guard.
#[derive(Debug, Default)]
pub(crate) struct RwLock<T: ?Sized>(std::sync::RwLock<T>);

impl<T> RwLock<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self(std::sync::RwLock::new(value))
    }
}

impl<T: ?Sized> RwLock<T> {
    pub(crate) fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.write().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Starts a thread its process cannot run without. If `body` panics, the process ends.
pub fn spawn<T: Send + 'static>(
    name: impl Into<String>,
    body: impl FnOnce() -> T + Send + 'static,
) -> std::io::Result<JoinHandle<T>> {
    std::thread::Builder::new().name(name.into()).spawn(move || {
        let _ends = EndsTheProcess;
        body()
    })
}

/// Dropped as a panic unwinds its thread, it ends the process. The panic hook has already logged the panic.
struct EndsTheProcess;

impl Drop for EndsTheProcess {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::error!(
                name: "thread_died", target: "processes",
                thread = std::thread::current().name().unwrap_or_default(),
                "a thread this process cannot run without panicked, so the process ends"
            );
            crate::logs::stop();
            std::process::exit(1);
        }
    }
}

/// How long after a panic [`keep_running`] waits before it starts the task again, so one that panics each time it
/// starts does not spin.
const RESTART_AFTER: Duration = Duration::from_secs(5);

/// Runs the task `make` builds. If it panics, it is started again after [`RESTART_AFTER`]; if it returns or is
/// cancelled, it is done. Use it for a task whose shared state sits behind this module's locks, which a panic leaves
/// usable.
pub fn keep_running<Task>(name: &'static str, mut make: impl FnMut() -> Task + Send + 'static)
where
    Task: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        // Each run is a task of its own: the runtime catches its panic, and this loop outlives it.
        while let Err(ended) = tokio::spawn(make()).await {
            if !ended.is_panic() {
                return;
            }
            tracing::error!(
                name: "task_restarted", target: "processes",
                task = name,
                after_secs = RESTART_AFTER.as_secs(),
                "a background task panicked, so it is started again"
            );
            tokio::time::sleep(RESTART_AFTER).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The environment variable that makes the second run of this test panic on a thread started with [`spawn`].
    const PANIC_ON_A_THREAD: &str = "SMELTR_TEST_PANIC_ON_A_THREAD";

    /// Runs itself a second time, in a process of its own, and there panics on a thread the process cannot run
    /// without. The "thread panicked" line that second process prints is the test working.
    #[test]
    fn a_process_ends_when_a_thread_it_cannot_run_without_panics() {
        if std::env::var_os(PANIC_ON_A_THREAD).is_some() {
            let _ = spawn("doomed", || panic!("a bug on a thread the process needs"))
                .unwrap()
                .join();
            // Reached only if the process outlived the thread, and then it exits 0.
            return;
        }
        let ended = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sync::tests::a_process_ends_when_a_thread_it_cannot_run_without_panics",
            ])
            .env(PANIC_ON_A_THREAD, "1")
            .output()
            .unwrap();
        assert_eq!(
            ended.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&ended.stderr)
        );
    }

    #[test]
    fn a_lock_a_panic_poisoned_still_hands_over_what_it_guards() {
        let held = Mutex::new(7);
        let _ = std::thread::scope(|s| {
            s.spawn(|| {
                let _guard = held.lock();
                panic!("a bug under the lock");
            })
            .join()
        });
        assert_eq!(*held.lock(), 7);
    }

    /// The first run panics, which prints a "thread panicked" line; the second run is the test passing.
    #[tokio::test]
    async fn a_task_that_panics_is_started_again() {
        let runs = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (again, ran_again) = tokio::sync::mpsc::unbounded_channel();
        keep_running("doomed once", move || {
            let (runs, again) = (runs.clone(), again.clone());
            async move {
                if runs.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                    panic!("a bug in a background task");
                }
                let _ = again.send(());
            }
        });
        let mut ran_again = ran_again;
        tokio::time::timeout(RESTART_AFTER * 6, ran_again.recv())
            .await
            .expect("the task ran a second time")
            .expect("the second run reported");
    }
}
