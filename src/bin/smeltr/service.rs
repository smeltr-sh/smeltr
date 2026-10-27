//! Running as a Windows service. The Service Control Manager starts the process and then waits to be told it is
//! running; one that never says so is killed. Its Stop is the operator's orderly stop, so it ends the process the
//! way a signal does elsewhere, and the process says it has stopped before it goes.
//!
//! The service works in `%ProgramData%\Smeltr`, or in the directory `SMELTR_CONFIG_FILE` names, so `smeltr.toml`
//! and every relative default resolve there. It has no console, so what the process prints -- its logs, and why it
//! refused to start -- goes to `smeltr.log` in that directory: what systemd's journal keeps on Linux. The log is
//! renamed to `smeltr.log.1` once it reaches `LOG_LIMIT`, so the two hold at most twice that.
//!
//! Started any other way -- a console, a container, systemd -- the process runs as it always does, and so does a
//! runtime a worker spawned, which no Service Control Manager started.

/// Runs `start` as the service, when the Service Control Manager started this process, or as itself otherwise.
/// Never returns: `start` ends the process. `stop` is what a Stop does; it is called once per Stop, and the process
/// ends on its own afterward.
#[cfg(windows)]
pub fn run(start: fn(), stop: fn()) -> ! {
    imp::run(start, stop)
}

#[cfg(not(windows))]
pub fn run(start: fn(), _stop: fn()) -> ! {
    start();
    unreachable!("start ends the process")
}

/// Tells the Service Control Manager the service has stopped, so it records an orderly stop rather than a crash.
/// Called last on the way out of an orderly stop; does nothing in a process that is not a service.
pub fn stopped() {
    #[cfg(windows)]
    imp::stopped();
}

/// How large `smeltr.log` grows before it is set aside for a new one.
#[cfg(windows)]
const LOG_LIMIT: u64 = 16 * 1024 * 1024;

#[cfg(windows)]
mod imp {
    use std::fs::{File, OpenOptions};
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::IntoRawHandle;
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_CALL_NOT_IMPLEMENTED, ERROR_FAILED_SERVICE_CONTROLLER_CONNECT, HANDLE, NO_ERROR,
    };
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};
    use windows_sys::Win32::System::Console::{STD_ERROR_HANDLE, SetStdHandle};
    use windows_sys::Win32::System::Services::{
        RegisterServiceCtrlHandlerExW, SERVICE_ACCEPT_PRESHUTDOWN, SERVICE_ACCEPT_SHUTDOWN, SERVICE_ACCEPT_STOP,
        SERVICE_CONTROL_INTERROGATE, SERVICE_CONTROL_PRESHUTDOWN, SERVICE_CONTROL_SHUTDOWN, SERVICE_CONTROL_STOP,
        SERVICE_RUNNING, SERVICE_STATUS, SERVICE_STATUS_CURRENT_STATE, SERVICE_STOP_PENDING, SERVICE_STOPPED,
        SERVICE_TABLE_ENTRYW, SERVICE_WIN32_OWN_PROCESS, SetServiceStatus, StartServiceCtrlDispatcherW,
    };

    /// How often a stop in progress tells the Service Control Manager it is still going, and how long it promises
    /// each time. A worker's stop is bounded by its own budgets (`worker::stop_before_exit`), so the process ends
    /// within them whatever this says; this only keeps the Service Control Manager from giving up on it first.
    const STOP_TICK: Duration = Duration::from_secs(2);
    const STOP_WAIT_HINT: Duration = Duration::from_secs(10);

    static START: OnceLock<fn()> = OnceLock::new();
    static STOP: OnceLock<fn()> = OnceLock::new();
    /// The handle `RegisterServiceCtrlHandlerExW` gave, as an address: a pointer is not `Sync`.
    static HANDLE: OnceLock<usize> = OnceLock::new();
    static CHECKPOINT: AtomicU32 = AtomicU32::new(0);

    pub fn run(start: fn(), stop: fn()) -> ! {
        let _ = START.set(start);
        let _ = STOP.set(stop);
        // A service's own process has one service, so the name is not looked at.
        let mut name = [0u16];
        let table = [
            SERVICE_TABLE_ENTRYW {
                lpServiceName: name.as_mut_ptr(),
                lpServiceProc: Some(service_main),
            },
            SERVICE_TABLE_ENTRYW {
                lpServiceName: std::ptr::null_mut(),
                lpServiceProc: None,
            },
        ];
        // Returns at once, failing, in a process the Service Control Manager did not start. In one it did, it
        // returns only after the service has stopped -- and `start` ends the process before that.
        if unsafe { StartServiceCtrlDispatcherW(table.as_ptr()) } == 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_FAILED_SERVICE_CONTROLLER_CONNECT as i32) {
                eprintln!(
                    "smeltr: refusing to start -- the Service Control Manager did not take this service: {error}"
                );
                std::process::exit(1);
            }
        }
        start();
        unreachable!("start ends the process")
    }

    unsafe extern "system" fn service_main(_argc: u32, _argv: *mut windows_sys::core::PWSTR) {
        let mut name = [0u16];
        let handle = unsafe { RegisterServiceCtrlHandlerExW(name.as_mut_ptr(), Some(on_control), std::ptr::null()) };
        if handle.is_null() {
            eprintln!(
                "smeltr: refusing to start -- the Service Control Manager did not take this service's handler: {}",
                std::io::Error::last_os_error()
            );
            std::process::exit(1);
        }
        let _ = HANDLE.set(handle as usize);
        // As systemd's `WorkingDirectory` would. Made if missing, so a first start works, inheriting its parent's
        // access; the install docs lock it down first.
        let dir = service_dir();
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::env::set_current_dir(&dir);
        log_to_file(&dir);
        // Running as soon as it is started: a process that refuses to start ends within moments, and the Service
        // Control Manager then records that it ended unexpectedly, which is what happened.
        report(
            SERVICE_RUNNING,
            SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_PRESHUTDOWN | SERVICE_ACCEPT_SHUTDOWN,
            Duration::ZERO,
        );
        START.get().expect("set before the dispatcher starts")();
    }

    unsafe extern "system" fn on_control(
        control: u32,
        _event: u32,
        _data: *mut std::ffi::c_void,
        _context: *mut std::ffi::c_void,
    ) -> u32 {
        match control {
            // Preshutdown comes first on a machine shutting down, and is given longer than shutdown is.
            SERVICE_CONTROL_STOP | SERVICE_CONTROL_PRESHUTDOWN | SERVICE_CONTROL_SHUTDOWN => {
                // Accepting nothing more, so a second Stop cannot cut the first one's drain short.
                report(SERVICE_STOP_PENDING, 0, STOP_WAIT_HINT);
                let ticked = std::thread::Builder::new()
                    .name("smeltr-service-stop".to_string())
                    .spawn(|| {
                        loop {
                            std::thread::sleep(STOP_TICK);
                            report(SERVICE_STOP_PENDING, 0, STOP_WAIT_HINT);
                        }
                    });
                // Without the ticker the stop still runs; the Service Control Manager may only report it as hung.
                drop(ticked);
                STOP.get().expect("set before the dispatcher starts")();
                NO_ERROR
            }
            SERVICE_CONTROL_INTERROGATE => NO_ERROR,
            _ => ERROR_CALL_NOT_IMPLEMENTED,
        }
    }

    pub fn stopped() {
        report(SERVICE_STOPPED, 0, Duration::ZERO);
    }

    /// Points stderr at `smeltr.log`, and keeps it under `LOG_LIMIT` from then on. Rust's stderr looks up
    /// the process's handle on every write, so everything printed after this lands in the file. A file that cannot
    /// be opened leaves stderr as it was: a service with nowhere to log is still a service.
    fn log_to_file(dir: &Path) {
        let path = dir.join("smeltr.log");
        let Ok(file) = open_log(&path) else { return };
        let mut current = point_stderr_at(file);
        let rotated = std::thread::Builder::new()
            .name("smeltr-service-log".to_string())
            .spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(10));
                    if std::fs::metadata(&path).is_ok_and(|m| m.len() >= super::LOG_LIMIT)
                        && std::fs::rename(&path, path.with_extension("log.1")).is_ok()
                        && let Ok(file) = open_log(&path)
                    {
                        // Closed a second later: a write that looked up the old handle just before the swap is done
                        // with it by then.
                        let previous = std::mem::replace(&mut current, point_stderr_at(file));
                        std::thread::sleep(Duration::from_secs(1));
                        unsafe { CloseHandle(previous as HANDLE) };
                    }
                }
            });
        // Without the thread the log only grows; nothing else depends on it.
        drop(rotated);
    }

    /// Where the service works: the directory of `SMELTR_CONFIG_FILE` when it names one, else
    /// `%ProgramData%\Smeltr`. A service starts in `System32`, so without this every relative default -- `smeltr.toml`,
    /// the data directories, the secrets encryption key -- would resolve there.
    fn service_dir() -> PathBuf {
        let program_data = || std::env::var_os("ProgramData").map(|dir| Path::new(&dir).join("Smeltr"));
        smeltr::startup_config::find_config_file("smeltr.toml")
            .filter(|config| config.is_absolute())
            .and_then(|config| config.parent().map(Path::to_path_buf))
            .or_else(program_data)
            .unwrap_or_default()
    }

    /// Shared for delete, so it can be renamed aside while open, and for reading, so it can be read while written.
    fn open_log(path: &Path) -> std::io::Result<File> {
        OpenOptions::new()
            .append(true)
            .create(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .open(path)
    }

    /// The handle stderr now writes to, as an address, for closing once it is replaced.
    fn point_stderr_at(file: File) -> usize {
        let handle = file.into_raw_handle();
        unsafe { SetStdHandle(STD_ERROR_HANDLE, handle as HANDLE) };
        handle as usize
    }

    fn report(state: SERVICE_STATUS_CURRENT_STATE, accepted: u32, wait_hint: Duration) {
        let Some(&handle) = HANDLE.get() else { return };
        let status = SERVICE_STATUS {
            dwServiceType: SERVICE_WIN32_OWN_PROCESS,
            dwCurrentState: state,
            dwControlsAccepted: accepted,
            dwWin32ExitCode: NO_ERROR,
            dwServiceSpecificExitCode: 0,
            // Only a pending state counts; each report of one must count higher than the last.
            dwCheckPoint: if state == SERVICE_STOP_PENDING {
                CHECKPOINT.fetch_add(1, Ordering::Relaxed) + 1
            } else {
                0
            },
            dwWaitHint: wait_hint.as_millis() as u32,
        };
        unsafe { SetServiceStatus(handle as _, &status) };
    }
}
