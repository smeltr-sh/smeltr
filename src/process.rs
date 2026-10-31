//! Running a child process under a deadline, and ending it and everything it spawned.
//!
//! A process that will not finish is the one failure a cooperative signal cannot reach, so what bounds it is the
//! operating system. A child spawned into its own process group (unix) or job object (windows) is one object to
//! kill, and a descendant cannot outlive the request that created it.
//!
//! The group is what makes reading the child's output safe, as much as what makes killing it complete. A pipe
//! reports end-of-file when its last writer closes, so a grandchild holding the child's stdout keeps that read
//! alive long after the child itself has exited -- which is how `Command::output` hangs on a process that has
//! already finished. Killing the group closes every writer at once, and the read ends.
//!
//! Parent death is contained differently on each platform, because each already offers one mechanism: a job
//! object closed when its handle drops takes its processes with it, and a unix child learns its parent is gone
//! from end-of-file on a pipe it holds.
//!
//! Three things remain outside what this can promise, and all are the operating system's rather than ours. A
//! unix child that calls `setsid` or `setpgid` leaves the group and survives the signal; closing that needs a
//! cgroup or a pid namespace, and this builds on neither. Confirming an exit means waiting for one, so a
//! process wedged in an uninterruptible kernel call does not die on `SIGKILL` and is not reaped until it leaves
//! that state. And a tree confirmed dead by its pipe's end-of-file is a tree whose members all still held that
//! pipe: one that closed or redirected its own copy first, then wedged, is no longer a writer the pipe is
//! waiting on, so the read finishes while that member remains. A caller that cannot confirm an exit has to
//! report that rather than start a replacement.

use std::io;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

/// How often a wait asks whether the child has ended. The runtime's own drain polls at this rate.
const POLL: Duration = Duration::from_millis(10);

/// How long a killed tree has to actually end. A process stuck in an uninterruptible call takes its signal only
/// when it leaves, and waiting on one without a deadline would strand the schedule that ran the command -- which
/// for a worker is the discovery that would have found the commit replacing what hung.
const KILL_CONFIRM: Duration = Duration::from_secs(30);

/// What became of a bounded run. Both carry whatever output was read: a killed process is usually the one whose
/// partial stderr says why it stalled.
#[derive(Debug)]
pub enum Outcome {
    /// The process ended on its own, and everything it spawned released the output pipes.
    Exited(Output),
    /// The budget expired. The process tree was killed and reaped.
    Killed(Output),
}

/// A child process and everything it spawns, ended as one.
pub struct Contained {
    /// Declared before `child`, because fields drop in declaration order and closing the job is what ends the
    /// tree. A `Contained` dropped without being killed still takes its processes with it.
    #[cfg(windows)]
    job: windows::Job,
    child: Child,
    /// The status of a child already waited for. A second wait on a reaped child has nothing to answer.
    reaped: Option<ExitStatus>,
}

impl Contained {
    /// Spawns `cmd` under a group of its own. The caller owns the command's stdio configuration.
    pub fn spawn(cmd: &mut Command) -> io::Result<Contained> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // 0 means a new group led by the child, so the group id is the child's own pid.
            cmd.process_group(0);
            let child = cmd.spawn()?;
            Ok(Contained { child, reaped: None })
        }
        #[cfg(windows)]
        {
            let job = windows::Job::create()?;
            let child = cmd.spawn()?;
            // A process spawned and then adopted could spawn its own child in between, and that grandchild would
            // not be in the job. Closing the window needs the child created suspended, which `Command` does not
            // expose; the exposure is microseconds wide and documented rather than claimed closed.
            if let Err(failed) = job.adopt(&child) {
                // Unadopted, so closing the job would not take it: the only handle to it is this one.
                let mut child = child;
                let _ = child.kill();
                let _ = child.wait();
                return Err(failed);
            }
            Ok(Contained {
                child,
                reaped: None,
                job,
            })
        }
    }

    /// This child's process id, which on unix is also its process group's.
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }

    /// One non-blocking wait. `Some` has reaped the child and released its process id, so this is only called
    /// once the group has been signaled: see [`reap_within`](Self::reap_within).
    fn try_reap(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.reaped {
            return Ok(Some(status));
        }
        let status = self.child.try_wait()?;
        self.reaped = status;
        Ok(status)
    }

    /// Kills the child and every process it spawned. Succeeds when the tree is already gone, since that is the
    /// state asked for.
    ///
    /// The child having exited is not a reason to skip this: a descendant outliving its parent is the case this
    /// exists for, and an exited child that has not been reaped still pins the process id its group is named by.
    /// An empty group answers `ESRCH`, which is the state asked for.
    ///
    /// Reaping the child first and killing afterward would name an id the system may have handed to somebody
    /// else, so a reaped child is not signaled: the order is enforced here rather than asked of each caller.
    pub fn kill_tree(&mut self) -> io::Result<()> {
        #[cfg(unix)]
        {
            // A reaped child's id is released and may already name somebody else's group. There is nothing of
            // ours left to signal, so this is not a kill that is skipped but a kill that has no target. Unix
            // only: a job handle is not an id, and on Windows `ended` reaps as it asks, so refusing here would
            // leave a job with a living descendant untouched for good.
            if self.reaped.is_some() {
                return Ok(());
            }
            let group = self.child.id();
            // 0 would mean this process's own group and 1 the init group. A spawned child is neither, so a value
            // that reads as one of them is a bug rather than a tree to kill.
            debug_assert!(group > 1, "a contained child has a process group of its own");
            if group <= 1 {
                return Ok(());
            }
            // SAFETY: a signal to the process group this child leads, created by `process_group(0)` above.
            if unsafe { libc::killpg(group as libc::pid_t, libc::SIGKILL) } == -1 {
                let failed = io::Error::last_os_error();
                if failed.raw_os_error() != Some(libc::ESRCH) {
                    return Err(failed);
                }
            }
            Ok(())
        }
        #[cfg(windows)]
        {
            // Terminating an empty job succeeds, so a reaped child is no reason to skip this.
            self.job.terminate()
        }
    }

    /// Whether the child has ended, leaving it unreaped.
    ///
    /// The group is named by the child's process id, and that id is held only while the child is unreaped -- as a
    /// live process or as a zombie. So a caller that means to kill the group asks this, kills, and only then
    /// reaps; asking [`finished`](Self::finished) first releases the id, and the kill would name whatever the
    /// system handed it to next.
    pub fn ended(&mut self) -> io::Result<bool> {
        if self.reaped.is_some() {
            return Ok(true);
        }
        #[cfg(unix)]
        {
            // `waitid` rather than `waitpid`, because `WNOWAIT` is its flag alone: it is what leaves the child
            // waitable, so this observes the exit without consuming it and `reap` still answers the status.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            // SAFETY: a zeroed `siginfo_t` is what `waitid` fills, and the child id is this process's own.
            let answered = unsafe {
                libc::waitid(
                    libc::P_PID,
                    self.child.id(),
                    &mut info,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            if answered == -1 {
                return Err(io::Error::last_os_error());
            }
            // `WNOHANG` answers success with nothing filled in when the child is still running.
            // SAFETY: reading the union member `waitid` sets for a process event.
            Ok(unsafe { info.si_pid() } != 0)
        }
        #[cfg(windows)]
        {
            // The job object holds the tree, not a process id, so nothing here is released by reaping.
            Ok(self.try_reap()?.is_some())
        }
    }

    /// Reaps the child within `budget`, answering its status, or `None` when it would not be reaped in time.
    ///
    /// Called after [`kill_tree`](Self::kill_tree), never before: reaping releases the process id the group is
    /// named by. It is polled rather than waited on because the thing being waited for may be a process wedged
    /// in an uninterruptible kernel call, which takes its signal only on leaving and would hold a blocking wait
    /// for as long as that is. `None` is an ending the caller cannot confirm, and reports rather than waits out.
    pub fn reap_within(&mut self, budget: Duration) -> io::Result<Option<ExitStatus>> {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(status) = self.try_reap()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(POLL);
        }
    }
}

/// How long a dropped `Contained` spends reaping what it killed. Short, because whichever thread dropped it did
/// not sign up to wait: a tree that will not confirm in this time is left as a zombie holding its id, which is a
/// leak the console reports rather than a thread that never returns.
const DROP_REAP: Duration = Duration::from_secs(1);

impl Drop for Contained {
    /// Nothing spawned is left running by a path that gave up on it. Every early return past `spawn` -- a reader
    /// thread that would not start, a frame that would not write, a caller that decided against the process --
    /// ends here, so containment does not depend on each such path remembering to kill.
    ///
    /// A child already confirmed reaped is left alone: its id is released and may name somebody else's group.
    fn drop(&mut self) {
        if self.reaped.is_some() {
            return;
        }
        let _ = self.kill_tree();
        if matches!(self.reap_within(DROP_REAP), Ok(None)) {
            tracing::warn!(
                target: "process.not_reaped",
                pid = self.child.id(),
                "a killed process tree would not confirm its exit in time and is left unreaped"
            );
        }
    }
}

/// Runs `cmd` to completion within `budget`, answering what it wrote. On expiry the process tree is killed and
/// reaped, so this returns within the budget plus however long the operating system takes to kill.
///
/// stdin is closed: a child that reads from a terminal it does not have would wait for one forever.
pub fn output_within(cmd: &mut Command, budget: Duration) -> io::Result<Outcome> {
    cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = Contained::spawn(cmd)?;

    // Each pipe is drained by a thread of its own, because a child that fills one while nobody reads it blocks on
    // the write and never reaches its own exit.
    let out = child.take_stdout();
    let err = child.take_stderr();
    // Through the fallible builder: a panicking spawn would unwind past the child, and dropping it is what ends
    // the tree, but a caller that asked for output and got a panic has no error to act on.
    let reading = Drains::start(out, err)?;

    // Exit alone is not the end: a grandchild holding an output pipe leaves a read outstanding after the child has
    // gone, and the budget covers that wait too.
    let deadline = Instant::now() + budget;
    let mut ended = false;
    loop {
        // The drains are asked first and the child only once they are done, and the child is asked without being
        // reaped: the process id the group is named by is held only while it is unreaped, so the kill below has a
        // live process or a zombie to aim at rather than an id the system has handed to somebody else.
        if reading.finished() && child.ended()? {
            ended = true;
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(POLL);
    }

    if ended {
        // Both pipes closed and the child gone still leaves room for a descendant that closed its own copies of
        // the pipes and kept running. Bounded means the whole tree ends here, and the kill comes before the reap
        // so it names this group rather than one that took its id. The joins are safe without a deadline of their
        // own: this is only reached once both drains reported themselves finished.
        child.kill_tree()?;
        return match child.reap_within(KILL_CONFIRM)? {
            Some(status) => Ok(Outcome::Exited(reading.collect(status))),
            None => Err(unconfirmed()),
        };
    }
    child.kill_tree()?;
    // What makes the joins below bounded is the reads having ended, not the child having exited: a descendant
    // holds its own copy of these pipes, and one the kill cannot reach keeps them open with the child already
    // gone. So both drains are what this waits for, and only then is the child reaped -- the same order the loop
    // above uses, for the same reason. A tree that will not confirm leaves the reads outstanding and is reported
    // rather than joined.
    let deadline = Instant::now() + KILL_CONFIRM;
    while !(reading.finished()) {
        if Instant::now() >= deadline {
            return Err(unconfirmed());
        }
        std::thread::sleep(POLL);
    }
    match child.reap_within(deadline.saturating_duration_since(Instant::now()))? {
        Some(status) => Ok(Outcome::Killed(reading.collect(status))),
        None => Err(unconfirmed()),
    }
}

/// The one error every unconfirmed ending here reports. A caller that sees it has a tree it must not replace.
fn unconfirmed() -> io::Error {
    io::Error::other("a killed process tree would not end, so the command's exit could not be confirmed")
}

/// A thread reading each of a child's output pipes to its end.
struct Drains {
    out: std::thread::JoinHandle<Vec<u8>>,
    err: std::thread::JoinHandle<Vec<u8>>,
}

impl Drains {
    /// A reader for each pipe, or neither.
    fn start(out: Option<ChildStdout>, err: Option<ChildStderr>) -> io::Result<Drains> {
        let out = std::thread::Builder::new()
            .name("smeltr-cmd-stdout".to_string())
            .spawn(move || drain(out))?;
        // If this one cannot start, the first ends on its own once the child is dropped and its pipe closes.
        let err = std::thread::Builder::new()
            .name("smeltr-cmd-stderr".to_string())
            .spawn(move || drain(err))?;
        Ok(Drains { out, err })
    }

    /// Both pipes read to their end.
    fn finished(&self) -> bool {
        self.out.is_finished() && self.err.is_finished()
    }

    /// What the child wrote, with the status it ended with.
    fn collect(self, status: ExitStatus) -> Output {
        Output {
            status,
            stdout: self.out.join().unwrap_or_default(),
            stderr: self.err.join().unwrap_or_default(),
        }
    }
}

fn drain<R: io::Read>(stream: Option<R>) -> Vec<u8> {
    let mut read = Vec::new();
    if let Some(mut stream) = stream {
        let _ = io::Read::read_to_end(&mut stream, &mut read);
    }
    read
}

#[cfg(windows)]
mod windows {
    use std::io;
    use std::os::windows::io::AsRawHandle;
    use std::process::Child;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation, SetInformationJobObject,
        TerminateJobObject,
    };

    /// A job object holding one process tree. Dropping it kills what it holds, which is what contains a runtime
    /// child when its supervisor dies.
    pub struct Job(HANDLE);

    // SAFETY: a job handle is not thread-affine, and `Drop` closes it exactly once.
    unsafe impl Send for Job {}
    unsafe impl Sync for Job {}

    impl Job {
        pub fn create() -> io::Result<Job> {
            // SAFETY: an unnamed job with default security, which is what both null arguments ask for.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            let job = Job(handle);
            let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: `limits` is the struct this information class names, and the length is its own.
            let set = unsafe {
                SetInformationJobObject(
                    job.0,
                    JobObjectExtendedLimitInformation,
                    (&raw const limits).cast(),
                    size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            if set == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(job)
        }

        pub fn adopt(&self, child: &Child) -> io::Result<()> {
            // SAFETY: a live job handle and a live process handle the `Child` still owns.
            if unsafe { AssignProcessToJobObject(self.0, child.as_raw_handle() as HANDLE) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub fn terminate(&self) -> io::Result<()> {
            // SAFETY: a live job handle. Terminating an empty job succeeds.
            if unsafe { TerminateJobObject(self.0, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    impl Drop for Job {
        fn drop(&mut self) {
            // SAFETY: the handle was created here and is closed once.
            unsafe { CloseHandle(self.0) };
        }
    }
}

/// A test's fake child: `unix` run by `sh`, `windows` by `cmd`, so each test states what its child does on both.
/// Its scripts sleep thirty seconds, long enough that a test reaching the end of one has failed rather than raced.
#[cfg(test)]
pub(crate) fn shell(unix: &str, windows: &str) -> Command {
    #[cfg(unix)]
    {
        let _ = windows;
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(unix);
        cmd
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let _ = unix;
        let mut cmd = Command::new("cmd");
        // Raw, because `cmd` does not read the quoting `Command` gives an argument.
        cmd.raw_arg(format!("/d /c {windows}"));
        cmd
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> Duration {
        Duration::from_millis(200)
    }

    #[test]
    fn a_command_that_finishes_inside_its_budget_answers_what_it_wrote() {
        let mut cmd = Command::new("git");
        cmd.arg("--version");
        match output_within(&mut cmd, Duration::from_secs(30)).expect("git --version runs") {
            Outcome::Exited(out) => {
                assert!(out.status.success());
                assert!(String::from_utf8_lossy(&out.stdout).starts_with("git version"));
            }
            Outcome::Killed(_) => panic!("git --version outlasted thirty seconds"),
        }
    }

    #[test]
    fn a_command_that_outlasts_its_budget_is_killed() {
        let started = Instant::now();
        let mut cmd = shell("sleep 30", "ping -n 31 127.0.0.1 >nul");
        let outcome = output_within(&mut cmd, budget()).expect("the fake child spawns");
        assert!(
            matches!(outcome, Outcome::Killed(_)),
            "a sleeping process was not killed"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the kill did not follow the budget"
        );
    }

    /// The hang this module exists for: the child exits at once, and a grandchild it left behind holds the output
    /// pipe open. `Command::output` waits for that pipe forever; a bounded run kills the group and the read ends.
    #[test]
    fn a_grandchild_holding_the_output_pipe_cannot_outlast_the_budget() {
        let started = Instant::now();
        let mut cmd = shell("sleep 30 & exit 0", "start /b ping -n 31 127.0.0.1 & exit 0");
        let outcome = output_within(&mut cmd, budget()).expect("the fake child spawns");
        assert!(
            matches!(outcome, Outcome::Killed(_)),
            "a grandchild held the pipe and the run was reported as a clean exit"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the pipe outlasted the budget"
        );
    }

    /// A descendant that redirects its output away from the pipes lets the run finish on time, and must not
    /// be what is left of it afterward.
    #[test]
    #[cfg(unix)]
    fn a_quiet_grandchild_does_not_outlive_a_finished_command() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 30 >/dev/null 2>&1 & echo $! ; exit 0");
        // Room for the shell to start and exit on a loaded machine: the test is about the grandchild, not the budget.
        let outcome = output_within(&mut cmd, Duration::from_secs(5)).expect("sh spawns");
        let Outcome::Exited(output) = outcome else {
            panic!("a command that exited was reported as killed");
        };
        let grandchild: i32 = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .expect("sh printed a pid");
        // Signal 0 tests for existence; a killed-but-unreaped grandchild is a zombie the shell's exit reparented
        // to init, so an answer of "gone" may take a moment.
        let gone = (0..200).any(|_| {
            // SAFETY: signal 0 delivers nothing.
            let alive = unsafe { libc::kill(grandchild, 0) } == 0;
            if alive {
                std::thread::sleep(Duration::from_millis(10));
            }
            !alive
        });
        assert!(gone, "the grandchild outlived the command that spawned it");
    }

    /// What a stalled process wrote before it was killed is what says why it stalled.
    #[test]
    fn output_written_before_a_kill_is_kept() {
        let mut cmd = shell("echo halfway; sleep 30", "echo halfway& ping -n 31 127.0.0.1 >nul");
        match output_within(&mut cmd, budget()).expect("the fake child spawns") {
            Outcome::Killed(out) => assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "halfway"),
            Outcome::Exited(_) => panic!("a sleeping process was reported as a clean exit"),
        }
    }

    /// A killed child that is never waited for stays a zombie holding its process id.
    #[test]
    fn a_killed_tree_is_reaped() {
        let mut cmd = shell("sleep 30", "ping -n 31 127.0.0.1 >nul");
        cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        let mut child = Contained::spawn(&mut cmd).expect("the fake child spawns");
        assert!(
            !child.ended().expect("waitid answers"),
            "a sleeping child read as ended"
        );
        child.kill_tree().expect("the group is killable");
        let status = child
            .reap_within(budget())
            .expect("the wait answers")
            .expect("a killed child is reaped inside the budget");
        assert!(!status.success(), "a killed process reported success");
        // Reaping twice answers the same status rather than blocking on a child that is already gone.
        assert_eq!(
            child.reap_within(Duration::ZERO).expect("the wait answers"),
            Some(status),
            "the status is remembered"
        );
    }

    #[test]
    fn killing_a_tree_that_has_already_gone_succeeds() {
        let mut cmd = shell("exit 0", "exit 0");
        cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        let mut child = Contained::spawn(&mut cmd).expect("the fake child spawns");
        // Exited but not reaped: a zombie, whose id still names an empty group. That is the state a clean exit
        // is killed in, and killing it is the state asked for.
        let deadline = Instant::now() + budget();
        while !child.ended().expect("waitid answers") {
            assert!(Instant::now() < deadline, "true never ended");
            std::thread::sleep(POLL);
        }
        child
            .kill_tree()
            .expect("killing what is already gone is the state asked for");
        child
            .reap_within(budget())
            .expect("the wait answers")
            .expect("a finished child is reaped");
    }

    /// The id of a reaped child is not ours to signal. The order is enforced by the type rather than asked of
    /// each caller, which is what stops the mistake recurring one call site at a time.
    #[test]
    #[cfg(unix)]
    fn a_reaped_child_is_never_signaled() {
        let mut cmd = Command::new("true");
        cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        let mut child = Contained::spawn(&mut cmd).expect("true spawns");
        child
            .reap_within(budget())
            .expect("the wait answers")
            .expect("a finished child is reaped");
        // Nothing observable distinguishes a refused signal from an empty group here; what this pins is that the
        // call is accepted and answers success without touching the released id.
        child.kill_tree().expect("a reaped child answers without signaling");
    }
}
