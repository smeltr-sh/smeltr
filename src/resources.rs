//! What this worker -- its supervisor and the runtime it runs -- is spending against what it is
//! allowed to run in. Sampled once per heartbeat; a CPU rate needs two points in time, so
//! [`Sampler`] holds the previous one rather than reading it fresh on every call.
//!
//! A k8s pod's own CPU and memory limits, on the node that runs it, *are* its container's cgroup
//! limits: there is no separate API to ask, only the cgroup the container was placed under, and a
//! limit set anywhere from that cgroup up to the root applies -- both versions enforce
//! hierarchically, so a container's own leaf can be bounded by a slice or a pod cgroup above it.
//! Linux is the only platform with cgroups (`process.rs` already says so, for the same reason);
//! nothing else has a container limit to find, so usage there is measured against the whole
//! machine instead.
//!
//! Every figure is this worker's own: its supervisor and the runtime it runs, never another
//! worker. A cgroup's usage stands in for that when nothing but this process's own tree lives in
//! it, a container's ordinarily, since that covers a runtime child the way a pod's own limit is
//! measured against. A cgroup shared with anything else -- other workers in one container, a
//! login session, a systemd scope -- answers with this process tree's own usage instead, still
//! against the cgroup's limit: attributing another process's usage to this worker would make
//! every worker in a shared container read the same figure.
//!
//! Disk is the one figure here that is not about this process at all. It is the filesystem under
//! the worker's state directory -- the one volume a worker owns, holding the identity, credential
//! and clone it has to keep across a restart -- so it is the one whose filling up stops the worker.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// One sample.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ResourceUsage {
    /// CPU time used since the previous sample over the wall time between the two, as a percent
    /// of `cpu_limit_cores`. 100 means every allowed core was busy.
    ///
    /// `None` on the first sample of a process's life: a rate needs a previous reading, and
    /// reporting zero would say "idle" about a worker that has simply just started.
    pub cpu_percent: Option<f64>,
    /// How many cores `cpu_percent` is a percentage of: a container's own quota where one was
    /// found, every core the host has otherwise. Fractional under a quota, which is what a k8s
    /// `cpu: 500m` limit is.
    pub cpu_limit_cores: f64,
    /// Whether `cpu_percent` is measured against a real container CPU quota. `false` means it is
    /// measured against every core the host has, which is what "100%" means on a shared or
    /// unlimited machine.
    pub cpu_limited: bool,
    pub memory_used_bytes: u64,
    /// What `memory_used_bytes` is measured against: a container memory limit where one was found,
    /// the host's own total otherwise. `None` only when neither could be read -- rare, and the one
    /// case a percentage cannot be shown at all.
    pub memory_limit_bytes: Option<u64>,
    /// Whether `memory_limit_bytes` is a real container memory limit rather than the host's own
    /// total.
    pub memory_limited: bool,
    /// Free and total bytes of the filesystem holding the worker's state directory. `None` when
    /// the filesystem would not answer. Free is what an unprivileged writer may still use, which
    /// is smaller than total minus used on a filesystem holding blocks back for root.
    pub disk_free_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
}

/// What a CPU rate needs between calls: the previous cumulative reading, which counter it came
/// from, and the moment it was taken. One belongs to a worker process for its whole life, and so
/// does the state directory whose filesystem it reports on.
pub struct Sampler {
    previous: Option<(Duration, CpuSource, Instant)>,
    carried: platform::Carried,
    /// The worker's own durable directory. A worker owns exactly one, so its filesystem is the
    /// only one whose free space says anything about whether this worker can keep running.
    state_dir: PathBuf,
}

impl Sampler {
    pub fn new(state_dir: PathBuf) -> Sampler {
        Sampler {
            previous: None,
            carried: Default::default(),
            state_dir,
        }
    }

    pub fn sample(&mut self) -> ResourceUsage {
        let (cpu, memory) = platform::sample(&mut self.carried);
        let mut usage = self.record(cpu, memory);
        // Not part of `record`: a free-space reading is instantaneous, so unlike a CPU total it
        // means something on its own and needs no previous sample to measure against.
        (usage.disk_free_bytes, usage.disk_total_bytes) = disk_space(&self.state_dir).unzip();
        usage
    }

    /// The rate computation, separated from `platform::sample()` so it can be exercised against
    /// synthetic readings instead of this host's own real CPU and memory.
    fn record(&mut self, cpu: CpuReading, memory: MemoryReading) -> ResourceUsage {
        // A total that missed a process has no rate of its own, and is no baseline for the next
        // one: the reading before it still is, since a process's time moves to its parent's
        // cutime when it is reaped and so is counted on one side of a sample or the other.
        let Some(time) = cpu.time else {
            return ResourceUsage {
                cpu_percent: None,
                cpu_limit_cores: cpu.limit_cores,
                cpu_limited: cpu.limited,
                memory_used_bytes: memory.used_bytes,
                memory_limit_bytes: memory.limit_bytes,
                memory_limited: memory.limited,
                disk_free_bytes: None,
                disk_total_bytes: None,
            };
        };
        let cpu_percent = self.previous.as_ref().and_then(|(prev_time, prev_source, prev_at)| {
            // A cumulative reading only means something measured against another reading of the
            // same counter -- a switch between the cgroup's own and this process's alone (the
            // cgroup became unreadable, or a sibling appeared or left a shared scope), or between
            // one cgroup and another (this worker was moved), makes the two incomparable, and
            // this is a fresh baseline rather than a rate across them.
            if *prev_source != cpu.source {
                return None;
            }
            let elapsed = cpu.at.saturating_duration_since(*prev_at).as_secs_f64();
            if elapsed <= 0.0 || cpu.limit_cores <= 0.0 {
                return None;
            }
            // A tree's total falls when a process leaves it without a parent in the tree to carry
            // its time on: always on Windows, on Linux an orphan adopted outside the tree. Such a
            // total starts over rather than reading as idle.
            let used = time.checked_sub(*prev_time)?.as_secs_f64();
            Some((used / elapsed / cpu.limit_cores * 100.0).max(0.0))
        });
        let cpu_limited = cpu.limited;
        let cpu_at = cpu.at;
        self.previous = Some((time, cpu.source, cpu_at));

        ResourceUsage {
            cpu_percent,
            cpu_limit_cores: cpu.limit_cores,
            cpu_limited,
            memory_used_bytes: memory.used_bytes,
            memory_limit_bytes: memory.limit_bytes,
            memory_limited: memory.limited,
            disk_free_bytes: None,
            disk_total_bytes: None,
        }
    }
}

/// What this process's memory is measured against: its container's limit where one was found, the host's total
/// otherwise. `None` when neither could be read. The same reading [`Sampler::sample`] reports, for a caller that
/// wants the ceiling and keeps no sampler.
pub fn memory_limit_bytes() -> Option<u64> {
    platform::sample(&mut Default::default()).1.limit_bytes
}

/// What this process alone holds in memory, counted as [`Sampler::sample`] counts each process of the worker's tree.
pub fn own_memory_bytes() -> u64 {
    platform::tree_memory(&[std::process::id()])
}

/// This process's container's memory limit. `None` where it has none, or none could be read.
pub fn container_memory_limit_bytes() -> Option<u64> {
    let memory = platform::sample(&mut Default::default()).1;
    memory.limit_bytes.filter(|_| memory.limited)
}

/// Every logical core the host reports, for whichever platform found no real limit to measure
/// against instead. `1.0` on a host that will not answer, so a caller dividing by it still gets a
/// number rather than a NaN.
fn host_cores() -> f64 {
    std::thread::available_parallelism()
        .map(|n| n.get() as f64)
        .unwrap_or(1.0)
}

/// Free and total bytes of the filesystem holding `dir`. Free is what an unprivileged process may
/// still write -- `df`'s "Avail", not total minus used -- because a filesystem reserving blocks for
/// root stops taking this worker's writes well before it is full.
///
/// Block counts are in `f_frsize` units; `f_bsize` is a preferred I/O size, and the two differ on
/// real filesystems.
#[cfg(target_os = "linux")]
fn disk_space(dir: &Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    let block = match stat.f_frsize {
        0 => stat.f_bsize,
        frsize => frsize,
    };
    Some((stat.f_bavail * block, stat.f_blocks * block))
}

/// `GetDiskFreeSpaceExW`'s first out-parameter is free space *for the calling user*, so a quota
/// narrows it the way a reserved-block count does on unix -- the same number `disk_space` means
/// everywhere else.
#[cfg(windows)]
fn disk_space(dir: &Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;
    let wide: Vec<u16> = dir.as_os_str().encode_wide().chain(std::iter::once(0)).collect();
    let mut free = 0u64;
    let mut total = 0u64;
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free,
            &mut total,
            std::ptr::null_mut(),
        )
    };
    (ok != 0).then_some((free, total))
}

/// Which counter a [`CpuReading`] is a cumulative total from -- two are comparable only when they
/// are the exact same counter, so [`Sampler::record`] treats any other pairing, a switch between
/// versions of `Cgroup` included, as a fresh start rather than a rate across them.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CpuSource {
    /// A cgroup's own accounting, named by its resolved directory, and distinct from another
    /// cgroup this worker is moved to mid-run. Linux alone: Windows has no cgroup.
    #[cfg(target_os = "linux")]
    Cgroup(std::path::PathBuf),
    /// This process and its descendants, summed one process at a time.
    Tree,
}

struct CpuReading {
    /// `None` when a process in the tree could not be read: it exited mid-sample.
    time: Option<Duration>,
    /// When `time` was read. Stamped at the counter read itself, not later when [`Sampler::record`]
    /// runs -- on Linux, a cgroup's reading is followed by a host-wide `/proc` scan to check
    /// exclusivity, and letting that scan's own duration leak into the elapsed time a rate
    /// is computed over would skew the rate by however long the scan happened to take.
    at: Instant,
    source: CpuSource,
    limit_cores: f64,
    limited: bool,
}

struct MemoryReading {
    used_bytes: u64,
    limit_bytes: Option<u64>,
    limited: bool,
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{CpuReading, CpuSource, MemoryReading, host_cores};
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::time::Instant;

    /// What a tree's CPU total carries between samples: nothing, since a reaped child's time
    /// moves to its parent's cutime and the tree's own counters carry it.
    #[derive(Default)]
    pub struct Carried;

    pub fn sample(carried: &mut Carried) -> (CpuReading, MemoryReading) {
        let own_cgroup = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        let root = Path::new(cgroup::ROOT);
        let cpu_probe = cgroup::cpu(root, &own_cgroup, &mountinfo);
        // Stamped here, immediately after the counter itself is read -- not after the exclusivity
        // scan below, which walks every process (and every nested cgroup) on the host and can
        // itself take real wall time on a dense node. Letting that scan's duration leak into an
        // elapsed-time calculation would skew the rate by however long the scan happened to take.
        let cpu_read_at = Instant::now();
        let memory_probe = cgroup::memory(root, &own_cgroup, &mountinfo);

        // Built on every sample, limit or not: a limit says what the ceiling is, never whose
        // usage the cgroup holds. Several workers sharing one container share its limit, and
        // only this worker's own tree is this worker's figure.
        // A process started after the tree was read and before `cgroup.procs` was makes an
        // exclusive cgroup look shared. One rebuild catches it, so one spawn does not flip a
        // sample between the cgroup's figures and the tree's.
        let probed: Vec<&Path> = [cpu_probe.as_ref().map(|p| &p.2), memory_probe.as_ref().map(|p| &p.2)]
            .into_iter()
            .flatten()
            .map(std::path::PathBuf::as_path)
            .collect();
        let mut tree = own_process_tree();
        let mut members: HashSet<u32> = tree.iter().copied().collect();
        if probed.iter().any(|dir| !cgroup::is_exclusive(dir, &members)) {
            tree = own_process_tree();
            members = tree.iter().copied().collect();
        }
        let exclusive = |dir: &Path| cgroup::is_exclusive(dir, &members);

        let cpu = match cpu_probe {
            Some((time, limit, dir)) if exclusive(&dir) => CpuReading {
                time: Some(time),
                at: cpu_read_at,
                source: CpuSource::Cgroup(dir),
                limit_cores: limit.unwrap_or_else(host_cores),
                limited: limit.is_some(),
            },
            probe => {
                let limit = probe.and_then(|(_, limit, _)| limit);
                CpuReading {
                    time: tree_cpu_time(&tree, carried),
                    at: Instant::now(),
                    source: CpuSource::Tree,
                    limit_cores: limit.unwrap_or_else(host_cores),
                    limited: limit.is_some(),
                }
            }
        };
        let memory = match memory_probe {
            Some((used_bytes, limit, dir)) if exclusive(&dir) => MemoryReading {
                used_bytes,
                limit_bytes: limit.or_else(host_memory_total),
                limited: limit.is_some(),
            },
            probe => {
                let limit = probe.and_then(|(_, limit, _)| limit);
                MemoryReading {
                    used_bytes: tree_memory(&tree),
                    limit_bytes: limit.or_else(host_memory_total),
                    limited: limit.is_some(),
                }
            }
        };
        (cpu, memory)
    }

    /// Every process in `tree`'s CPU time, for a cgroup that holds more than this worker or a host
    /// with no cgroup interface. Each process's reaped children count too (its cutime and cstime), so
    /// a runtime that exits keeps the time it spent: its supervisor's own figure carries it on.
    ///
    /// `tree` is read in order, parents first: a child reaped between the two reads then moves
    /// into a parent already read and is missed, which `None` reports, rather than being counted
    /// in both. `None` too when any process could not be read.
    pub(super) fn tree_cpu_time(tree: &[u32], _: &mut Carried) -> Option<std::time::Duration> {
        let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if ticks_per_second <= 0 {
            return None;
        }
        let ticks = tree
            .iter()
            .map(|pid| parse_cpu_ticks(&std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?))
            .sum::<Option<u64>>()?;
        Some(std::time::Duration::from_secs_f64(
            ticks as f64 / ticks_per_second as f64,
        ))
    }

    /// `utime + stime + cutime + cstime` from a `/proc/<pid>/stat` line, the 14th to 17th fields.
    /// Counted from after the command name's closing paren, as [`parse_ppid`] does: what follows
    /// it is the 3rd field on.
    pub(super) fn parse_cpu_ticks(stat: &str) -> Option<u64> {
        let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().skip(11).take(4).collect();
        if fields.len() < 4 {
            return None;
        }
        fields.iter().map(|f| f.parse::<u64>().ok()).sum()
    }

    /// Every process in `tree`'s proportional share of the memory it maps, on the same terms as
    /// [`tree_cpu_time`]. Proportional, not resident: a supervisor and its runtime map one
    /// binary and may share lookup files, and resident sizes summed would count those twice.
    pub(super) fn tree_memory(tree: &[u32]) -> u64 {
        tree.iter().map(|pid| process_memory(*pid)).sum()
    }

    /// Anonymous and shared-memory PSS from `smaps_rollup`: file-backed pages, a mapped lookup's
    /// included, are cache the kernel can drop, as the cgroup's working set leaves them out. A
    /// kernel too old to split PSS gives it whole, and one older than 4.14 gives `VmRSS:`. A
    /// process that has gone since the tree was built counts nothing.
    fn process_memory(pid: u32) -> u64 {
        let rollup = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup")).ok();
        let field = |name: &str| rollup.as_deref().and_then(|text| status_field(text, name));
        let kib = match (field("Pss_Anon:"), field("Pss_Shmem:")) {
            (Some(anon), shmem) => Some(anon + shmem.unwrap_or(0)),
            (None, _) => field("Pss:"),
        };
        kib.or_else(|| read_proc_status_field(&format!("/proc/{pid}/status"), "VmRSS:"))
            .unwrap_or(0)
            * 1024
    }

    fn host_memory_total() -> Option<u64> {
        read_proc_status_field("/proc/meminfo", "MemTotal:").map(|kb| kb * 1024)
    }

    /// A `/proc` status-style file's field, in whatever unit its own line states (every file this
    /// reads reports kibibytes). One line looks like `VmRSS:\t   1234 kB`.
    fn read_proc_status_field(path: &str, field: &str) -> Option<u64> {
        status_field(&std::fs::read_to_string(path).ok()?, field)
    }

    pub(super) fn status_field(text: &str, field: &str) -> Option<u64> {
        text.lines()
            .find_map(|line| line.strip_prefix(field))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|n| n.parse().ok())
    }

    /// This process's own pid and every descendant of it -- the process tree a worker's own
    /// cgroup should hold exclusively, a runtime child included. Built fresh each sample: a
    /// child spawned or reaped since the last one changes what "exclusive" means.
    pub(super) fn own_process_tree() -> Vec<u32> {
        let mut children_of: HashMap<u32, Vec<u32>> = HashMap::new();
        if let Ok(entries) = std::fs::read_dir("/proc") {
            for entry in entries.flatten() {
                let Some(pid) = entry.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) else {
                    continue;
                };
                if let Some(ppid) = read_ppid(pid) {
                    children_of.entry(ppid).or_default().push(pid);
                }
            }
        }
        let mut seen = HashSet::new();
        let mut mine = Vec::new();
        let mut stack = vec![std::process::id()];
        while let Some(pid) = stack.pop() {
            if seen.insert(pid) {
                mine.push(pid);
                if let Some(children) = children_of.get(&pid) {
                    stack.extend(children.iter().copied());
                }
            }
        }
        mine
    }

    fn read_ppid(pid: u32) -> Option<u32> {
        parse_ppid(&std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?)
    }

    /// `/proc/<pid>/stat`'s second field (the command name) is parenthesized and may itself hold
    /// spaces or parens of its own, so this splits after its closing paren rather than counting
    /// fields from the front. What follows is `state ppid ...`.
    fn parse_ppid(stat: &str) -> Option<u32> {
        stat.rsplit_once(')')?.1.split_whitespace().nth(1)?.parse().ok()
    }

    mod cgroup {
        use super::host_memory_total;
        use std::collections::HashSet;
        use std::path::{Path, PathBuf};
        use std::time::Duration;

        pub const ROOT: &str = "/sys/fs/cgroup";

        /// `(used_bytes, limit_bytes, dir)`. `None` overall when no recognizable cgroup memory
        /// interface answers; `limit_bytes` alone is `None` when neither the cgroup nor any
        /// ancestor of it sets a ceiling. `used_bytes` is `dir`'s own, and `dir` is this process's
        /// own resolved directory, for the caller to weigh with [`is_exclusive`] whether that usage
        /// is this worker's alone. `own_cgroup` and `mountinfo` are this process's own
        /// `/proc/self/cgroup` and `/proc/self/mountinfo` -- read once by the caller and passed
        /// in, rather than read here, so they are one fact instead of two that could disagree.
        pub fn memory(root: &Path, own_cgroup: &str, mountinfo: &str) -> Option<(u64, Option<u64>, PathBuf)> {
            let (used, limit, dir) = if root.join("cgroup.controllers").exists() {
                v2_memory(root, own_cgroup, mountinfo)
            } else {
                v1_memory(root, own_cgroup, mountinfo)
            }?;
            Some((used, cap_by_host_memory(limit, host_memory_total()), dir))
        }

        /// A ceiling at or above the host's own physical memory cannot be a real constraint --
        /// nothing running on this machine will ever reach it -- so it reads as no limit rather
        /// than an unreachable denominator a usage percentage could never climb toward. A service
        /// with `MemoryMax=` copied from a larger box, or a cgroup v1 host whose sentinel-free
        /// "no limit set" value happens to parse as a real number below the `1 << 62` cutoff
        /// [`v1_memory`] already filters, both land here.
        fn cap_by_host_memory(limit: Option<u64>, host_total: Option<u64>) -> Option<u64> {
            match (limit, host_total) {
                (Some(l), Some(host)) if l >= host => None,
                _ => limit,
            }
        }

        /// `(cumulative CPU time, limit in cores, dir)`, on the same terms [`memory`] is.
        pub fn cpu(root: &Path, own_cgroup: &str, mountinfo: &str) -> Option<(Duration, Option<f64>, PathBuf)> {
            if root.join("cgroup.controllers").exists() {
                v2_cpu(root, own_cgroup, mountinfo)
            } else {
                v1_cpu(root, own_cgroup, mountinfo)
            }
        }

        /// Whether `dir`, and every cgroup nested under it, names only processes in `mine`. True
        /// for a container's own cgroup, whether or not it carries a limit -- true is what lets the
        /// cgroup's usage stand in for this worker's own, runtime child included. False for a
        /// container several workers share, and for a scope shared with processes this worker
        /// knows nothing about (a login session, a systemd user slice launched directly rather
        /// than as its own service), where cgroup-wide usage would count them as its own, and
        /// false when a `procs`
        /// file cannot be read or nothing is named anywhere: an unreadable or empty answer is not a
        /// confirmed-exclusive one.
        ///
        /// The walk goes past `dir` itself because `memory.current`/`cpu.stat` (v2) and
        /// `memory.usage_in_bytes`/`cpuacct.usage` (v1) both roll up every descendant cgroup's
        /// usage, while `cgroup.procs` at a directory lists only the processes attached there
        /// directly. A v2 cgroup cannot otherwise hold both processes and controller-enabled
        /// children at once -- except the root, which the kernel exempts from that rule -- so a
        /// worker placed there directly, sharing it with other cgroups' processes, is exactly the
        /// case this check exists to catch; v1 allows the same split at any level.
        pub fn is_exclusive(dir: &Path, mine: &HashSet<u32>) -> bool {
            let mut named_any = false;
            subtree_names_only(dir, mine, &mut named_any) && named_any
        }

        fn subtree_names_only(dir: &Path, mine: &HashSet<u32>, named_any: &mut bool) -> bool {
            let Ok(procs) = std::fs::read_to_string(dir.join("cgroup.procs")) else {
                return false;
            };
            for pid in procs.lines().filter_map(|l| l.trim().parse::<u32>().ok()) {
                *named_any = true;
                if !mine.contains(&pid) {
                    return false;
                }
            }
            let Ok(entries) = std::fs::read_dir(dir) else {
                return false;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && !subtree_names_only(&path, mine, named_any) {
                    return false;
                }
            }
            true
        }

        fn read_trimmed(path: &Path) -> Option<String> {
            std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
        }

        fn read_u64(path: &Path) -> Option<u64> {
            read_trimmed(path)?.parse().ok()
        }

        /// One `key value` line of a cgroup `.stat` file, like `memory.stat`'s `inactive_file 4096`.
        fn stat_field(path: &Path, key: &str) -> Option<u64> {
            std::fs::read_to_string(path)
                .ok()?
                .lines()
                .find_map(|line| line.strip_prefix(key)?.strip_prefix(' ')?.trim().parse().ok())
        }

        /// Every directory from `root` down to `leaf` inclusive, root first. Cgroup resource
        /// controls apply hierarchically in both versions -- a leaf with no limit of its own can
        /// still be bounded by a slice or pod cgroup above it -- so a limit search has to walk
        /// the whole chain rather than trust the leaf alone.
        fn ancestors_from_root(root: &Path, leaf: &Path) -> Vec<PathBuf> {
            let mut dirs = vec![root.to_path_buf()];
            if let Ok(rel) = leaf.strip_prefix(root) {
                let mut acc = root.to_path_buf();
                for component in rel.components() {
                    acc = acc.join(component);
                    dirs.push(acc.clone());
                }
            }
            dirs
        }

        /// The tighter of two limits found walking a hierarchy -- `None` only when neither level
        /// set one.
        fn narrowest(acc: Option<f64>, cores: f64) -> Option<f64> {
            Some(acc.map_or(cores, |a| a.min(cores)))
        }

        /// This mount's own place in whatever filesystem backs it, from `mountinfo` -- `/` for an
        /// ordinary mount, or a container's own slice when the cgroup mount is a bind mount of one
        /// part of a larger cgroupfs rather than a namespace of its own.
        fn mount_source_root(mountinfo: &str, mount_point: &Path) -> Option<String> {
            let target = mount_point.to_str()?;
            mountinfo.lines().find_map(|line| {
                // `id parentId major:minor root mountPoint options... - fstype source superOptions`.
                // The optional-fields section before " - " can hold zero or more entries, so this
                // finds that separator rather than counting fields from the front.
                let (pre, _) = line.split_once(" - ")?;
                let mut fields = pre.split_whitespace();
                fields.next()?; // mount id
                fields.next()?; // parent id
                fields.next()?; // major:minor
                let root = fields.next()?;
                let point = fields.next()?;
                (point == target).then(|| unescape_mount_field(root))
            })
        }

        /// The kernel escapes space, tab, newline and backslash in a mountinfo field as their
        /// octal codes -- the only characters a path could hold that would otherwise break the
        /// format's own whitespace-separated fields.
        fn unescape_mount_field(field: &str) -> String {
            let mut out = String::with_capacity(field.len());
            let mut chars = field.chars();
            while let Some(c) = chars.next() {
                if c != '\\' {
                    out.push(c);
                    continue;
                }
                let escape: String = chars.by_ref().take(3).collect();
                match escape.as_str() {
                    "040" => out.push(' '),
                    "011" => out.push('\t'),
                    "012" => out.push('\n'),
                    "134" => out.push('\\'),
                    _ => {
                        out.push('\\');
                        out.push_str(&escape);
                    }
                }
            }
            out
        }

        /// `subpath` with the mount's own source root stripped, so joining what is left onto the
        /// local mount point lands in the right place even when the mount was not made at the
        /// filesystem's own root. A mount made at its root -- the ordinary, cgroup-namespaced
        /// case -- reports `/`, and stripping that changes nothing.
        fn strip_mount_root(subpath: &str, mountinfo: &str, mount_point: &Path) -> String {
            match mount_source_root(mountinfo, mount_point) {
                Some(source_root) if source_root != "/" => {
                    subpath.strip_prefix(&source_root).unwrap_or(subpath).to_string()
                }
                _ => subpath.to_string(),
            }
        }

        // -- cgroup v2: one unified hierarchy, this process's own place in it read off
        // `/proc/self/cgroup`'s single line -- inside a container's cgroup namespace that line is
        // always `0::/`, the same directory `ROOT` itself already names, so this is a no-op there
        // and only matters on a bare host with no cgroup namespace of its own.

        fn v2_dir(root: &Path, own_cgroup: &str, mountinfo: &str) -> PathBuf {
            match own_v2_subpath(own_cgroup) {
                Some(sub) => root.join(strip_mount_root(&sub, mountinfo, root).trim_start_matches('/')),
                None => root.to_path_buf(),
            }
        }

        fn own_v2_subpath(content: &str) -> Option<String> {
            content.lines().find_map(|line| {
                let mut parts = line.splitn(3, ':');
                parts.next()?;
                let controllers = parts.next()?;
                let path = parts.next()?;
                controllers.is_empty().then(|| path.to_string())
            })
        }

        /// `used_bytes` is this cgroup's own working set: `memory.current` less the file cache the
        /// kernel would reclaim first (`inactive_file`), which is what `kubectl top` reports and
        /// what pressure builds against. It is the leaf's own even when the limit is an
        /// ancestor's, because the caller wants this worker's figure, and an ancestor's aggregate
        /// holds its siblings too. With no limit anywhere, the limit is `None`.
        fn v2_memory(root: &Path, own_cgroup: &str, mountinfo: &str) -> Option<(u64, Option<u64>, PathBuf)> {
            let dir = v2_dir(root, own_cgroup, mountinfo);
            let limit = ancestors_from_root(root, &dir)
                .into_iter()
                .filter_map(|d| {
                    let max = read_trimmed(&d.join("memory.max"))?;
                    (max != "max").then(|| max.parse::<u64>().ok()).flatten()
                })
                .min();
            let used = read_u64(&dir.join("memory.current"))?;
            let reclaimable = stat_field(&dir.join("memory.stat"), "inactive_file").unwrap_or(0);
            Some((used.saturating_sub(reclaimable), limit, dir))
        }

        fn v2_cpu(root: &Path, own_cgroup: &str, mountinfo: &str) -> Option<(Duration, Option<f64>, PathBuf)> {
            let dir = v2_dir(root, own_cgroup, mountinfo);
            let stat = std::fs::read_to_string(dir.join("cpu.stat")).ok()?;
            let usage_usec: u64 = stat
                .lines()
                .find_map(|l| l.strip_prefix("usage_usec "))
                .and_then(|v| v.trim().parse().ok())?;
            let quota_limit = ancestors_from_root(root, &dir)
                .iter()
                .filter_map(|d| cpu_max_cores(&read_trimmed(&d.join("cpu.max"))?))
                .fold(None, narrowest);
            let cpuset_limit = cpuset_cores(&dir, "cpuset.cpus.effective", "cpuset.cpus");
            let limit_cores = cap_by_cpuset(quota_limit, cpuset_limit, raw_host_cores());
            Some((Duration::from_micros(usage_usec), limit_cores, dir))
        }

        /// `cpu.max`'s two fields, as a core count -- `None` when the first is `max`, meaning
        /// this level sets no quota of its own.
        fn cpu_max_cores(max: &str) -> Option<f64> {
            let mut parts = max.split_whitespace();
            let quota = parts.next()?;
            let period: u64 = parts.next()?.parse().ok()?;
            (quota != "max")
                .then(|| quota.parse::<u64>().ok())
                .flatten()
                .map(|q| q as f64 / period as f64)
        }

        /// A cpuset can bound a worker tighter than any CPU quota does -- Kubernetes' static CPU
        /// manager policy pins a Guaranteed pod to a fixed core count and sets no quota at all, so
        /// a quota-only reading would divide by every core the host has instead. `dir`'s own
        /// effective set already reflects any ancestor's own cpuset, the way a `.max`/`.limit`
        /// file does not, so this reads only the leaf rather than walking a hierarchy of its own.
        fn cpuset_cores(dir: &Path, effective_name: &str, plain_name: &str) -> Option<f64> {
            let list = read_trimmed(&dir.join(effective_name)).or_else(|| read_trimmed(&dir.join(plain_name)))?;
            count_cpuset_list(&list).map(|n| n as f64)
        }

        /// A cpuset core list as `cpuset.cpus`/`cpuset.cpus.effective` write it: comma separated,
        /// each entry a single core or an inclusive range (`0-3`). `None` for an empty list --
        /// unset, rather than zero cores.
        fn count_cpuset_list(list: &str) -> Option<usize> {
            let mut count = 0usize;
            let mut any = false;
            for token in list.trim().split(',').map(str::trim).filter(|t| !t.is_empty()) {
                any = true;
                match token.split_once('-') {
                    Some((start, end)) => {
                        let start: usize = start.parse().ok()?;
                        let end: usize = end.parse().ok()?;
                        count += end.checked_sub(start)?.checked_add(1)?;
                    }
                    None => {
                        token.parse::<usize>().ok()?;
                        count += 1;
                    }
                }
            }
            any.then_some(count)
        }

        /// The host's own online CPU count, ignoring any cpuset already narrowing what this
        /// process itself can run on. `host_cores` (the top-level one, via
        /// `available_parallelism`) asks the scheduler what this process specifically may use, so
        /// under a real cpuset restriction it already equals the cpuset's own count -- comparing
        /// a cpuset against that can never show it narrowing anything, real or not. Only
        /// [`cap_by_cpuset`]'s no-quota branch needs a count untouched by the very restriction it
        /// is trying to detect.
        fn raw_host_cores() -> f64 {
            let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
            if n > 0 { n as f64 } else { 1.0 }
        }

        /// The tighter of a quota-derived limit and a cpuset's own core count. With a quota
        /// present, this is an unconditional minimum: a cpuset caps what is actually schedulable
        /// no matter what the quota grants, so there is no "does it narrow anything" question to
        /// ask -- asking it against `host_cores` would ask the wrong thing anyway, since a real
        /// cpuset restriction already narrows `host_cores` to match, and a cpuset can never look
        /// like it narrows a count it already produced. With no quota, the cpuset is trusted as a
        /// limit only when it narrows below `raw_host_cores`: every cgroup with the controller
        /// enabled has an effective core list, including one naming every core the host has, and
        /// that is not a limit -- treating it as one would mark an ordinary unlimited worker as
        /// bounded on `cpuset_limit`'s say-so alone.
        fn cap_by_cpuset(quota_limit: Option<f64>, cpuset_limit: Option<f64>, raw_host_cores: f64) -> Option<f64> {
            match quota_limit {
                Some(quota) => match cpuset_limit {
                    Some(cpuset) => Some(quota.min(cpuset)),
                    None => Some(quota),
                },
                None => cpuset_limit.filter(|&cores| cores < raw_host_cores),
            }
        }

        // -- cgroup v1: one directory per controller, combined as `cpu,cpuacct` by every
        // container runtime and by most distributions' own layout. This process's own place
        // within each controller's hierarchy is read off `/proc/self/cgroup` the same way v2's
        // unified one is -- inside a container's cgroup namespace that is always the controller's
        // own root, so this only matters on a bare host or a systemd-managed service, where it is
        // what makes a unit's own `MemoryMax=`/`CPUQuota=` visible instead of the whole machine's.

        fn own_v1_subpath(content: &str, controller: &str) -> Option<String> {
            content.lines().find_map(|line| {
                let mut parts = line.splitn(3, ':');
                parts.next()?;
                let controllers = parts.next()?;
                let path = parts.next()?;
                controllers
                    .split(',')
                    .any(|c| c == controller)
                    .then(|| path.to_string())
            })
        }

        /// `mount_root` is where `controller`'s files live at the top -- `ROOT`'s own `memory`,
        /// say. This descends from there into this process's own place, the same way `v2_dir`
        /// does for the unified hierarchy.
        fn v1_controller_dir(mount_root: &Path, own_cgroup: &str, mountinfo: &str, controller: &str) -> PathBuf {
            match own_v1_subpath(own_cgroup, controller) {
                Some(sub) => mount_root.join(strip_mount_root(&sub, mountinfo, mount_root).trim_start_matches('/')),
                None => mount_root.to_path_buf(),
            }
        }

        /// `used` is the leaf's own working set, on the same terms [`v2_memory`] reads it. v1's
        /// `memory.stat` names the subtree-wide figure `total_inactive_file`, which is the one
        /// that matches `memory.usage_in_bytes`'s own subtree.
        fn v1_memory(root: &Path, own_cgroup: &str, mountinfo: &str) -> Option<(u64, Option<u64>, PathBuf)> {
            let mount = root.join("memory");
            let dir = v1_controller_dir(&mount, own_cgroup, mountinfo, "memory");
            // What cgroup v1 reports when nothing was ever set: near `i64::MAX`, rounded down to
            // a page. Nothing a real machine's memory could reach.
            const UNSET: u64 = 1 << 62;
            let limit = ancestors_from_root(&mount, &dir)
                .into_iter()
                .filter_map(|d| read_u64(&d.join("memory.limit_in_bytes")).filter(|raw| *raw < UNSET))
                .min();
            let used = read_u64(&dir.join("memory.usage_in_bytes"))?;
            let reclaimable = stat_field(&dir.join("memory.stat"), "total_inactive_file").unwrap_or(0);
            Some((used.saturating_sub(reclaimable), limit, dir))
        }

        fn v1_cpu(root: &Path, own_cgroup: &str, mountinfo: &str) -> Option<(Duration, Option<f64>, PathBuf)> {
            let combined = root.join("cpu,cpuacct");
            let acct_mount = if combined.join("cpuacct.usage").exists() {
                combined.clone()
            } else {
                root.join("cpuacct")
            };
            let cpu_mount = if combined.join("cpu.cfs_quota_us").exists() {
                combined
            } else {
                root.join("cpu")
            };
            let acct_dir = v1_controller_dir(&acct_mount, own_cgroup, mountinfo, "cpuacct");
            let cpu_dir = v1_controller_dir(&cpu_mount, own_cgroup, mountinfo, "cpu");
            let usage_ns = read_u64(&acct_dir.join("cpuacct.usage"))?;
            let quota_limit = ancestors_from_root(&cpu_mount, &cpu_dir)
                .iter()
                .filter_map(|d| {
                    let quota: i64 = read_trimmed(&d.join("cpu.cfs_quota_us"))?.parse().ok()?;
                    let period = read_u64(&d.join("cpu.cfs_period_us"))?;
                    (quota > 0).then(|| quota as f64 / period as f64)
                })
                .fold(None, narrowest);
            let cpuset_dir = v1_controller_dir(&root.join("cpuset"), own_cgroup, mountinfo, "cpuset");
            let cpuset_limit = cpuset_cores(&cpuset_dir, "cpuset.effective_cpus", "cpuset.cpus");
            let limit_cores = cap_by_cpuset(quota_limit, cpuset_limit, raw_host_cores());
            Some((Duration::from_nanos(usage_ns), limit_cores, acct_dir))
        }

        #[cfg(test)]
        mod tests {
            use super::*;

            fn dir(name: &str) -> std::path::PathBuf {
                let d = std::env::temp_dir().join(format!("smeltr-cgroup-test-{name}-{}", std::process::id()));
                let _ = std::fs::remove_dir_all(&d);
                std::fs::create_dir_all(&d).unwrap();
                d
            }

            /// An empty `own_cgroup`: `own_v2_subpath`/`own_v1_subpath` find no matching line and
            /// fall back to the mount root unchanged -- what every fixture below wants except the
            /// ones proving subpath descent itself, which build their own instead. An empty
            /// `mountinfo` behaves the same way for `strip_mount_root`: no matching mount, no fact
            /// to strip.
            const AT_ROOT: &str = "";

            #[test]
            fn a_v2_cgroup_with_no_limit_reports_usage_and_no_ceiling() {
                let root = dir("v2-unlimited");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory\n").unwrap();
                std::fs::write(root.join("memory.current"), "104857600\n").unwrap();
                std::fs::write(root.join("memory.max"), "max\n").unwrap();
                std::fs::write(root.join("cpu.stat"), "usage_usec 2000000\nnr_periods 0\n").unwrap();
                std::fs::write(root.join("cpu.max"), "max 100000\n").unwrap();

                assert_eq!(memory(&root, AT_ROOT, AT_ROOT), Some((104_857_600, None, root.clone())));
                assert_eq!(
                    cpu(&root, AT_ROOT, AT_ROOT),
                    Some((Duration::from_micros(2_000_000), None, root.clone()))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v2_cgroup_with_a_quota_reports_it_in_cores() {
                let root = dir("v2-limited");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory\n").unwrap();
                std::fs::write(root.join("memory.current"), "50000000\n").unwrap();
                std::fs::write(root.join("memory.max"), "536870912\n").unwrap();
                std::fs::write(root.join("cpu.stat"), "usage_usec 1500000\n").unwrap();
                // 200000/100000 = a two-core quota.
                std::fs::write(root.join("cpu.max"), "200000 100000\n").unwrap();

                assert_eq!(
                    memory(&root, AT_ROOT, AT_ROOT),
                    Some((50_000_000, Some(536_870_912), root.clone()))
                );
                assert_eq!(
                    cpu(&root, AT_ROOT, AT_ROOT),
                    Some((Duration::from_micros(1_500_000), Some(2.0), root.clone()))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            /// File cache the kernel reclaims before it would refuse an allocation is not memory a
            /// worker needs, so a worker that streamed a large file does not read as full.
            #[test]
            fn a_v2_cgroups_memory_is_its_working_set() {
                let root = dir("v2-working-set");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory\n").unwrap();
                std::fs::write(root.join("memory.current"), "300000000\n").unwrap();
                std::fs::write(root.join("memory.max"), "536870912\n").unwrap();
                std::fs::write(
                    root.join("memory.stat"),
                    "anon 90000000\nfile 210000000\nactive_file 10000000\ninactive_file 200000000\n",
                )
                .unwrap();

                assert_eq!(
                    memory(&root, AT_ROOT, AT_ROOT),
                    Some((100_000_000, Some(536_870_912), root.clone()))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            /// v1 names the leaf-only figure `inactive_file` and the subtree's `total_inactive_file`;
            /// `memory.usage_in_bytes` is the subtree's, so the subtree's is the one to take off.
            #[test]
            fn a_v1_cgroups_memory_is_its_working_set() {
                let root = dir("v1-working-set");
                std::fs::create_dir_all(root.join("memory")).unwrap();
                std::fs::write(root.join("memory/memory.usage_in_bytes"), "300000000\n").unwrap();
                std::fs::write(root.join("memory/memory.limit_in_bytes"), "536870912\n").unwrap();
                std::fs::write(
                    root.join("memory/memory.stat"),
                    "inactive_file 1000\ntotal_inactive_file 200000000\n",
                )
                .unwrap();

                assert_eq!(
                    memory(&root, AT_ROOT, AT_ROOT),
                    Some((100_000_000, Some(536_870_912), root.join("memory")))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn cap_by_host_memory_drops_a_ceiling_at_or_above_the_host_total() {
                assert_eq!(
                    cap_by_host_memory(Some(64_000_000_000), Some(64_000_000_000)),
                    None,
                    "exactly the host total is still unreachable, not a limit"
                );
                assert_eq!(cap_by_host_memory(Some(1_000_000_000_000), Some(64_000_000_000)), None);
            }

            #[test]
            fn cap_by_host_memory_keeps_a_ceiling_below_the_host_total() {
                assert_eq!(
                    cap_by_host_memory(Some(32_000_000_000), Some(64_000_000_000)),
                    Some(32_000_000_000)
                );
            }

            #[test]
            fn cap_by_host_memory_leaves_an_absent_limit_or_host_total_alone() {
                assert_eq!(cap_by_host_memory(None, Some(64_000_000_000)), None);
                assert_eq!(cap_by_host_memory(Some(32_000_000_000), None), Some(32_000_000_000));
            }

            /// A `MemoryMax=` copied from a bigger box, or any other ceiling above what this
            /// machine physically has: no real host has anywhere near this much memory, so the
            /// cap applies regardless of the test machine's own real total -- unlike
            /// `cap_by_host_memory`'s unit tests, this exercises `memory()`'s real
            /// `host_memory_total()` rather than a synthetic one.
            #[test]
            fn a_v2_memory_ceiling_no_real_host_could_reach_is_not_treated_as_a_limit() {
                let root = dir("v2-memory-oversized");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory\n").unwrap();
                std::fs::write(root.join("memory.current"), "1024\n").unwrap();
                std::fs::write(root.join("memory.max"), "18446744073709551000\n").unwrap();

                assert_eq!(
                    memory(&root, AT_ROOT, AT_ROOT).and_then(|(_, limit, _)| limit),
                    None,
                    "a ceiling no real host could reach is not a limit"
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v2_cgroup_reads_this_processs_own_subpath_not_the_unified_root() {
                // A bare host running several services under one unified hierarchy: the root's
                // own files would answer for the whole machine, not this one service.
                let root = dir("v2-subpath");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory\n").unwrap();
                std::fs::write(root.join("memory.current"), "999999999\n").unwrap();
                std::fs::write(root.join("memory.max"), "max\n").unwrap();
                let service = root.join("system.slice/smeltr.service");
                std::fs::create_dir_all(&service).unwrap();
                std::fs::write(service.join("memory.current"), "60000000\n").unwrap();
                std::fs::write(service.join("memory.max"), "134217728\n").unwrap();
                std::fs::write(service.join("cpu.stat"), "usage_usec 4000000\n").unwrap();
                std::fs::write(service.join("cpu.max"), "max 100000\n").unwrap();

                let own_cgroup = "0::/system.slice/smeltr.service\n";
                assert_eq!(
                    memory(&root, own_cgroup, AT_ROOT),
                    Some((60_000_000, Some(134_217_728), service.clone())),
                    "read the service's own files, not the unified root's"
                );
                assert_eq!(
                    cpu(&root, own_cgroup, AT_ROOT),
                    Some((Duration::from_micros(4_000_000), None, service))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v2_cgroup_inherits_a_tighter_limit_from_an_ancestor() {
                // Kubernetes-shaped: a slice above the pod sets the ceiling, and the pod's own
                // leaf sets none -- the leaf alone would misread this pod as unlimited.
                let root = dir("v2-ancestor-limit");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory\n").unwrap();
                std::fs::write(root.join("memory.current"), "1\n").unwrap();
                std::fs::write(root.join("memory.max"), "max\n").unwrap();
                std::fs::write(root.join("cpu.stat"), "usage_usec 1\n").unwrap();
                std::fs::write(root.join("cpu.max"), "max 100000\n").unwrap();
                let slice = root.join("kubepods.slice");
                std::fs::create_dir_all(&slice).unwrap();
                // The slice's own aggregate, a sibling pod's usage included: not this pod's.
                std::fs::write(slice.join("memory.current"), "900000000\n").unwrap();
                std::fs::write(slice.join("memory.max"), "1073741824\n").unwrap();
                // 400000/100000 = a four-core quota.
                std::fs::write(slice.join("cpu.max"), "400000 100000\n").unwrap();
                let pod = slice.join("pod-abc");
                std::fs::create_dir_all(&pod).unwrap();
                std::fs::write(pod.join("memory.current"), "500000000\n").unwrap();
                std::fs::write(pod.join("memory.max"), "max\n").unwrap();
                std::fs::write(pod.join("cpu.stat"), "usage_usec 2000000\n").unwrap();
                std::fs::write(pod.join("cpu.max"), "max 100000\n").unwrap();

                let own_cgroup = "0::/kubepods.slice/pod-abc\n";
                assert_eq!(
                    memory(&root, own_cgroup, AT_ROOT),
                    Some((500_000_000, Some(1_073_741_824), pod.clone())),
                    "the pod's own memory.max is unset, but its parent slice's ceiling still \
                     applies -- measured against this pod's own usage, not the slice's aggregate"
                );
                assert_eq!(
                    cpu(&root, own_cgroup, AT_ROOT),
                    Some((Duration::from_micros(2_000_000), Some(4.0), pod)),
                    "same for the slice's own CPUQuota="
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v1_cgroup_with_no_limit_set_reads_as_unlimited() {
                let root = dir("v1-unlimited");
                std::fs::create_dir_all(root.join("memory")).unwrap();
                std::fs::create_dir_all(root.join("cpu,cpuacct")).unwrap();
                std::fs::write(root.join("memory/memory.usage_in_bytes"), "10000000\n").unwrap();
                std::fs::write(root.join("memory/memory.limit_in_bytes"), "9223372036854771712\n").unwrap();
                std::fs::write(root.join("cpu,cpuacct/cpuacct.usage"), "3000000000\n").unwrap();
                std::fs::write(root.join("cpu,cpuacct/cpu.cfs_quota_us"), "-1\n").unwrap();
                std::fs::write(root.join("cpu,cpuacct/cpu.cfs_period_us"), "100000\n").unwrap();

                assert_eq!(
                    memory(&root, AT_ROOT, AT_ROOT),
                    Some((10_000_000, None, root.join("memory")))
                );
                assert_eq!(
                    cpu(&root, AT_ROOT, AT_ROOT),
                    Some((Duration::from_nanos(3_000_000_000), None, root.join("cpu,cpuacct")))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v1_cgroup_with_a_quota_reports_it_in_cores() {
                let root = dir("v1-limited");
                std::fs::create_dir_all(root.join("memory")).unwrap();
                std::fs::create_dir_all(root.join("cpu,cpuacct")).unwrap();
                std::fs::write(root.join("memory/memory.usage_in_bytes"), "20000000\n").unwrap();
                std::fs::write(root.join("memory/memory.limit_in_bytes"), "268435456\n").unwrap();
                std::fs::write(root.join("cpu,cpuacct/cpuacct.usage"), "1000000000\n").unwrap();
                // 50000/100000 = half a core.
                std::fs::write(root.join("cpu,cpuacct/cpu.cfs_quota_us"), "50000\n").unwrap();
                std::fs::write(root.join("cpu,cpuacct/cpu.cfs_period_us"), "100000\n").unwrap();

                assert_eq!(
                    memory(&root, AT_ROOT, AT_ROOT),
                    Some((20_000_000, Some(268_435_456), root.join("memory")))
                );
                assert_eq!(
                    cpu(&root, AT_ROOT, AT_ROOT),
                    Some((Duration::from_nanos(1_000_000_000), Some(0.5), root.join("cpu,cpuacct")))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            /// A ceiling set explicitly -- unlike the sentinel `a_v1_cgroup_with_no_limit_set_...`
            /// covers -- but still above any real host's memory, the way a `MemoryMax=` copied
            /// from a bigger box would be. Far below the `1 << 62` sentinel cutoff, so this is the
            /// gap that cutoff alone cannot close.
            #[test]
            fn a_v1_memory_ceiling_no_real_host_could_reach_is_not_treated_as_a_limit() {
                let root = dir("v1-memory-oversized");
                std::fs::create_dir_all(root.join("memory")).unwrap();
                std::fs::write(root.join("memory/memory.usage_in_bytes"), "1024\n").unwrap();
                // 100 TiB: nowhere near any real host's memory, but nowhere near `1 << 62`
                // (~4.6 EiB) either -- the gap that cutoff alone cannot close.
                std::fs::write(root.join("memory/memory.limit_in_bytes"), "109951162777600\n").unwrap();

                assert_eq!(
                    memory(&root, AT_ROOT, AT_ROOT).and_then(|(_, limit, _)| limit),
                    None,
                    "a ceiling no real host could reach is not a limit"
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            /// A review gap: a limit found above the leaf must be read against *that ancestor's
            /// own* usage, since v1 rolls a directory's usage up over its whole subtree the same
            /// way v2 does -- pairing the tighter ceiling with the leaf's narrower figure would
            /// understate how close the real, shared ceiling is.
            #[test]
            fn a_v1_cgroup_inherits_a_tighter_limit_from_an_ancestor() {
                let root = dir("v1-ancestor-limit");
                std::fs::create_dir_all(root.join("memory")).unwrap();
                let service = "system.slice/smeltr.service";
                let mem_dir = root.join("memory").join(service);
                std::fs::create_dir_all(&mem_dir).unwrap();
                let slice_dir = root.join("memory/system.slice");
                // The slice's own aggregate, a sibling service's usage included: not this service's.
                std::fs::write(slice_dir.join("memory.usage_in_bytes"), "900000000\n").unwrap();
                std::fs::write(slice_dir.join("memory.limit_in_bytes"), "1073741824\n").unwrap();
                std::fs::write(mem_dir.join("memory.usage_in_bytes"), "500000000\n").unwrap();
                std::fs::write(mem_dir.join("memory.limit_in_bytes"), "9223372036854771712\n").unwrap();

                let own_cgroup = format!("4:memory:/{service}\n");
                assert_eq!(
                    memory(&root, &own_cgroup, AT_ROOT),
                    Some((500_000_000, Some(1_073_741_824), mem_dir)),
                    "the service's own limit is unset, but the slice's ceiling applies to the service's own usage"
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v1_cgroup_reads_this_processs_own_subpath_not_the_controllers_root() {
                // The exact gap a review caught: a systemd service on a v1 host has its own
                // `MemoryMax=`/`CPUQuota=` several directories below the `memory`/`cpu,cpuacct`
                // mount root, and reading the root itself would answer for the whole machine.
                let root = dir("v1-subpath");
                let service = "system.slice/smeltr.service";
                let mem_dir = root.join("memory").join(service);
                let cpu_dir = root.join("cpu,cpuacct").join(service);
                std::fs::create_dir_all(&mem_dir).unwrap();
                std::fs::create_dir_all(&cpu_dir).unwrap();
                std::fs::write(root.join("memory/memory.usage_in_bytes"), "999999999\n").unwrap();
                std::fs::write(root.join("memory/memory.limit_in_bytes"), "9223372036854771712\n").unwrap();
                std::fs::write(mem_dir.join("memory.usage_in_bytes"), "45000000\n").unwrap();
                std::fs::write(mem_dir.join("memory.limit_in_bytes"), "268435456\n").unwrap();
                // A real cgroupfs carries a controller's interface files at every level of the
                // hierarchy, not just the leaf -- the mount root's own copies are written here
                // too, with different numbers, so the assertions below can tell which ones answered.
                std::fs::write(root.join("cpu,cpuacct/cpuacct.usage"), "999999999\n").unwrap();
                std::fs::write(root.join("cpu,cpuacct/cpu.cfs_quota_us"), "-1\n").unwrap();
                std::fs::write(root.join("cpu,cpuacct/cpu.cfs_period_us"), "100000\n").unwrap();
                std::fs::write(cpu_dir.join("cpuacct.usage"), "2000000000\n").unwrap();
                std::fs::write(cpu_dir.join("cpu.cfs_quota_us"), "100000\n").unwrap();
                std::fs::write(cpu_dir.join("cpu.cfs_period_us"), "100000\n").unwrap();

                let own_cgroup = format!("5:cpu,cpuacct:/{service}\n4:memory:/{service}\n");
                assert_eq!(
                    memory(&root, &own_cgroup, AT_ROOT),
                    Some((45_000_000, Some(268_435_456), mem_dir)),
                    "read the service's own files, not the memory controller's root"
                );
                assert_eq!(
                    cpu(&root, &own_cgroup, AT_ROOT),
                    Some((Duration::from_nanos(2_000_000_000), Some(1.0), cpu_dir))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v1_container_without_its_own_cgroup_namespace_is_still_read_correctly() {
                // The controller mount is a bind mount of the container's own slice on the host,
                // not a cgroup namespace of its own -- `/proc/self/cgroup` reports the same
                // un-remapped path the mount's own source root already names, so joining both
                // would double it and miss the files entirely.
                let root = dir("v1-bind-mount");
                std::fs::create_dir_all(root.join("memory")).unwrap();
                std::fs::write(root.join("memory/memory.usage_in_bytes"), "30000000\n").unwrap();
                std::fs::write(root.join("memory/memory.limit_in_bytes"), "268435456\n").unwrap();

                let own_cgroup = "4:memory:/docker/abc123\n";
                let mountinfo = format!(
                    "25 30 0:23 /docker/abc123 {} rw,relatime - cgroup cgroup rw,memory\n",
                    root.join("memory").display()
                );
                assert_eq!(
                    memory(&root, own_cgroup, &mountinfo),
                    Some((30_000_000, Some(268_435_456), root.join("memory"))),
                    "the mount's own source root must be stripped before joining, not doubled"
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_directory_with_neither_layout_answers_nothing() {
                let root = dir("neither");
                assert_eq!(memory(&root, AT_ROOT, AT_ROOT), None);
                assert_eq!(cpu(&root, AT_ROOT, AT_ROOT), None);
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v1_cpu_mount_split_apart_from_cpuacct_is_still_read() {
                let root = dir("v1-split");
                std::fs::create_dir_all(root.join("memory")).unwrap();
                std::fs::create_dir_all(root.join("cpu")).unwrap();
                std::fs::create_dir_all(root.join("cpuacct")).unwrap();
                std::fs::write(root.join("memory/memory.usage_in_bytes"), "1\n").unwrap();
                std::fs::write(root.join("memory/memory.limit_in_bytes"), "1\n").unwrap();
                std::fs::write(root.join("cpuacct/cpuacct.usage"), "500000000\n").unwrap();
                std::fs::write(root.join("cpu/cpu.cfs_quota_us"), "100000\n").unwrap();
                std::fs::write(root.join("cpu/cpu.cfs_period_us"), "100000\n").unwrap();

                assert_eq!(
                    cpu(&root, AT_ROOT, AT_ROOT),
                    Some((Duration::from_nanos(500_000_000), Some(1.0), root.join("cpuacct")))
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn own_v2_subpath_reads_the_unified_lines_empty_controller_field() {
                // A hybrid host lists v1 controllers above the unified line; the one with no
                // controller name is always the v2 hierarchy, whichever position it is in.
                let content = "12:pids:/user.slice\n0::/user.slice/user-1000.slice/session.scope\n";
                assert_eq!(
                    own_v2_subpath(content),
                    Some("/user.slice/user-1000.slice/session.scope".to_string())
                );
            }

            #[test]
            fn own_v2_subpath_is_root_inside_a_containers_own_namespace() {
                assert_eq!(own_v2_subpath("0::/\n"), Some("/".to_string()));
            }

            #[test]
            fn own_v1_subpath_matches_a_combined_controller_list() {
                let content = "5:cpu,cpuacct:/system.slice/smeltr.service\n4:memory:/system.slice/smeltr.service\n";
                assert_eq!(
                    own_v1_subpath(content, "cpuacct"),
                    Some("/system.slice/smeltr.service".to_string()),
                    "cpuacct is one of the names on a combined cpu,cpuacct line"
                );
                assert_eq!(
                    own_v1_subpath(content, "memory"),
                    Some("/system.slice/smeltr.service".to_string())
                );
            }

            #[test]
            fn own_v1_subpath_matches_a_lone_controller_name() {
                let content = "3:cpuacct:/docker/abc123\n";
                assert_eq!(own_v1_subpath(content, "cpuacct"), Some("/docker/abc123".to_string()));
                assert_eq!(
                    own_v1_subpath(content, "cpu"),
                    None,
                    "cpu is not named on this line, mounted apart"
                );
            }

            #[test]
            fn mount_source_root_finds_the_matching_mount_point() {
                let content = "25 30 0:23 / /sys/fs/cgroup rw,nosuid - cgroup2 cgroup2 rw\n\
                    26 30 0:24 /docker/abc123 /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n";
                assert_eq!(
                    mount_source_root(content, Path::new("/sys/fs/cgroup/memory")),
                    Some("/docker/abc123".to_string())
                );
                assert_eq!(
                    mount_source_root(content, Path::new("/sys/fs/cgroup")),
                    Some("/".to_string())
                );
                assert_eq!(mount_source_root(content, Path::new("/no/such/mount")), None);
            }

            #[test]
            fn unescape_mount_field_decodes_the_kernels_own_octal_escapes() {
                assert_eq!(unescape_mount_field(r"/a\040b"), "/a b");
                assert_eq!(unescape_mount_field(r"/a\134b"), "/a\\b");
                assert_eq!(unescape_mount_field("/plain/path"), "/plain/path");
            }

            #[test]
            fn is_exclusive_when_cgroup_procs_names_only_this_processs_own_tree() {
                let root = dir("exclusive-yes");
                std::fs::write(root.join("cgroup.procs"), "10\n11\n").unwrap();
                let mine = HashSet::from([10, 11, 12]);
                assert!(is_exclusive(&root, &mine));
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn is_not_exclusive_when_cgroup_procs_names_an_unrelated_pid() {
                let root = dir("exclusive-no");
                std::fs::write(root.join("cgroup.procs"), "10\n999\n").unwrap();
                let mine = HashSet::from([10]);
                assert!(!is_exclusive(&root, &mine));
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn is_not_exclusive_when_cgroup_procs_cannot_be_read() {
                let root = dir("exclusive-unreadable");
                assert!(!is_exclusive(&root, &HashSet::new()));
                let _ = std::fs::remove_dir_all(&root);
            }

            /// The v2 root cgroup, and any v1 cgroup, can hold processes directly while a child
            /// cgroup holds more -- `memory.current`/`cpu.stat` roll both into `root`'s own
            /// aggregate, so a check of `root`'s `cgroup.procs` alone would miss the stranger.
            #[test]
            fn is_not_exclusive_when_a_descendant_cgroup_names_an_unrelated_pid() {
                let root = dir("exclusive-descendant-no");
                std::fs::write(root.join("cgroup.procs"), "10\n").unwrap();
                let child = root.join("other.scope");
                std::fs::create_dir_all(&child).unwrap();
                std::fs::write(child.join("cgroup.procs"), "999\n").unwrap();
                let mine = HashSet::from([10]);
                assert!(!is_exclusive(&root, &mine));
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn is_exclusive_when_every_descendant_cgroup_names_only_this_processs_own_tree() {
                let root = dir("exclusive-descendant-yes");
                std::fs::write(root.join("cgroup.procs"), "10\n").unwrap();
                let child = root.join("runtime.scope");
                std::fs::create_dir_all(&child).unwrap();
                std::fs::write(child.join("cgroup.procs"), "11\n").unwrap();
                let mine = HashSet::from([10, 11]);
                assert!(is_exclusive(&root, &mine));
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn count_cpuset_list_sums_singles_and_ranges() {
                assert_eq!(count_cpuset_list("0-1,4"), Some(3));
                assert_eq!(count_cpuset_list("0-3"), Some(4));
                assert_eq!(count_cpuset_list("2"), Some(1));
                assert_eq!(count_cpuset_list(""), None, "empty means unset, not zero cores");
            }

            /// Every cgroup with the controller enabled has an effective core list, including one
            /// restricting nothing, and that must not read as a real limit -- it would mark an
            /// ordinary unlimited worker as bounded.
            #[test]
            fn cap_by_cpuset_ignores_a_cpuset_naming_every_host_core() {
                assert_eq!(cap_by_cpuset(None, Some(8.0), 8.0), None);
                assert_eq!(
                    cap_by_cpuset(Some(4.0), Some(8.0), 8.0),
                    Some(4.0),
                    "an unrelated quota still applies on its own"
                );
            }

            #[test]
            fn cap_by_cpuset_applies_a_genuinely_narrower_set() {
                assert_eq!(cap_by_cpuset(None, Some(2.0), 8.0), Some(2.0));
                assert_eq!(cap_by_cpuset(Some(8.0), Some(2.0), 8.0), Some(2.0));
            }

            /// A second review gap: `raw_host_cores` (unlike the top-level `host_cores`) is not
            /// itself narrowed by this process's own cpuset, so a real cpuset that happens to
            /// equal what this process could already schedule still caps a quota above it -- an
            /// eight-core quota next to a genuine two-CPU cpuset is two cores, not eight, whether
            /// or not two also happens to be every core this process could otherwise use.
            #[test]
            fn cap_by_cpuset_applies_even_when_it_matches_the_schedulable_core_count() {
                assert_eq!(cap_by_cpuset(Some(8.0), Some(2.0), 2.0), Some(2.0));
            }

            #[test]
            fn a_v2_cpuset_naming_every_host_core_is_not_treated_as_a_limit() {
                let root = dir("v2-cpuset-unrestricted");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory cpuset\n").unwrap();
                std::fs::write(root.join("memory.current"), "1\n").unwrap();
                std::fs::write(root.join("memory.max"), "max\n").unwrap();
                std::fs::write(root.join("cpu.stat"), "usage_usec 1\n").unwrap();
                std::fs::write(root.join("cpu.max"), "max 100000\n").unwrap();
                // `raw_host_cores`, not `host_cores`: the point of this fixture is every core the
                // machine has, not whatever this process's own affinity already narrows it to.
                let cores = raw_host_cores() as usize;
                std::fs::write(
                    root.join("cpuset.cpus.effective"),
                    format!("0-{}\n", cores.saturating_sub(1)),
                )
                .unwrap();

                assert_eq!(
                    cpu(&root, AT_ROOT, AT_ROOT).and_then(|(_, limit, _)| limit),
                    None,
                    "a cpuset naming every host core restricts nothing"
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v2_cpuset_narrower_than_the_host_becomes_the_limit_with_no_quota_set() {
                // Kubernetes' static CPU manager policy: a fixed cpuset and no CFS quota at all.
                let root = dir("v2-cpuset-only");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory cpuset\n").unwrap();
                std::fs::write(root.join("memory.current"), "1\n").unwrap();
                std::fs::write(root.join("memory.max"), "max\n").unwrap();
                std::fs::write(root.join("cpu.stat"), "usage_usec 1000000\n").unwrap();
                std::fs::write(root.join("cpu.max"), "max 100000\n").unwrap();
                std::fs::write(root.join("cpuset.cpus.effective"), "0-1\n").unwrap();

                assert_eq!(
                    cpu(&root, AT_ROOT, AT_ROOT),
                    Some((Duration::from_micros(1_000_000), Some(2.0), root.clone())),
                    "no quota is set, so the cpuset's own two cores are the only limit there is"
                );
                let _ = std::fs::remove_dir_all(&root);
            }

            #[test]
            fn a_v2_cpuset_tighter_than_the_quota_wins() {
                let root = dir("v2-cpuset-tighter");
                std::fs::write(root.join("cgroup.controllers"), "cpu memory cpuset\n").unwrap();
                std::fs::write(root.join("memory.current"), "1\n").unwrap();
                std::fs::write(root.join("memory.max"), "max\n").unwrap();
                std::fs::write(root.join("cpu.stat"), "usage_usec 1\n").unwrap();
                // An eight-core quota, but only two CPUs are actually schedulable.
                std::fs::write(root.join("cpu.max"), "800000 100000\n").unwrap();
                std::fs::write(root.join("cpuset.cpus.effective"), "0-1\n").unwrap();

                assert_eq!(
                    cpu(&root, AT_ROOT, AT_ROOT).and_then(|(_, limit, _)| limit),
                    Some(2.0),
                    "a worker saturating both permitted CPUs is 100% of two, not 25% of eight"
                );
                let _ = std::fs::remove_dir_all(&root);
            }
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::{CpuReading, CpuSource, MemoryReading, host_cores};
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
    };
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX2,
    };
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
    use windows_sys::Win32::System::Threading::{GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

    /// Job-object CPU and memory limits -- what a Windows container's own limits are -- are not
    /// read yet. This measures this process's tree against the whole host, the same as any
    /// unlimited machine.
    pub fn sample(carried: &mut Carried) -> (CpuReading, MemoryReading) {
        let tree = own_process_tree();
        let cpu = CpuReading {
            time: tree_cpu_time(&tree, carried),
            at: std::time::Instant::now(),
            source: CpuSource::Tree,
            limit_cores: host_cores(),
            limited: false,
        };
        let memory = MemoryReading {
            used_bytes: tree_memory(&tree),
            limit_bytes: host_memory_total(),
            limited: false,
        };
        (cpu, memory)
    }

    /// A handle, closed on drop. One opened on a process answers for that process.
    struct Handle(HANDLE);

    impl Handle {
        fn open(pid: u32) -> Option<Handle> {
            // SAFETY: no handle is inherited, and a null return is checked.
            let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
            (!handle.is_null()).then_some(Handle(handle))
        }

        /// `(creation, kernel + user)`, both in 100-nanosecond ticks.
        fn times(&self) -> Option<(u64, u64)> {
            let mut creation = FILETIME::default();
            let mut exit = FILETIME::default();
            let mut kernel = FILETIME::default();
            let mut user = FILETIME::default();
            // SAFETY: a live handle opened for query, and four out-parameters of the type asked for.
            let ok = unsafe { GetProcessTimes(self.0, &mut creation, &mut exit, &mut kernel, &mut user) };
            (ok != 0).then(|| (ticks(creation), ticks(kernel) + ticks(user)))
        }

        /// Resident memory no other process shares, measured against physical memory the way the
        /// host total is. Working set would count a binary and the files a supervisor and its
        /// runtime both map once in each of them; private bytes would count commit that is paged
        /// out. Windows before 10 21H2 has no private working set, and gives the whole working set.
        fn private_working_set(&self) -> Option<u64> {
            let mut counters = PROCESS_MEMORY_COUNTERS_EX2 {
                cb: size_of::<PROCESS_MEMORY_COUNTERS_EX2>() as u32,
                ..Default::default()
            };
            // SAFETY: the extended struct begins with the plain one and `cb` names its own size,
            // which is how the call is told the larger one was passed.
            let ok = unsafe {
                GetProcessMemoryInfo(
                    self.0,
                    (&raw mut counters).cast::<PROCESS_MEMORY_COUNTERS>(),
                    counters.cb,
                )
            };
            if ok != 0 {
                return Some(counters.PrivateWorkingSetSize as u64);
            }
            let mut plain = PROCESS_MEMORY_COUNTERS {
                cb: size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
                ..Default::default()
            };
            // SAFETY: as above, with the plain struct.
            let ok = unsafe { GetProcessMemoryInfo(self.0, &raw mut plain, plain.cb) };
            (ok != 0).then_some(plain.WorkingSetSize as u64)
        }
    }

    impl Drop for Handle {
        fn drop(&mut self) {
            // SAFETY: opened in `open` and closed once.
            unsafe { CloseHandle(self.0) };
        }
    }

    /// FILETIME is 100-nanosecond ticks since an epoch nothing here cares about: only a
    /// difference, or an order, between two readings is ever used.
    fn ticks(ft: FILETIME) -> u64 {
        ((ft.dwHighDateTime as u64) << 32) | ft.dwLowDateTime as u64
    }

    /// This process and every descendant of it, by the parent each process names. Windows does
    /// not reparent an orphan, and reuses its parent's pid, so a process older than the one its
    /// parent pid now names is somebody else's child (`super::descendants`).
    pub(super) fn own_process_tree() -> Vec<u32> {
        let mut entries = Vec::new();
        // SAFETY: a process snapshot takes no process id, and an invalid handle is checked.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
        if snapshot != INVALID_HANDLE_VALUE {
            let snapshot = Handle(snapshot);
            let mut entry = PROCESSENTRY32W {
                dwSize: size_of::<PROCESSENTRY32W>() as u32,
                ..Default::default()
            };
            // SAFETY: a live snapshot handle, and an entry whose `dwSize` names its own size.
            let mut more = unsafe { Process32FirstW(snapshot.0, &mut entry) } != 0;
            while more {
                entries.push((entry.th32ProcessID, entry.th32ParentProcessID));
                // SAFETY: as above.
                more = unsafe { Process32NextW(snapshot.0, &mut entry) } != 0;
            }
        }
        let created = |pid: u32| Handle::open(pid)?.times().map(|(creation, _)| creation);
        super::descendants(std::process::id(), &entries, created)
            .into_iter()
            .collect()
    }

    pub use super::Carried;

    /// Every process in `tree`'s CPU time, and what `carried` holds of those that have left it.
    /// `None` when any process could not be read, which leaves `carried` as it was.
    pub(super) fn tree_cpu_time(tree: &[u32], carried: &mut Carried) -> Option<std::time::Duration> {
        let live = tree
            .iter()
            .map(|pid| {
                let (created, used) = Handle::open(*pid)?.times()?;
                Some(((*pid, created), used))
            })
            .collect::<Option<_>>()?;
        Some(std::time::Duration::from_nanos(carried.total(live) * 100))
    }

    pub(super) fn tree_memory(tree: &[u32]) -> u64 {
        tree.iter()
            .filter_map(|pid| Handle::open(*pid)?.private_working_set())
            .sum()
    }

    fn host_memory_total() -> Option<u64> {
        let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
        status.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
        let ok = unsafe { GlobalMemoryStatusEx(&mut status) };
        (ok != 0).then_some(status.ullTotalPhys)
    }
}

/// What a Windows tree's CPU total carries between samples. A process takes its time with it
/// when it exits and nothing inherits it, so the time last read for each process that has left
/// is kept here: a departed runtime's past stays counted, and one process's growth cannot hide
/// another's leaving. Only the leaver's time since the last sample is lost.
#[cfg(any(windows, test))]
#[derive(Default)]
pub struct Carried {
    /// Each process's CPU time at the last sample, by pid and creation time, since a pid is reused.
    last: std::collections::HashMap<(u32, u64), u64>,
    departed: u64,
}

#[cfg(any(windows, test))]
impl Carried {
    /// The tree's total, given each live process's CPU time now.
    fn total(&mut self, live: std::collections::HashMap<(u32, u64), u64>) -> u64 {
        for (process, used) in self.last.drain() {
            if !live.contains_key(&process) {
                self.departed += used;
            }
        }
        let total = self.departed + live.values().sum::<u64>();
        self.last = live;
        total
    }
}

/// `root` and every process descending from it, given each process's `(pid, parent pid)`. A
/// child counts only when `created` says it is no older than the parent it names: a parent pid
/// outlives its process on Windows, and a pid reused since then would otherwise adopt a
/// stranger's children. A process `created` cannot answer for has gone, and counts nothing.
#[cfg(any(windows, test))]
fn descendants(
    root: u32,
    entries: &[(u32, u32)],
    created: impl Fn(u32) -> Option<u64>,
) -> std::collections::HashSet<u32> {
    let mut children_of: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
    for &(pid, parent) in entries {
        if pid != parent {
            children_of.entry(parent).or_default().push(pid);
        }
    }
    let mut mine = std::collections::HashSet::from([root]);
    let mut stack = vec![(root, created(root).unwrap_or(0))];
    while let Some((parent, parent_created)) = stack.pop() {
        for &child in children_of.get(&parent).into_iter().flatten() {
            match created(child) {
                Some(child_created) if child_created >= parent_created && mine.insert(child) => {
                    stack.push((child, child_created))
                }
                _ => {}
            }
        }
    }
    mine
}

#[cfg(not(any(target_os = "linux", windows)))]
compile_error!("smeltr runs on Linux and Windows, and has no resource sampler for any other target");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_first_sample_reports_no_cpu_rate() {
        let mut sampler = Sampler::new(std::env::temp_dir());
        assert_eq!(sampler.sample().cpu_percent, None, "a rate needs a previous reading");
    }

    #[test]
    fn a_second_sample_reports_a_rate() {
        let mut sampler = Sampler::new(std::env::temp_dir());
        sampler.sample();
        std::thread::sleep(Duration::from_millis(20));
        let second = sampler.sample();
        assert!(
            second.cpu_percent.is_some(),
            "a second reading has a previous one to measure against"
        );
    }

    #[test]
    fn memory_used_is_always_reported() {
        // Whichever platform arm answers, a running process has some memory footprint.
        assert!(Sampler::new(std::env::temp_dir()).sample().memory_used_bytes > 0);
    }

    #[test]
    fn a_process_reports_its_own_memory() {
        assert!(own_memory_bytes() > 0);
    }

    /// Whichever arm answers, the divisor behind `cpu_percent` is a real number of cores -- a zero
    /// would make the percentage itself meaningless, which is why `host_cores` floors at one.
    #[test]
    fn cpu_limit_cores_is_always_a_positive_number() {
        assert!(Sampler::new(std::env::temp_dir()).sample().cpu_limit_cores > 0.0);
    }

    #[test]
    fn a_sample_reports_the_state_directorys_own_filesystem() {
        let usage = Sampler::new(std::env::temp_dir()).sample();
        let total = usage
            .disk_total_bytes
            .expect("a temp directory sits on a real filesystem");
        let free = usage
            .disk_free_bytes
            .expect("a filesystem answering for total answers for free");
        assert!(total > 0, "a mounted filesystem has a size");
        assert!(free <= total, "free space cannot exceed the filesystem holding it");
    }

    /// The probe answers about a directory, so a worker whose state directory has been removed
    /// under it reports nothing rather than a zero that would read as a full disk.
    #[test]
    fn a_state_directory_that_is_not_there_reports_no_disk_figures() {
        let usage = Sampler::new(std::env::temp_dir().join("smeltr-no-such-directory-8f3a1c")).sample();
        assert_eq!(usage.disk_free_bytes, None);
        assert_eq!(usage.disk_total_bytes, None);
    }

    /// A child that spins, killed when dropped so a failed assert does not leave it running.
    struct Spinning(std::process::Child);

    impl Spinning {
        fn start() -> Spinning {
            Spinning(
                crate::process::shell("while :; do :; done", "for /l %i in (0,0,1) do @rem")
                    .spawn()
                    .expect("a shell starts"),
            )
        }
    }

    impl Drop for Spinning {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// The runtime is a child of its supervisor, so a worker's tree is what holds it.
    #[test]
    fn the_process_tree_holds_a_child() {
        let child = Spinning::start();
        let tree = platform::own_process_tree();
        assert!(tree.contains(&std::process::id()), "the tree starts at this process");
        assert!(tree.contains(&child.0.id()), "and holds its child");
    }

    /// A busy runtime is a busy worker. This process sleeps while its child spins, so only the
    /// child's time can account for what the tree spent. It waits for that time rather than
    /// timing a second: on a loaded machine the child gets less than a core.
    #[test]
    fn the_process_trees_cpu_counts_a_busy_child() {
        let _child = Spinning::start();
        let tree = platform::own_process_tree();
        let mut carried = Default::default();
        let mut read = || platform::tree_cpu_time(&tree, &mut carried).expect("every process in the tree is running");
        let before = read();
        let start = Instant::now();
        let mut spent = Duration::ZERO;
        while spent < Duration::from_millis(300) && start.elapsed() < Duration::from_secs(20) {
            std::thread::sleep(Duration::from_millis(100));
            spent = read().saturating_sub(before);
        }
        assert!(
            spent >= Duration::from_millis(300),
            "a spinning child spent {spent:?} in {:?}, by the tree's count",
            start.elapsed()
        );
    }

    #[test]
    fn the_process_trees_memory_counts_a_child() {
        let child = Spinning::start();
        let tree = platform::own_process_tree();
        let alone = platform::tree_memory(&[std::process::id()]);
        let with_child = platform::tree_memory(&tree);
        assert!(alone > 0, "this process has a footprint");
        assert!(
            with_child > alone,
            "child {} adds to {alone} bytes, but the tree read {with_child}",
            child.0.id()
        );
    }

    #[test]
    fn descendants_follow_parents_down_from_the_root() {
        let entries = [(1, 0), (10, 1), (11, 10), (12, 11), (20, 2)];
        assert_eq!(
            descendants(10, &entries, |_| Some(0)),
            std::collections::HashSet::from([10, 11, 12])
        );
    }

    /// Windows keeps a dead parent's pid in its children's entries and reuses it. A process older
    /// than the one its parent pid now names was some earlier holder's child.
    #[test]
    fn descendants_skip_a_child_older_than_the_parent_it_names() {
        let entries = [(10, 1), (11, 10), (12, 10)];
        let created = |pid| match pid {
            10 => Some(500),
            11 => Some(100),
            12 => Some(600),
            _ => None,
        };
        assert_eq!(
            descendants(10, &entries, created),
            std::collections::HashSet::from([10, 12])
        );
    }

    #[test]
    fn descendants_skip_a_child_that_has_gone() {
        let entries = [(10, 1), (11, 10), (12, 11)];
        let created = |pid| (pid != 11).then_some(0);
        assert_eq!(
            descendants(10, &entries, created),
            std::collections::HashSet::from([10])
        );
    }

    /// A tree's total falls when a process leaves it with no parent in the tree to carry its time:
    /// a runtime ending, on Windows. Reading that drop as zero would call a busy worker idle.
    #[test]
    fn a_falling_total_starts_over_rather_than_reading_as_idle() {
        let start = Instant::now();
        let mut sampler = Sampler::new(std::env::temp_dir());
        let mut rate = |cpu_secs: u64, at_secs: u64| {
            let cpu = CpuReading {
                time: Some(Duration::from_secs(cpu_secs)),
                at: start + Duration::from_secs(at_secs),
                source: CpuSource::Tree,
                limit_cores: 1.0,
                limited: false,
            };
            let memory = MemoryReading {
                used_bytes: 1,
                limit_bytes: None,
                limited: false,
            };
            sampler.record(cpu, memory).cpu_percent
        };
        assert_eq!(rate(100, 0), None);
        assert_eq!(rate(10, 1), None, "a runtime's time left with it");
        assert_eq!(rate(11, 2), Some(100.0), "and the next reading measures from there");
    }

    /// The two builders below feed the counter-identity tests, which are Linux's alone.
    #[cfg(target_os = "linux")]
    fn cpu_reading(time: Duration, source: CpuSource) -> CpuReading {
        CpuReading {
            time: Some(time),
            at: Instant::now(),
            source,
            limit_cores: 1.0,
            limited: true,
        }
    }

    #[cfg(target_os = "linux")]
    fn memory_reading() -> MemoryReading {
        MemoryReading {
            used_bytes: 1,
            limit_bytes: None,
            limited: false,
        }
    }

    /// A process that leaves keeps the time it was last read with, so the total never falls, and
    /// one process's growth cannot hide another's leaving.
    #[test]
    fn a_carried_total_keeps_a_departed_processs_time() {
        let mut carried = Carried::default();
        let live = |entries: &[((u32, u64), u64)]| entries.iter().copied().collect();
        assert_eq!(carried.total(live(&[((1, 0), 10), ((2, 5), 90)])), 100);
        assert_eq!(carried.total(live(&[((1, 0), 30)])), 120, "2 left with 90");
        assert_eq!(
            carried.total(live(&[((1, 0), 30), ((2, 9), 4)])),
            124,
            "a reused pid is a new process"
        );
    }

    /// Mapped lookup files are cache the kernel can drop, so PSS from them is not a worker's.
    #[cfg(target_os = "linux")]
    #[test]
    fn status_field_reads_one_named_line() {
        let rollup = "55d0-7ffe ---p 00000000 00:00 0 [rollup]\nRss: 900 kB\nPss: 800 kB\nPss_Anon: 300 kB\nPss_File: 450 kB\nPss_Shmem: 50 kB\n";
        assert_eq!(platform::status_field(rollup, "Pss:"), Some(800));
        assert_eq!(platform::status_field(rollup, "Pss_Anon:"), Some(300));
        assert_eq!(platform::status_field(rollup, "Pss_Shmem:"), Some(50));
        assert_eq!(platform::status_field(rollup, "Swap:"), None);
    }

    /// A process that exited mid-sample leaves a total with a hole in it. That sample has no
    /// rate, and the next measures from the last whole one.
    #[test]
    fn a_total_that_missed_a_process_has_no_rate_and_keeps_the_baseline() {
        let start = Instant::now();
        let mut sampler = Sampler::new(std::env::temp_dir());
        let mut rate = |cpu_secs: Option<u64>, at_secs: u64| {
            let cpu = CpuReading {
                time: cpu_secs.map(Duration::from_secs),
                at: start + Duration::from_secs(at_secs),
                source: CpuSource::Tree,
                limit_cores: 1.0,
                limited: false,
            };
            let memory = MemoryReading {
                used_bytes: 1,
                limit_bytes: None,
                limited: false,
            };
            sampler.record(cpu, memory).cpu_percent
        };
        assert_eq!(rate(Some(100), 0), None);
        assert_eq!(rate(None, 1), None, "a process could not be read");
        assert_eq!(rate(Some(102), 2), Some(100.0), "measured from the reading at 0");
    }

    /// A command name may hold spaces and parens, so the fields are counted from its closing one.
    #[cfg(target_os = "linux")]
    #[test]
    fn cpu_ticks_are_utime_stime_and_reaped_childrens_from_proc_stat() {
        let stat = "42 (a (b) c) S 1 42 42 0 -1 4194560 100 0 0 0 7 3 20 10 20 0 1 0 5 0 0";
        assert_eq!(platform::parse_cpu_ticks(stat), Some(40));
        assert_eq!(platform::parse_cpu_ticks("42 (short) S 1 42"), None);
    }

    /// The counter-identity tests below are Linux's: they need two counters to tell apart, and
    /// only Linux has a second one.
    #[cfg(target_os = "linux")]
    fn cgroup_source(path: &str) -> CpuSource {
        CpuSource::Cgroup(std::path::PathBuf::from(path))
    }

    /// CPU time over wall time, as a percent of the allowed cores: 100 is every allowed core busy. The same counter
    /// throughout, so this is also the positive control for the counter-identity tests below.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_rate_is_cpu_time_over_wall_time_as_a_percent_of_the_allowed_cores() {
        let start = Instant::now();
        let mut sampler = Sampler::new(std::env::temp_dir());
        let mut rate = |cpu_ms: u64, at_secs: u64| {
            let cpu = CpuReading {
                at: start + Duration::from_secs(at_secs),
                limit_cores: 2.0,
                ..cpu_reading(Duration::from_millis(cpu_ms), cgroup_source("/a"))
            };
            sampler.record(cpu, memory_reading()).cpu_percent
        };
        assert_eq!(rate(10_000, 0), None);
        assert_eq!(rate(10_500, 1), Some(25.0), "half a core busy, out of two");
        assert_eq!(rate(12_500, 2), Some(100.0), "both cores busy");
        assert_eq!(rate(13_000, 2), None, "no wall time has passed");
    }

    /// The exact gap a review caught: a cgroup going unreadable mid-run, or a sibling appearing
    /// or leaving a shared scope, switches which counter `CpuReading::time` is cumulative
    /// against. Subtracting across that switch compares two unrelated totals.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_rate_does_not_carry_across_a_change_of_counter() {
        let mut sampler = Sampler::new(std::env::temp_dir());
        sampler.record(
            cpu_reading(Duration::from_secs(100), cgroup_source("/a")),
            memory_reading(),
        );
        std::thread::sleep(Duration::from_millis(5));
        let second = sampler.record(cpu_reading(Duration::from_secs(1), CpuSource::Tree), memory_reading());
        assert_eq!(
            second.cpu_percent, None,
            "a switch from the cgroup's counter to this process's own is a fresh baseline, not a rate"
        );
    }

    /// Fresh evidence on the same fix: `CpuSource::Cgroup` only told a cgroup counter apart from
    /// a process counter, not one cgroup from another -- a worker moved to a different cgroup
    /// mid-run has just as unrelated a counter as a switch to `Tree` does.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_rate_does_not_carry_across_a_move_to_a_different_cgroup() {
        let mut sampler = Sampler::new(std::env::temp_dir());
        sampler.record(
            cpu_reading(Duration::from_secs(100), cgroup_source("/a")),
            memory_reading(),
        );
        std::thread::sleep(Duration::from_millis(5));
        let second = sampler.record(
            cpu_reading(Duration::from_secs(1), cgroup_source("/b")),
            memory_reading(),
        );
        assert_eq!(
            second.cpu_percent, None,
            "a and b are different cgroups with different counters, not a rate across one"
        );
    }
}
