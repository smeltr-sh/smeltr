//! How much CPU cache one transform thread can count on, so a batch runs in slices that stay in it.
//!
//! A pipeline runs one step over its whole input before the next. Once that input outgrows the core's L2, every step
//! reads its columns back from main memory instead of from the cache the step before left them in. L3 is left out:
//! a virtual machine reports its host's whole L3, which tenants it cannot see share with it.

use std::sync::OnceLock;

/// Raw bytes in one slice where the cache cannot be read.
const UNKNOWN_SLICE_BYTES: usize = 512 * 1024;
/// The least a slice holds, below which each slice's own setup outweighs what the cache saves.
const LEAST_SLICE_BYTES: usize = 256 * 1024;

/// Raw bytes a transform thread runs through its pipelines at once: half the L2 one hardware thread of this machine
/// can count on, since what a record decodes to is several times its raw bytes. Read once per process.
pub fn slice_bytes() -> usize {
    static SLICE: OnceLock<usize> = OnceLock::new();
    *SLICE.get_or_init(|| slice_for(platform::l2_per_thread()))
}

fn slice_for(l2_per_thread: Option<usize>) -> usize {
    l2_per_thread.map_or(UNKNOWN_SLICE_BYTES, |l2| {
        (l2 / 2).clamp(LEAST_SLICE_BYTES, crate::worker::TICK_MAX_BYTES)
    })
}

#[cfg(target_os = "linux")]
mod platform {
    use std::path::Path;

    /// The least L2 any hardware thread has to itself: each L2's size over the threads sharing it, the least over
    /// every CPU, since a transform thread may run on any of them and a hybrid CPU's cores differ.
    pub fn l2_per_thread() -> Option<usize> {
        l2_per_thread_in(Path::new("/sys/devices/system/cpu"))
    }

    pub(super) fn l2_per_thread_in(cpus: &Path) -> Option<usize> {
        let mut least: Option<usize> = None;
        for cpu in std::fs::read_dir(cpus).ok()?.flatten() {
            let name = cpu.file_name();
            let Some(n) = name.to_str().and_then(|n| n.strip_prefix("cpu")) else {
                continue;
            };
            if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
                continue;
            }
            let Ok(caches) = std::fs::read_dir(cpu.path().join("cache")) else {
                continue;
            };
            for cache in caches.flatten() {
                let read = |file: &str| std::fs::read_to_string(cache.path().join(file)).ok();
                if read("level").as_deref().map(str::trim) != Some("2")
                    || read("type").as_deref().map(str::trim) == Some("Instruction")
                {
                    continue;
                }
                let Some(size) = read("size").and_then(|s| parse_size(&s)) else {
                    continue;
                };
                let sharing = read("shared_cpu_list").map_or(1, |l| count_cpus(&l)).max(1);
                let per_thread = size / sharing;
                least = Some(least.map_or(per_thread, |l| l.min(per_thread)));
            }
        }
        least
    }

    /// A cache's `size`, such as `2048K`.
    pub(super) fn parse_size(text: &str) -> Option<usize> {
        let text = text.trim();
        let (digits, unit) = match text.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
            Some((at, _)) => text.split_at(at),
            None => (text, ""),
        };
        let n: usize = digits.parse().ok()?;
        let scale = match unit {
            "" => 1,
            "K" => 1 << 10,
            "M" => 1 << 20,
            "G" => 1 << 30,
            _ => return None,
        };
        n.checked_mul(scale).filter(|&b| b > 0)
    }

    /// How many CPUs a list such as `0-3,8-11` names.
    pub(super) fn count_cpus(list: &str) -> usize {
        list.trim()
            .split(',')
            .filter(|part| !part.is_empty())
            .map(|part| match part.split_once('-') {
                Some((a, b)) => match (a.parse::<usize>(), b.parse::<usize>()) {
                    (Ok(a), Ok(b)) if b >= a => b - a + 1,
                    _ => 0,
                },
                None => usize::from(part.parse::<usize>().is_ok()),
            })
            .sum()
    }
}

#[cfg(windows)]
mod platform {
    use windows_sys::Win32::System::SystemInformation::{
        CacheInstruction, GetLogicalProcessorInformationEx, RelationCache, SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
    };

    /// The least L2 any hardware thread has to itself: each L2's size over the threads its mask names.
    pub fn l2_per_thread() -> Option<usize> {
        let mut len = 0u32;
        // Asked once with no buffer for the length it needs.
        unsafe { GetLogicalProcessorInformationEx(RelationCache, std::ptr::null_mut(), &mut len) };
        if len == 0 {
            return None;
        }
        // u64s, so every record the call lays out is aligned. A cache record is shorter than the struct, whose union
        // holds every kind, so a whole struct's worth more keeps a reference to the last record inside the buffer.
        let mut buffer = vec![0u64; (len as usize + size_of::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>()).div_ceil(8)];
        let at = buffer.as_mut_ptr().cast::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>();
        if unsafe { GetLogicalProcessorInformationEx(RelationCache, at, &mut len) } == 0 {
            return None;
        }
        let mut least: Option<usize> = None;
        let mut offset = 0usize;
        while offset < len as usize {
            // SAFETY: the call wrote `len` bytes of records, each `Size` bytes long, starting at the buffer's start.
            let record = unsafe {
                &*buffer
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset)
                    .cast::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>()
            };
            if record.Size == 0 || offset + record.Size as usize > len as usize {
                break;
            }
            offset += record.Size as usize;
            if record.Relationship != RelationCache {
                continue;
            }
            let cache = unsafe { &record.Anonymous.Cache };
            if cache.Level != 2 || cache.Type == CacheInstruction {
                continue;
            }
            let sharing = (unsafe { cache.Anonymous.GroupMask.Mask }.count_ones() as usize).max(1);
            let per_thread = cache.CacheSize as usize / sharing;
            least = Some(least.map_or(per_thread, |l| l.min(per_thread)));
        }
        least.filter(|&b| b > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slice_is_half_the_l2_within_its_bounds() {
        assert_eq!(slice_for(Some(2 << 20)), 1 << 20);
        assert_eq!(slice_for(Some(256 << 10)), LEAST_SLICE_BYTES);
        assert_eq!(slice_for(Some(64 << 20)), crate::worker::TICK_MAX_BYTES);
        assert_eq!(slice_for(None), UNKNOWN_SLICE_BYTES);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_counts_each_l2_over_the_threads_sharing_it() {
        use super::platform::{count_cpus, l2_per_thread_in, parse_size};
        assert_eq!(parse_size("2048K\n"), Some(2 << 20));
        assert_eq!(parse_size("1M"), Some(1 << 20));
        assert_eq!(parse_size("big"), None);
        assert_eq!(count_cpus("0-3,8-11\n"), 8);
        assert_eq!(count_cpus("5"), 1);

        let root = std::env::temp_dir().join(format!("smeltr-cpu-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let cache = |cpu: &str, index: &str, level: &str, kind: &str, size: &str, shared: &str| {
            let dir = root.join(cpu).join("cache").join(index);
            std::fs::create_dir_all(&dir).unwrap();
            for (file, text) in [
                ("level", level),
                ("type", kind),
                ("size", size),
                ("shared_cpu_list", shared),
            ] {
                std::fs::write(dir.join(file), format!("{text}\n")).unwrap();
            }
        };
        // A performance core with 2 MiB to two threads, and an efficiency core sharing 3 MiB among four.
        cache("cpu0", "index0", "1", "Data", "48K", "0-1");
        cache("cpu0", "index1", "1", "Instruction", "32K", "0-1");
        cache("cpu0", "index2", "2", "Unified", "2048K", "0-1");
        cache("cpu0", "index3", "3", "Unified", "266240K", "0-7");
        cache("cpu4", "index2", "2", "Unified", "3072K", "4-7");
        std::fs::create_dir_all(root.join("cpufreq")).unwrap();
        assert_eq!(l2_per_thread_in(&root), Some(768 << 10));
        std::fs::remove_dir_all(&root).unwrap();
    }
}
