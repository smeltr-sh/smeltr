//! Pooled columnar memory, and the ceiling on how much of it may exist.
//!
//! Every columnar buffer comes from a `BufferPool`, so the pool is where a bound on a batch's memory is enforced *on
//! the allocation* rather than sampled after the fact (design doc, Runtime stages). Everything else a step allocates
//! is counted by the global allocator and held to the same budget by `check`.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

use bytemuck::{AnyBitPattern, NoUninit};

/// A request the budget refused. Carried as a panic payload — see `catch_over_budget`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverBudget {
    /// Bytes the refused allocation asked for, after rounding up to the bucket size that would have been allocated.
    pub requested: usize,
    /// Bytes already charged when the request arrived.
    pub used: usize,
    /// The ceiling.
    pub limit: usize,
}

impl std::fmt::Display for OverBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "columnar memory budget exhausted: asked for {} bytes with {} of {} already in use",
            self.requested, self.used, self.limit
        )
    }
}

/// A ceiling on the bytes columnar buffers may hold at once, shared by every pool that clones it.
///
/// Charged when a buffer is allocated and released when it is dropped — not on `take`/`give`. A pooled free buffer is
/// still resident, so it still counts; and a buffer an unwinding call never gave back still releases, so the total
/// cannot drift upward.
///
/// A recycled buffer is already charged, so `take` consults the budget only for a fresh allocation.
#[derive(Clone, Default)]
pub struct MemoryBudget(Option<Arc<BudgetState>>);

struct BudgetState {
    limit: AtomicUsize,
    used: AtomicUsize,
    /// High-water mark of `used`.
    peak: AtomicUsize,
}

impl MemoryBudget {
    /// A budget of `limit` bytes.
    pub fn new(limit: usize) -> Self {
        MemoryBudget(Some(Arc::new(BudgetState {
            limit: AtomicUsize::new(limit),
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })))
    }

    /// No ceiling at all.
    pub fn unlimited() -> Self {
        MemoryBudget(None)
    }

    /// The ceiling, or `None` when there is none.
    pub fn limit(&self) -> Option<usize> {
        self.0.as_ref().map(|s| s.limit.load(Ordering::Relaxed))
    }

    /// Moves the ceiling to `limit`, for every pool sharing this budget. A ceiling below what is charged refuses every
    /// fresh allocation until enough is released. No-op on an unlimited budget.
    pub fn set_limit(&self, limit: usize) {
        if let Some(state) = self.0.as_ref() {
            state.limit.store(limit, Ordering::Relaxed);
        }
    }

    /// Bytes currently charged. Zero for an unlimited budget.
    pub fn used(&self) -> usize {
        self.0.as_ref().map_or(0, |s| s.used.load(Ordering::Relaxed))
    }

    /// The most that was ever charged at once. Zero for an unlimited budget.
    pub fn peak(&self) -> usize {
        self.0.as_ref().map_or(0, |s| s.peak.load(Ordering::Relaxed))
    }

    /// Charge `bytes`, or report the refusal. Never charges partially.
    fn charge(&self, bytes: usize) -> Result<(), OverBudget> {
        let Some(state) = self.0.as_ref() else {
            return Ok(());
        };
        let limit = state.limit.load(Ordering::Relaxed);
        let mut used = state.used.load(Ordering::Relaxed);
        loop {
            let next = match used.checked_add(bytes) {
                Some(n) if n <= limit => n,
                _ => {
                    return Err(OverBudget {
                        requested: bytes,
                        used,
                        limit,
                    });
                }
            };
            match state
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    state.peak.fetch_max(next, Ordering::Relaxed);
                    return Ok(());
                }
                Err(actual) => used = actual,
            }
        }
    }

    /// The refusal for a request whose size has no representable allocation.
    fn refusal_for(&self, bytes: usize) -> OverBudget {
        OverBudget {
            requested: bytes,
            used: self.used(),
            limit: self.limit().unwrap_or(usize::MAX),
        }
    }

    fn release(&self, bytes: usize) {
        if let Some(state) = self.0.as_ref() {
            state.used.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

/// Run `f`, turning a budget refusal inside it into an `Err`.
///
/// The refusal travels as an unwind because every columnar allocation reaches `BufferPool::take` through infallible
/// signatures. Any other panic is resumed untouched, so a real bug still reaches `Worker::tick`'s own `catch_unwind`
/// and is still a poison batch.
///
/// **The caller must discard everything `f` touched**: the batch it was building, and the pool it allocated from.
pub fn catch_over_budget<T>(f: impl FnOnce() -> T) -> Result<T, OverBudget> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(v) => Ok(v),
        Err(payload) => match payload.downcast::<OverBudget>() {
            Ok(over) => Err(*over),
            Err(other) => std::panic::resume_unwind(other),
        },
    }
}

/// Stop the default panic hook from printing a backtrace for a budget refusal, which is expected and handled.
/// Process-wide, installed once; belongs to a binary's startup.
pub fn quiet_refusals() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if info.payload().downcast_ref::<OverBudget>().is_some() {
                return;
            }
            previous(info);
        }));
    });
}

/// The global allocator: jemalloc, or mimalloc on Windows, counting the bytes each thread holds, so a step's heap beside its columns can be
/// held to the budget. Counting never fails and never panics; only `check` refuses.
pub struct Counting;

thread_local! {
    /// Bytes this thread allocated less what it freed, leaving out column buffers, which the budget charges itself. A
    /// block freed on another thread counts there, so only a difference over one stretch of one thread means anything.
    static LIVE: Cell<isize> = const { Cell::new(0) };
    /// `LIVE` past which `check` asks the budget for more.
    static CEILING: Cell<isize> = const { Cell::new(isize::MAX) };
    static CHARGE: RefCell<Option<Charge>> = const { RefCell::new(None) };
}

#[inline]
fn count(bytes: isize) {
    LIVE.set(LIVE.get().wrapping_add(bytes));
    ALLOCATED[SHARD.with(|s| *s)].0.fetch_add(bytes, Ordering::Relaxed);
}

/// Bytes the process allocated less what it freed, in shards so the threads counting into it seldom share a line.
/// A block freed on another thread counts in that thread's shard, so only the sum means anything.
static ALLOCATED: [Shard; SHARDS] = [const { Shard(AtomicIsize::new(0)) }; SHARDS];

const SHARDS: usize = 64;

#[repr(align(128))]
struct Shard(AtomicIsize);

thread_local! {
    /// This thread's shard of `ALLOCATED`, given out in turn as threads start.
    static SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
}

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

/// What the process's own allocations hold now: what it allocated and has not freed, whatever the allocator keeps of
/// what was freed. Memory a C library allocates itself is not in it.
pub fn allocated_bytes() -> usize {
    let sum: isize = ALLOCATED.iter().map(|s| s.0.load(Ordering::Relaxed)).sum();
    sum.max(0) as usize
}

#[cfg(windows)]
use mimalloc::MiMalloc as Underlying;
#[cfg(unix)]
use tikv_jemallocator::Jemalloc as Underlying;

// SAFETY: every call is passed to the allocator unchanged; the count beside it touches only a thread-local integer.
unsafe impl GlobalAlloc for Counting {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size() as isize);
        unsafe { Underlying.alloc(layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size() as isize);
        unsafe { Underlying.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        count(-(layout.size() as isize));
        unsafe { Underlying.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count(new_size as isize - layout.size() as isize);
        unsafe { Underlying.realloc(ptr, layout, new_size) }
    }
}

/// Hands back this thread's free pages as [`release_idle`] does, unless it did within [`RELEASE_EVERY`]. For a thread
/// that cannot tell when it goes idle, only that it is about to wait: each wait hands back what the work before it
/// freed, at a bounded cost while waits come quickly. Whether it handed them back.
pub fn release_if_due() -> bool {
    thread_local! {
        static RELEASED: Cell<Option<std::time::Instant>> = const { Cell::new(None) };
    }
    let now = std::time::Instant::now();
    let due = RELEASED.get().is_none_or(|at| now.duration_since(at) >= RELEASE_EVERY);
    if due {
        release_idle();
        RELEASED.set(Some(now));
    }
    due
}

/// How often a thread that waits often hands back its free pages, at most.
const RELEASE_EVERY: std::time::Duration = std::time::Duration::from_millis(100);

/// Hands back what this thread's allocator holds free for it. Each thread caches blocks it freed for its own next
/// allocations, so a thread that has gone idle keeps whatever its last work freed until it works again.
pub fn release_idle() {
    // jemalloc's background threads hand free pages to the OS; the thread's cache goes back to the arenas only here.
    // SAFETY: the name is NUL-terminated, and `thread.tcache.flush` reads and writes nothing through the pointers.
    #[cfg(unix)]
    unsafe {
        tikv_jemalloc_sys::mallctl(
            c"thread.tcache.flush".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        );
    }
    // SAFETY: `mi_collect` takes no pointers and touches only this thread's heap and the arenas it shares.
    #[cfg(windows)]
    unsafe {
        libmimalloc_sys::mi_collect(false)
    }
}

/// jemalloc's settings, read once at its start: background threads hand freed pages back to the OS ten seconds after
/// they go unused, so an idle process holds little more than it allocated. A shorter wait hands back pages the next
/// batches fault in again, which cost throughput. Its memory comes in huge pages, each faulted in once rather than
/// in 4 KiB pieces, which kept the transform as fast as under mimalloc. `MALLOC_CONF` in the environment, spelled
/// `_RJEM_MALLOC_CONF`, overrides them.
#[cfg(unix)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static MALLOC_CONF: Conf =
    Conf(c"background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:0,thp:always,metadata_thp:always".as_ptr());

/// A C string jemalloc reads at its start.
#[cfg(unix)]
#[repr(transparent)]
pub struct Conf(*const std::ffi::c_char);

// SAFETY: it points at a static string nothing writes.
#[cfg(unix)]
unsafe impl Sync for Conf {}

/// What a `Scratch` has charged to its budget.
struct Charge {
    budget: MemoryBudget,
    start: isize,
    charged: usize,
}

/// The least `check` charges at once, so a step of ordinary size touches the shared budget once.
const SCRATCH_CHUNK: usize = 64 << 10;

/// While it lives, what this thread allocates beside its columns is charged to `budget` at each `check`, and released
/// when it drops. Scratches nest: an inner one counts from where it began, and the outer one sees what it left behind.
pub struct Scratch {
    prior: Option<Charge>,
    prior_ceiling: isize,
}

impl Scratch {
    pub fn begin(budget: &MemoryBudget) -> Scratch {
        let start = LIVE.get();
        let charge = budget.limit().map(|_| Charge {
            budget: budget.clone(),
            start,
            charged: 0,
        });
        let ceiling = if charge.is_some() { start } else { isize::MAX };
        Scratch {
            prior: CHARGE.replace(charge),
            prior_ceiling: CEILING.replace(ceiling),
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Some(c) = CHARGE.replace(self.prior.take()) {
            c.budget.release(c.charged);
        }
        CEILING.set(self.prior_ceiling);
    }
}

/// Hold what this thread allocated since its `Scratch` began to the budget. Call it where a step's heap grows with its
/// input: a record can then overshoot by what it allocated since the last call, never by its whole expansion. Unwinds
/// with an `OverBudget` payload when the budget refuses; does nothing outside a `Scratch`.
#[inline]
pub fn check() {
    if LIVE.get() > CEILING.get() {
        charge_scratch();
    }
}

#[cold]
fn charge_scratch() {
    CHARGE.with_borrow_mut(|c| {
        let Some(c) = c else { return };
        let need = (LIVE.get() - c.start) as usize - c.charged;
        let more = match c.budget.charge(need.max(SCRATCH_CHUNK)) {
            Ok(()) => need.max(SCRATCH_CHUNK),
            Err(_) => {
                c.budget.charge(need).unwrap_or_else(|over| refuse(over));
                need
            }
        };
        c.charged += more;
        CEILING.set(c.start + c.charged as isize);
    });
}

/// A raw byte buffer, `u64`-backed so any 8-byte type views it without `unsafe`. `capacity` is the requested length;
/// the allocation behind it is the next power of two.
#[derive(Default)]
pub struct Buffer {
    data: Vec<u64>,
    capacity: usize,
    budget: MemoryBudget,
    /// Bytes charged: the allocation's own size, not `capacity`.
    charged: usize,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        count(self.charged as isize);
        self.budget.release(self.charged);
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("capacity", &self.capacity)
            .field("allocated", &self.charged)
            .finish()
    }
}

/// The smallest allocation: one `u64`.
const MIN_ALLOC: usize = 8;

impl Buffer {
    /// Panics if `capacity` is not a multiple of `size_of::<T>()`.
    pub fn as_slice<T: AnyBitPattern>(&self) -> &[T] {
        let full_bytes: &[u8] = bytemuck::cast_slice(&self.data);
        bytemuck::cast_slice(&full_bytes[..self.capacity])
    }

    pub fn as_mut_slice<T: NoUninit + AnyBitPattern>(&mut self) -> &mut [T] {
        let full_bytes: &mut [u8] = bytemuck::cast_slice_mut(&mut self.data);
        bytemuck::cast_slice_mut(&mut full_bytes[..self.capacity])
    }

    /// Unwinds with an `OverBudget` payload when `budget` refuses. A request too large to round up to a power of two is
    /// refused the same way rather than wrapping, because that allocation would abort the process instead of unwinding.
    fn with_capacity(capacity: usize, budget: &MemoryBudget) -> Self {
        Self::try_with_capacity(capacity, budget).unwrap_or_else(|over| refuse(over))
    }

    fn try_with_capacity(capacity: usize, budget: &MemoryBudget) -> Result<Self, OverBudget> {
        let cap = match capacity.checked_next_power_of_two() {
            Some(cap) => std::cmp::max(MIN_ALLOC, cap),
            None => refuse(budget.refusal_for(capacity)),
        };
        budget.charge(cap)?;
        let data = vec![0u64; cap / 8];
        // The budget charges a column buffer itself; `check` counts the rest.
        count(-(cap as isize));
        Ok(Self {
            data,
            capacity,
            budget: budget.clone(),
            charged: cap,
        })
    }

    /// The bytes it can grow to without taking a new buffer.
    pub fn room(&self) -> usize {
        self.data.len() * 8
    }

    pub fn grow(&mut self, buffer_pool: &mut BufferPool, new_capacity: usize) {
        if new_capacity <= self.data.len() * 8 {
            self.capacity = new_capacity;
            return;
        }
        let mut bigger = buffer_pool.take(new_capacity);
        bigger.as_mut_slice::<u8>()[..self.capacity].copy_from_slice(self.as_slice::<u8>());
        let old = std::mem::replace(self, bigger);
        buffer_pool.give(old);
    }
}

fn refuse(over: OverBudget) -> ! {
    std::panic::panic_any(over)
}

/// How many batches `trim` looks back over.
const TRIM_WINDOW: usize = 8;

/// Recycles buffers by power-of-two size class. `take` hands out a buffer of at least the requested size; `give`
/// returns it. Buffers come back dirty.
///
/// A pool outlives its batches, so the next batch reuses what the last one gave back. `trim` ends a batch and frees what
/// recent batches did not need, so an outsized batch's buffers do not stay charged to the budget for good.
pub struct BufferPool {
    /// `classes[i]` holds buffers whose allocation is exactly `2^i` bytes.
    classes: Vec<SizeClass>,
    budget: MemoryBudget,
    /// The slot of each class's `recent` that `trim` fills next.
    batch: usize,
    /// The most bytes `trim` leaves idle.
    idle_cap: usize,
}

#[derive(Default)]
struct SizeClass {
    free: Vec<Buffer>,
    /// Handed out and not yet given back.
    out: usize,
    /// The most `out` reached this batch.
    peak: usize,
    /// `peak` of each of the last `TRIM_WINDOW` batches.
    recent: [usize; TRIM_WINDOW],
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferPool {
    /// A pool with no ceiling on what it may allocate.
    pub fn new() -> Self {
        Self::with_budget(MemoryBudget::unlimited())
    }

    /// A pool whose allocations are charged against `budget`, which other pools may share.
    pub fn with_budget(budget: MemoryBudget) -> Self {
        BufferPool {
            classes: Vec::new(),
            budget,
            batch: 0,
            idle_cap: usize::MAX,
        }
    }

    /// The size class a request for `bytes` is served from: the bucket of the allocation `Buffer::with_capacity` would
    /// make for it, so a buffer given back lands in the bucket `take` looks in.
    fn bucket_index(&mut self, bytes: usize) -> usize {
        let min_bytes = std::cmp::max(MIN_ALLOC, bytes.next_power_of_two());
        let index = min_bytes.trailing_zeros() as usize;
        if index >= self.classes.len() {
            self.classes.resize_with(index + 1, SizeClass::default);
        }
        index
    }

    /// Keep at most `bytes` idle after `trim`. The budget is shared, and another pool's idle buffers are charged where
    /// this one cannot free them; a cap bounds how much of the budget a refused pool finds taken that way.
    pub fn with_idle_cap(mut self, bytes: usize) -> Self {
        self.idle_cap = bytes;
        self
    }

    pub fn budget(&self) -> &MemoryBudget {
        &self.budget
    }

    /// Whether the pool holds free buffers, which are charged though nothing uses them.
    pub fn holds_idle(&self) -> bool {
        self.classes.iter().any(|c| !c.free.is_empty())
    }

    /// An empty pool with this one's budget and cap.
    pub fn empty_like(&self) -> Self {
        Self::with_budget(self.budget.clone()).with_idle_cap(self.idle_cap)
    }

    /// A buffer of at least `bytes`, recycled when the pool holds one.
    ///
    /// Unwinds with an `OverBudget` payload when the budget refuses a fresh allocation. A recycled buffer is already
    /// charged and never refused.
    /// Unwinds as an allocation of `bytes` the budget refused: for a request no columnar buffer can hold at all, such as
    /// more elements than a `u32` offset counts. The runtime takes it as it takes any refusal.
    pub fn refuse(&self, bytes: usize) -> ! {
        refuse(self.budget.refusal_for(bytes))
    }

    pub fn take(&mut self, bytes: usize) -> Buffer {
        if bytes.checked_next_power_of_two().is_none() {
            refuse(self.budget.refusal_for(bytes));
        }
        let index = self.bucket_index(bytes);
        let mut buffer = match self.classes[index].free.pop() {
            Some(b) => b,
            None => self.fresh(bytes),
        };
        buffer.capacity = bytes;
        let class = &mut self.classes[index];
        class.out += 1;
        class.peak = class.peak.max(class.out);
        buffer
    }

    /// A new buffer. Where the budget refuses it, the pool's free buffers go first and it asks once more: they are
    /// charged, and nothing is using them.
    fn fresh(&mut self, bytes: usize) -> Buffer {
        match Buffer::try_with_capacity(bytes, &self.budget) {
            Ok(buffer) => buffer,
            Err(over) if self.classes.iter().all(|c| c.free.is_empty()) => refuse(over),
            Err(_) => {
                self.classes.iter_mut().for_each(|c| c.free.clear());
                Buffer::with_capacity(bytes, &self.budget)
            }
        }
    }

    pub fn give(&mut self, buffer: Buffer) {
        let index = self.bucket_index(buffer.data.len() * 8);
        let class = &mut self.classes[index];
        // Saturating: a buffer made outside the pool may be given to it.
        class.out = class.out.saturating_sub(1);
        class.free.push(buffer);
    }

    /// End a batch, once every buffer it took is given back. Each size class keeps as many free buffers as it had out at
    /// once in the most demanding of the last `TRIM_WINDOW` batches, and frees the rest, oldest first.
    pub fn trim(&mut self) {
        for class in &mut self.classes {
            class.recent[self.batch] = class.peak;
            let keep = class.recent.iter().copied().max().unwrap_or(0);
            let surplus = class.free.len().saturating_sub(keep);
            class.free.drain(..surplus);
            // From zero: a buffer dropped rather than given back must not keep its class's count up for good.
            class.out = 0;
            class.peak = 0;
        }
        self.batch = (self.batch + 1) % TRIM_WINDOW;
        // Over the cap, the largest buffers go first: the fewest frees bring it under.
        let mut idle: usize = self.classes.iter().flat_map(|c| &c.free).map(|b| b.charged).sum();
        for class in self.classes.iter_mut().rev() {
            while idle > self.idle_cap
                && let Some(b) = class.free.pop()
            {
                idle -= b.charged;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The process total counts a block while it lives, whichever thread frees it.
    #[cfg(unix)]
    #[test]
    fn jemalloc_reads_our_settings_and_runs_background_threads() {
        let mut on = false;
        let mut len = std::mem::size_of::<bool>();
        // SAFETY: `opt.background_thread` is a bool, and `on` and `len` are sized for one.
        let read = unsafe {
            tikv_jemalloc_sys::mallctl(
                c"opt.background_thread".as_ptr(),
                (&raw mut on).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        assert_eq!(read, 0);
        assert!(on);
    }

    #[test]
    fn the_process_total_counts_what_is_held_until_it_is_freed() {
        const BIG: usize = 256 << 20;
        let before = allocated_bytes();
        let held = std::thread::spawn(|| vec![1u8; BIG]).join().unwrap();
        assert!(allocated_bytes() >= before + BIG / 2, "held, though its thread is gone");
        drop(held);
        assert!(allocated_bytes() < before + BIG / 2, "freed on another thread");
    }

    fn pool() -> BufferPool {
        BufferPool::new()
    }

    #[test]
    fn roundtrip_i64() {
        let mut buf = Buffer::with_capacity(8 * 3, &MemoryBudget::unlimited());
        buf.as_mut_slice::<i64>().copy_from_slice(&[1, -2, 3]);
        assert_eq!(buf.as_slice::<i64>(), &[1, -2, 3]);
    }

    #[test]
    fn a_tiny_buffer_is_recycled_too() {
        let mut p = pool();
        let buf = p.take(0);
        let ptr = buf.data.as_ptr();
        p.give(buf);
        assert_eq!(p.take(4).data.as_ptr(), ptr);
    }

    /// A pool holding only a smaller buffer still hands out one that holds the whole request.
    #[test]
    fn take_never_returns_undersized_buffer() {
        let mut p = pool();
        let small = p.take(8);
        p.give(small);
        let mut buf = p.take(4096);
        let bytes = buf.as_mut_slice::<u8>();
        assert_eq!(bytes.len(), 4096);
        bytes.fill(0xAB);
    }

    /// A buffer reads back as exactly the bytes asked for, whatever the underlying allocation rounded up to -- the
    /// property every kernel that slices a column depends on.
    #[test]
    fn a_buffer_is_exactly_as_long_as_it_was_asked_for() {
        let mut p = pool();
        for want in [4, 8, 9, 12, 64] {
            assert_eq!(
                Buffer::with_capacity(want, &MemoryBudget::unlimited())
                    .as_slice::<u8>()
                    .len(),
                want
            );
            assert_eq!(p.take(want).as_slice::<u8>().len(), want);
        }
    }

    #[test]
    fn take_after_give_reuses_allocation() {
        let mut p = pool();
        let buf = p.take(64);
        let ptr = buf.data.as_ptr();
        p.give(buf);
        assert_eq!(p.take(64).data.as_ptr(), ptr);
    }

    /// 12 and 10 bytes share a size class, so the second take recycles the first buffer.
    #[test]
    fn a_recycled_buffer_is_exactly_as_long_as_it_was_asked_for() {
        let mut p = pool();
        let buf = p.take(12);
        p.give(buf);
        assert_eq!(p.take(10).as_slice::<u8>().len(), 10);
    }

    #[test]
    fn an_allocation_over_the_budget_is_refused() {
        let budget = MemoryBudget::new(4096);
        let mut p = BufferPool::with_budget(budget.clone());
        let over = catch_over_budget(|| p.take(8192)).expect_err("8192 does not fit in 4096");
        assert_eq!(over.limit, 4096);
        assert_eq!(over.requested, 8192);
        assert_eq!(budget.used(), 0, "a refused request charges nothing");
    }

    #[test]
    fn the_budget_counts_what_is_already_out() {
        let budget = MemoryBudget::new(4096);
        let mut p = BufferPool::with_budget(budget.clone());
        let _held = p.take(4096);
        assert_eq!(budget.used(), 4096);
        let over = catch_over_budget(|| p.take(8)).expect_err("nothing is left");
        assert_eq!(over.used, 4096);
    }

    #[test]
    fn a_buffer_the_pool_never_gets_back_still_releases_its_charge() {
        let budget = MemoryBudget::new(1 << 20);
        let mut p = BufferPool::with_budget(budget.clone());
        {
            let _dropped_on_the_floor = p.take(4096);
            assert_eq!(budget.used(), 4096);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn a_buffer_returned_to_the_pool_is_still_charged() {
        let budget = MemoryBudget::new(1 << 20);
        let mut p = BufferPool::with_budget(budget.clone());
        let buf = p.take(4096);
        p.give(buf);
        assert_eq!(budget.used(), 4096);
        drop(p);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn a_recycled_buffer_is_not_refused_by_an_exhausted_budget() {
        let budget = MemoryBudget::new(4096);
        let mut p = BufferPool::with_budget(budget.clone());
        let buf = p.take(4096);
        p.give(buf);
        assert_eq!(budget.used(), 4096);
        let reused = catch_over_budget(|| p.take(4096)).expect("recycling asks for nothing new");
        assert_eq!(reused.as_slice::<u8>().len(), 4096);
    }

    #[test]
    fn a_request_too_large_to_round_up_is_refused_rather_than_wrapping() {
        let mut p = BufferPool::with_budget(MemoryBudget::new(1 << 20));
        let over = catch_over_budget(|| p.take(usize::MAX - 1)).expect_err("no power of two holds it");
        assert_eq!(over.requested, usize::MAX - 1);
    }

    #[test]
    fn pools_sharing_a_budget_spend_the_same_ceiling() {
        let budget = MemoryBudget::new(8192);
        let mut first = BufferPool::with_budget(budget.clone());
        let mut second = BufferPool::with_budget(budget.clone());
        let _held = first.take(8192);
        let over = catch_over_budget(|| second.take(8)).expect_err("the other pool spent the budget");
        assert_eq!(over.used, 8192);
    }

    #[test]
    fn an_unbudgeted_pool_refuses_nothing() {
        let mut p = pool();
        let buf = catch_over_budget(|| p.take(1 << 20)).expect("no ceiling to hit");
        assert_eq!(buf.as_slice::<u8>().len(), 1 << 20);
    }

    #[test]
    fn an_unrelated_panic_is_not_mistaken_for_a_refusal() {
        let escaped = std::panic::catch_unwind(|| {
            let _ = catch_over_budget(|| panic!("a real bug"));
        });
        assert!(escaped.is_err(), "the panic resumed rather than being caught");
    }

    #[test]
    fn the_peak_survives_the_buffers_that_set_it() {
        let budget = MemoryBudget::new(1 << 20);
        let mut p = BufferPool::with_budget(budget.clone());
        {
            let _a = p.take(4096);
            let _b = p.take(4096);
        }
        drop(p);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.peak(), 8192);
    }

    /// One batch that needed three buffers of a class keeps them through the window, then the pool settles at what
    /// the batches since needed.
    #[test]
    fn trim_frees_what_recent_batches_did_not_need() {
        let budget = MemoryBudget::new(1 << 20);
        let mut p = BufferPool::with_budget(budget.clone());
        let big: Vec<Buffer> = (0..3).map(|_| p.take(4096)).collect();
        big.into_iter().for_each(|b| p.give(b));
        p.trim();
        assert_eq!(budget.used(), 3 * 4096);
        for _ in 1..TRIM_WINDOW {
            let b = p.take(4096);
            p.give(b);
            p.trim();
        }
        assert_eq!(budget.used(), 3 * 4096, "still inside the window");
        let b = p.take(4096);
        p.give(b);
        p.trim();
        assert_eq!(budget.used(), 4096);
    }

    #[test]
    fn trim_keeps_no_more_idle_than_the_cap() {
        let budget = MemoryBudget::new(1 << 20);
        let mut p = BufferPool::with_budget(budget.clone()).with_idle_cap(3 * 4096);
        let big: Vec<Buffer> = [8192, 8192, 4096, 4096].into_iter().map(|n| p.take(n)).collect();
        big.into_iter().for_each(|b| p.give(b));
        p.trim();
        assert_eq!(budget.used(), 2 * 4096, "the larger buffers go first");
    }

    #[test]
    fn a_refusal_frees_the_idle_buffers_and_asks_again() {
        let budget = MemoryBudget::new(8192);
        let mut p = BufferPool::with_budget(budget.clone());
        let idle = p.take(4096);
        p.give(idle);
        let b = p.take(8192);
        assert_eq!(budget.used(), 8192);
        p.give(b);
    }

    #[test]
    fn a_refusal_with_nothing_idle_is_still_refused() {
        let budget = MemoryBudget::new(8192);
        let mut p = BufferPool::with_budget(budget);
        let _held = p.take(4096);
        assert!(catch_over_budget(|| p.take(8192)).is_err());
    }

    #[test]
    #[should_panic]
    fn cast_to_non_dividing_size_panics() {
        let buf = Buffer::with_capacity(10, &MemoryBudget::unlimited());
        let _ = buf.as_slice::<u64>();
    }
}
