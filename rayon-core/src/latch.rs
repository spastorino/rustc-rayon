use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use registry::{Registry, WorkerThread};

/// We define various kinds of latches, which are all a primitive signaling
/// mechanism. A latch starts as false. Eventually someone calls `set()` and
/// it becomes true. You can test if it has been set by calling `probe()`.
///
/// Some kinds of latches, but not all, support a `wait()` operation
/// that will wait until the latch is set, blocking efficiently. That
/// is not part of the trait since it is not possibly to do with all
/// latches.
///
/// The intention is that `set()` is called once, but `probe()` may be
/// called any number of times. Once `probe()` returns true, the memory
/// effects that occurred before `set()` become visible.
///
/// It'd probably be better to refactor the API into two paired types,
/// but that's a bit of work, and this is not a public API.
///
/// ## Memory ordering
///
/// Latches need to guarantee two things:
///
/// - Once `probe()` returns true, all memory effects from the `set()`
///   are visible (in other words, the set should synchronize-with
///   the probe).
/// - Once `set()` occurs, the next `probe()` *will* observe it.  This
///   typically requires a seq-cst ordering. See [the "tickle-then-get-sleepy" scenario in the sleep
///   README](/src/sleep/README.md#tickle-then-get-sleepy) for details.
pub(super) trait Latch {
    /// Set the latch, signalling others.
    fn set(&self);
}

pub(super) trait AsCoreLatch {
    fn as_core_latch(&self) -> &CoreLatch;
}

/// Latch is not set, owning thread is awake
const UNSET: usize = 0;

/// Latch is not set, owning thread is going to sleep on this latch
/// (but has not yet fallen asleep).
const SLEEPY: usize = 1;

/// Latch is not set, owning thread is asleep on this latch and
/// must be awoken.
const SLEEPING: usize = 2;

/// Latch is set.
const SET: usize = 3;

/// Spin latches are the simplest, most efficient kind, but they do
/// not support a `wait()` operation. They just have a boolean flag
/// that becomes true when `set()` is called.
#[derive(Debug)]
pub(super) struct CoreLatch {
    state: AtomicUsize,
}

impl CoreLatch {
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
        }
    }

    /// Invoked by owning thread as it prepares to sleep. Returns true
    /// if the owning thread may proceed to fall asleep, false if the
    /// latch was set in the meantime.
    pub(super) fn get_sleepy(&self) -> bool {
        self
            .state
            .compare_exchange(UNSET, SLEEPY, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Invoked by owning thread as it falls asleep sleep. Returns
    /// true if the owning thread should block, or false if the latch
    /// was set in the meantime.
    pub(super) fn fall_asleep(&self) -> bool {
        self
            .state
            .compare_exchange(SLEEPY, SLEEPING, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Invoked by owning thread as it falls asleep sleep. Returns
    /// true if the owning thread should block, or false if the latch
    /// was set in the meantime.
    pub(super) fn wake_up(&self) {
        if !self.probe() {
            let _ =
                self
                .state
                .compare_exchange(SLEEPING, UNSET, Ordering::SeqCst, Ordering::Relaxed);
        }
    }

    /// Set the latch. If this returns true, the owning thread was sleeping
    /// and must be awoken.
    ///
    /// This is private because, typically, setting a latch involves
    /// doing some wakeups; those are encapsulated in the surrounding
    /// latch code.
    fn set(&self) -> bool {
        let old_state = self.state.swap(SET, Ordering::AcqRel);
        old_state == SLEEPING
    }

    /// Test if this latch has been set.
    pub(super) fn probe(&self) -> bool {
        self.state.load(Ordering::Acquire) == SET
    }
}

/// Spin latches are the simplest, most efficient kind, but they do
/// not support a `wait()` operation. They just have a boolean flag
/// that becomes true when `set()` is called.
pub(super) struct SpinLatch<'r> {
    core_latch: CoreLatch,
    registry: &'r Registry,
    target_worker_index: usize,
}

impl<'r> SpinLatch<'r> {
    /// Creates a new spin latch that is owned by `thread`. This means
    /// that `thread` is the only thread that should be blocking on
    /// this latch -- it also means that when the latch is set, we
    /// will wake `thread` if it is sleeping.
    #[inline]
    pub(super) fn new(thread: &'r WorkerThread) -> SpinLatch {
        SpinLatch {
            core_latch: CoreLatch::new(),
            registry: thread.registry(),
            target_worker_index: thread.index(),
        }
    }

    pub(super) fn probe(&self) -> bool {
        self.core_latch.probe()
    }
}

impl<'r> AsCoreLatch for SpinLatch<'r> {
    #[inline]
    fn as_core_latch(&self) -> &CoreLatch {
        &self.core_latch
    }
}

impl<'r> Latch for SpinLatch<'r> {
    #[inline]
    fn set(&self) {
        if self.core_latch.set() {
            self.registry.tickle_worker(self.target_worker_index);
        }
    }
}

/// A Latch starts as false and eventually becomes true. You can block
/// until it becomes true.
pub(super) struct LockLatch {
    m: Mutex<bool>,
    v: Condvar,
}

impl LockLatch {
    #[inline]
    pub(super) fn new() -> LockLatch {
        LockLatch {
            m: Mutex::new(false),
            v: Condvar::new(),
        }
    }

    /// Block until latch is set, then resets this lock latch so it can be reused again.
    pub(super) fn wait_and_reset(&self) {
        let mut guard = self.m.lock().unwrap();
        while !*guard {
            guard = self.v.wait(guard).unwrap();
        }
        *guard = false;
    }

    /// Block until latch is set.
    pub(super) fn wait(&self) {
        let mut guard = self.m.lock().unwrap();
        while !*guard {
            guard = self.v.wait(guard).unwrap();
        }
    }
}

impl Latch for LockLatch {
    #[inline]
    fn set(&self) {
        let mut guard = self.m.lock().unwrap();
        *guard = true;
        self.v.notify_all();
    }
}

/// Counting latches are used to implement scopes. They track a
/// counter. Unlike other latches, calling `set()` does not
/// necessarily make the latch be considered `set()`; instead, it just
/// decrements the counter. The latch is only "set" (in the sense that
/// `probe()` returns true) once the counter reaches zero.
///
/// Note: like a `SpinLatch`, count laches are always associated with
/// some registry that is probing them, which must be tickled when
/// they are set. *Unlike* a `SpinLatch`, they don't themselves hold a
/// reference to that registry. This is because in some cases the
/// registry owns the count-latch, and that would create a cycle. So a
/// `CountLatch` must be given a reference to its owning registry when
/// it is set. For this reason, it does not implement the `Latch`
/// trait (but it doesn't have to, as it is not used in those generic
/// contexts).
#[derive(Debug)]
pub(super) struct CountLatch {
    core_latch: CoreLatch,
    counter: AtomicUsize,
}

impl CountLatch {
    #[inline]
    pub(super) fn new() -> CountLatch {
        CountLatch {
            core_latch: CoreLatch::new(),
            counter: AtomicUsize::new(1),
        }
    }

    #[inline]
    pub(super) fn increment(&self) {
        debug_assert!(!self.core_latch.probe());
        self.counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the latch counter by one. If this is the final
    /// count, then the latch is **set**, and calls to `probe()` will
    /// return true. Returns whether the latch was set. This is an
    /// internal operation, as it does not tickle, and to fail to
    /// tickle would lead to deadlock.
    #[inline]
    fn set(&self) -> bool {
        if self.counter.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.core_latch.set();
            true
        } else {
            false
        }
    }

    /// Decrements the latch counter by one and possibly set it.  If
    /// the latch is set, then the specific worker thread is tickled,
    /// which should be the one that owns this latch.
    ///
    /// FIXME: currently, we just tickle all threads.
    #[inline]
    pub(super) fn set_and_tickle_one(&self, registry: &Registry, target_worker_index: usize) {
        if self.set() {
            registry.tickle_worker(target_worker_index);
        }
    }
}

impl AsCoreLatch for CountLatch {
    #[inline]
    fn as_core_latch(&self) -> &CoreLatch {
        &self.core_latch
    }
}

impl<'a, L> Latch for &'a L
where
    L: Latch,
{
    fn set(&self) {
        L::set(self);
    }
}
