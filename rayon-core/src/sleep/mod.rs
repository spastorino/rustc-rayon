//! Code that decides when workers should go to sleep. See README.md
//! for an overview.

use crossbeam_utils::CachePadded;
use latch::CoreLatch;
use log::Event::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::usize;

pub(super) struct Sleep {
    /// One "sleep state" per worker. Used to track if a worker is sleeping and to have
    /// them block.
    worker_sleep_states: Vec<CachePadded<WorkerSleepState>>,

    /// Low-word: number of idle threads looking for work.
    ///
    /// High-word: number of sleeping threads. Note that a sleeping thread still counts as idle, too.
    thread_counts: AtomicU64,
}

/// The "sleep state" for an individual worker.
#[derive(Default)]
struct WorkerSleepState {
    is_asleep: Mutex<bool>,
    condvar: Condvar,
}

const ROUNDS_UNTIL_SLEEP: usize = 16;

impl Sleep {
    pub(super) fn new(n_threads: usize) -> Sleep {
        Sleep {
            worker_sleep_states: (0..n_threads).map(|_| Default::default()).collect(),
            thread_counts: AtomicU64::new(0),
        }
    }

    fn add_idle_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_add(1, Ordering::Relaxed);
    }

    fn add_sleeping_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_add(0x1_0000_0000, Ordering::Relaxed);
    }

    fn sub_idle_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_sub(0x1, Ordering::Relaxed);
    }

    fn sub_sleeping_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_sub(0x1_0000_0000, Ordering::Relaxed);
    }

    fn load_thread_counts(&self) -> (u32, u32) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        let thread_counts = self.thread_counts.load(Ordering::Relaxed);
        (thread_counts as u32, (thread_counts >> 32) as u32)
    }

    #[inline]
    pub(super) fn start_looking(&self, _worker_index: usize) -> usize {
        self.add_idle_thread();
        0
    }

    #[inline]
    pub(super) fn work_found(&self, worker_index: usize, yields: usize) {
        log!(FoundWork {
            worker: worker_index,
            yields: yields,
        });
        self.sub_idle_thread();
    }

    #[inline]
    pub(super) fn no_work_found(
        &self,
        worker_index: usize,
        yields: usize,
        latch: &CoreLatch,
    ) -> usize {
        log!(DidNotFindWork {
            worker: worker_index,
            yields: yields,
        });
        if yields < ROUNDS_UNTIL_SLEEP {
            thread::yield_now();
            yields + 1
        } else {
            debug_assert_eq!(yields, ROUNDS_UNTIL_SLEEP);
            self.sleep(worker_index, latch);
            0
        }
    }

    #[cold]
    fn sleep(&self, worker_index: usize, latch: &CoreLatch) {
        let latch_addr = latch as *const CoreLatch as usize;

        log!(GetSleepy {
            worker: worker_index,
            latch_addr: latch_addr,
        });

        if !latch.get_sleepy() {
            log!(GotInterrupted {
                worker: worker_index,
                latch_addr: latch_addr,
            });

            return;
        }

        log!(GotSleepy {
            worker: worker_index,
            latch_addr: latch_addr,
        });

        let sleep_state = &self.worker_sleep_states[worker_index];
        let mut is_asleep = sleep_state.is_asleep.lock().unwrap();
        debug_assert!(!*is_asleep);

        if !latch.fall_asleep() {
            log!(GotInterrupted {
                worker: worker_index,
                latch_addr: latch_addr,
            });

            return;
        }

        // Don't do this in a loop. If we do it in a loop, we need
        // some way to distinguish the ABA scenario where the pool
        // was awoken but before we could process it somebody went
        // to sleep. Note that if we get a false wakeup it's not a
        // problem for us, we'll just loop around and maybe get
        // sleepy again.

        log!(FellAsleep {
            worker: worker_index,
            latch_addr: latch_addr,
        });

        // Increase the count of the number of sleepers.
        //
        // Relaxed ordering suffices here because in no case do we
        // gate other reads on this value.
        self.add_sleeping_thread();

        // Flag ourselves as asleep.
        *is_asleep = true;

        is_asleep = sleep_state.condvar.wait(is_asleep).unwrap();

        // Flag ourselves as awake.
        *is_asleep = false;

        // Decrease number of sleepers.
        //
        // Relaxed ordering suffices here because in no case do we
        // gate other reads on this value.
        self.sub_sleeping_thread();

        log!(GotAwoken {
            worker: worker_index,
            latch_addr,
        });

        latch.wake_up();
    }

    /// A "tickle" is used to indicate that an event of interest has
    /// occurred.
    ///
    /// The `source_worker_index` is the thread index that caused the
    /// event, or `std::usize::MAX` if that is not readily
    /// identified. This is used only for logging.
    ///
    /// The `target_worker_index` is the thread index to awaken, or
    /// `std::usize::MAX` if the event is not targeting any specific
    /// thread. This is used (e.g.) when a latch is set, to awaken
    /// just the thread that was blocking on the latch.
    pub(super) fn tickle_one(&self, source_worker_index: usize, target_worker_index: usize) {
        log!(TickleOne {
            source_worker: source_worker_index,
            target_worker: target_worker_index,
        });

        // We need to acquire the worker's lock and check whether they
        // are sleeping.
        let sleep_state = &self.worker_sleep_states[target_worker_index];
        let is_asleep = sleep_state.is_asleep.lock().unwrap();
        if *is_asleep {
            sleep_state.condvar.notify_one();
        }
    }

    /// See `tickle_one` -- this method is used to tickle any single
    /// worker, but it doesn't matter which one. This occurs typically
    /// when a new bit of stealable work has arrived.
    #[inline]
    pub(super) fn tickle_any(
        &self,
        source_worker_index: usize,
        _idle_threads: impl FnOnce() -> bool,
    ) {
        log!(TickleAny {
            source_worker: source_worker_index,
        });

        let (num_idle, num_sleepers) = self.load_thread_counts();

        if num_sleepers == 0 {
            // nobody to wake
            return;
        }

        if num_idle > num_sleepers {
            // still have idle threads looking for work, don't go
            // waking up new ones
            return;
        }

        let mut num_awake = self.worker_sleep_states.len() as u32 - num_sleepers;
        for (i, sleep_state) in self.worker_sleep_states.iter().enumerate() {
            let is_asleep = sleep_state.is_asleep.lock().unwrap();
            if *is_asleep {
                sleep_state.condvar.notify_one();
                log!(TickleAnyTarget {
                    source_worker: source_worker_index,
                    target_worker: i,
                });
                num_awake += 1;
                if is_power_of_2(num_awake) {
                    return;
                }
            }
        }
    }
}

fn is_power_of_2(v: u32) -> bool {
    (v & v.wrapping_sub(1)) == 0
}
