//! Code that decides when workers should go to sleep. See README.md
//! for an overview.

use crossbeam_utils::CachePadded;
use latch::CoreLatch;
use log::Event::*;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    thread_counts: AtomicUsize,
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
            thread_counts: AtomicUsize::new(0),
        }
    }

    fn add_sleeping_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_add(1, Ordering::Relaxed);
    }

    fn sub_sleeping_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the number of threads that are asleep.
    fn load_thread_counts(&self) -> usize {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.load(Ordering::Relaxed)
    }

    #[inline]
    pub(super) fn start_looking(&self, _worker_index: usize) -> usize {
        0
    }

    #[inline]
    pub(super) fn work_found(&self, worker_index: usize, yields: usize) {
        log!(FoundWork {
            worker: worker_index,
            yields: yields,
        });
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
        was_empty: bool,
    ) {
        log!(TickleAny {
            source_worker: source_worker_index,
        });

        let num_sleepers = self.load_thread_counts();
        let num_awake = self.worker_sleep_states.len() - num_sleepers;

        if num_sleepers == 0 {
            // nobody to wake
            return;
        }

        if num_awake > 0 {
            // If there are threads awake, and our queue is empty,
            // then they are doing a good job keeping our queue clear,
            // so we can avoid waking any new threads.
            if was_empty {
                return;
            }
        }

        // Otherwise, we need help!

        // we know we just pushed a new job -- but check also if the
        // queue was empty before we pushed the job. If not, then we
        // should wake *two* threads -- one to handle the previous
        // contents of the queue, and one for the new job.
        let desired_threads = (was_empty as usize) + 1;
        let mut num_to_wake = std::cmp::min(desired_threads, num_sleepers);
        for (i, sleep_state) in self.worker_sleep_states.iter().enumerate() {
            let is_asleep = sleep_state.is_asleep.lock().unwrap();
            if *is_asleep {
                sleep_state.condvar.notify_one();
                log!(TickleAnyTarget {
                    source_worker: source_worker_index,
                    target_worker: i,
                });

                num_to_wake -= 1;
                if num_to_wake == 0 {
                    return;
                }
            }
        }
    }
}
