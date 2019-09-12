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

/// An instance of this struct is created when a thread becomes idle.
/// It is consumed when the thread finds work, and passed by `&mut`
/// reference for operations that preserve the idle state. (In other
/// words, producing one of these structs is evidence the thread is
/// idle.) It tracks state such as how long the thread has been idle.
pub(super) struct IdleState {
    /// What is worker index of the idle thread?
    worker_index: usize,

    /// How many rounds have we been circling without sleeping?
    rounds: usize,
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

    /// Returns `(num_awake_but_idle, num_sleeping)`, the pair of:
    ///
    /// - the number of threads that are awake but idle, looking for work
    /// - the number of threads that are asleep
    fn load_thread_counts(&self) -> (u32, u32) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        let thread_counts = self.thread_counts.load(Ordering::Relaxed);
        let num_sleeping = (thread_counts >> 32) as u32;
        let num_awake_but_idle = (thread_counts as u32) - num_sleeping;
        (num_awake_but_idle, num_sleeping)
    }

    #[inline]
    pub(super) fn start_looking(&self, worker_index: usize) -> IdleState {
        self.add_idle_thread();
        IdleState { worker_index, rounds: 0 }
    }

    #[inline]
    pub(super) fn work_found(&self, idle_state: IdleState) {
        log!(FoundWork {
            worker: idle_state.worker_index,
            yields: idle_state.rounds,
        });
        self.sub_idle_thread();
    }

    #[inline]
    pub(super) fn no_work_found(
        &self,
        idle_state: &mut IdleState,
        latch: &CoreLatch,
    ) {
        log!(DidNotFindWork {
            worker: idle_state.worker_index,
            yields: idle_state.rounds,
        });
        if idle_state.rounds < ROUNDS_UNTIL_SLEEP {
            thread::yield_now();
            idle_state.rounds += 1;
        } else {
            debug_assert_eq!(idle_state.rounds, ROUNDS_UNTIL_SLEEP);
            self.sleep(idle_state, latch);
            idle_state.rounds = 0;
        }
    }

    #[cold]
    fn sleep(&self, idle_state: &IdleState, latch: &CoreLatch) {
        let latch_addr = latch as *const CoreLatch as usize;
        let worker_index = idle_state.worker_index;

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

    /// Signals that `num_jobs` new jobs were pushed and made
    /// available for idle workers to steal. This function will try to
    /// ensure that there are threads available to service those jobs,
    /// waking threads from sleep if necessary.
    ///
    /// # Parameters
    ///
    /// - `source_worker_index` -- index of the thread that did the
    ///   push, or `usize::MAX` if this came from outside the thread
    ///   pool -- it is used only for logging.
    /// - `num_jobs` -- lower bound on number of jobs available for stealing.
    ///   We'll try to get at least one thread per job.
    #[inline]
    pub(super) fn new_jobs(
        &self,
        source_worker_index: usize,
        num_jobs: u32,
    ) {
        log!(TickleAny {
            source_worker: source_worker_index,
        });

        let (num_awake_but_idle, num_sleepers) = self.load_thread_counts();

        if num_sleepers == 0 {
            // nobody to wake
            return;
        }

        if num_awake_but_idle >= num_jobs {
            // still have idle threads looking for work, don't go
            // waking up new ones
            return;
        }

        let mut num_to_wake = std::cmp::min(num_jobs - num_awake_but_idle, num_sleepers);
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
