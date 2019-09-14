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

    /// Packs together two 32-bit counters, JOBS and SLEEPY, into one
    /// 64-bit world. These counters are used as part of a protocol
    /// to help ensure we don't miss work.
    j_s_counters: AtomicU64,
}

#[derive(Copy, Clone, Debug)]
struct SleepyCounter(u32);

#[derive(Copy, Clone, Debug)]
struct JobsCounter(u32);

const INVALID_SLEEPY_COUNTER: SleepyCounter = SleepyCounter(std::u32::MAX);

/// An instance of this struct is created when a thread becomes idle.
/// It is consumed when the thread finds work, and passed by `&mut`
/// reference for operations that preserve the idle state. (In other
/// words, producing one of these structs is evidence the thread is
/// idle.) It tracks state such as how long the thread has been idle.
pub(super) struct IdleState {
    /// What is worker index of the idle thread?
    worker_index: usize,

    /// How many rounds have we been circling without sleeping?
    rounds: u32,

    /// Once we become sleepy, what was the sleepy counter value?
    /// Set to `INVALID_SLEEPY_COUNTER` otherwise.
    sleepy_counter: SleepyCounter,
}

/// The "sleep state" for an individual worker.
#[derive(Default)]
struct WorkerSleepState {
    is_asleep: Mutex<bool>,
    condvar: Condvar,
}

const ROUNDS_UNTIL_SLEEPY: u32 = 128;
const ROUNDS_UNTIL_SLEEPING: u32 = ROUNDS_UNTIL_SLEEPY + 1;

/// A value of 1 in the "most significant word" (that is, the upper 32
/// bits of a 64 bit counter). Used because we frequently pack two 32
/// bit counters into a 64 bit word.
const MSW_ONE: u64 = 0x1_0000_0000;

/// A value of 1 in the "least significant word" (that is, the upper 32
/// bits of a 64 bit counter). Used because we frequently pack two 32
/// bit counters into a 64 bit word.
const LSW_ONE: u64 = 0x1;

impl Sleep {
    pub(super) fn new(n_threads: usize) -> Sleep {
        Sleep {
            worker_sleep_states: (0..n_threads).map(|_| Default::default()).collect(),
            thread_counts: AtomicU64::new(0),
            j_s_counters: AtomicU64::new(0),
        }
    }

    fn add_idle_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_add(LSW_ONE, Ordering::Relaxed);
    }

    fn add_sleeping_thread(&self) {
        // NB: We need SeqCst ordering on *this operation* to avoid
        // deadlock. See `check_for_injected_job` for details.
        self.thread_counts.fetch_add(MSW_ONE, Ordering::SeqCst);
    }

    fn sub_idle_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_sub(LSW_ONE, Ordering::Relaxed);
    }

    fn sub_sleeping_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_sub(MSW_ONE, Ordering::Relaxed);
    }

    fn split_j_s_counters(counters: u64) -> (JobsCounter, SleepyCounter) {
        let jobs_counter = JobsCounter(counters as u32);
        let sleepy_counter = SleepyCounter((counters >> 32) as u32);
        debug_assert!(jobs_counter.0 <= sleepy_counter.0);
        (jobs_counter, sleepy_counter)
    }

    fn increment_sleepy_counter(counters: u64) -> u64 {
        counters + MSW_ONE
    }

    fn replicate_sleepy_counter(sleepy_counter: SleepyCounter) -> u64 {
        debug_assert!(sleepy_counter.0 < std::u32::MAX);
        let lc = sleepy_counter.0 as u64;
        lc << 32 | lc
    }

    fn announce_sleepy(&self, worker_index: usize) -> SleepyCounter {
        loop {
            let counters = self.j_s_counters.load(Ordering::Relaxed);
            let (_, sleepy_counter) = Self::split_j_s_counters(counters);
            if sleepy_counter.0 == std::u32::MAX {
                if self.j_s_counters.compare_exchange(counters, 0, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                    log!(AnnouncedSleepy { worker: worker_index, sleepy_counter: 0 });
                    return SleepyCounter(0);
                }
            } else {
                let counters1 = Self::increment_sleepy_counter(counters);
                if self.j_s_counters.compare_exchange(counters, counters1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                    log!(AnnouncedSleepy { worker: worker_index, sleepy_counter: sleepy_counter.0 });
                    return sleepy_counter;
                }
            }
        }
    }

    fn announce_job(&self, worker_index: usize) {
        loop {
            let counters = self.j_s_counters.load(Ordering::SeqCst);
            let (jobs_counter, sleepy_counter) = Self::split_j_s_counters(counters);
            if jobs_counter.0 == sleepy_counter.0 {
                log!(JobAnnounceEq { worker: worker_index, jobs_counter: jobs_counter.0 });
                return;
            }

            let counters1 = Self::replicate_sleepy_counter(sleepy_counter);
            if self.j_s_counters.compare_exchange(counters, counters1, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                log!(JobAnnounceBump {
                    worker: worker_index,
                    jobs_counter: jobs_counter.0,
                    sleepy_counter: sleepy_counter.0,
                });
                return;
            }
        }
    }

    fn check_still_sleepy(&self, old_sleepy_counter: SleepyCounter) -> bool {
        debug_assert!(old_sleepy_counter.0 != std::u32::MAX);

        let counters = self.j_s_counters.load(Ordering::SeqCst);
        let (jobs_counter, sleepy_counter) = Self::split_j_s_counters(counters);

        if jobs_counter.0 > sleepy_counter.0 {
            // Somebody published a job since we became sleepy.
            return false;
        }

        if sleepy_counter.0 < old_sleepy_counter.0 {
            // Sleepy counter rolled over since we became sleepy.
            return false;
        }

        true
    }

    /// Returns `(num_awake_but_idle, num_sleeping)`, the pair of:
    ///
    /// - the number of threads that are awake but idle, looking for work
    /// - the number of threads that are asleep
    fn load_thread_counts(&self, ordering: Ordering) -> (u32, u32) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        let thread_counts = self.thread_counts.load(ordering);
        let num_sleeping = (thread_counts >> 32) as u32;
        let num_awake_but_idle = (thread_counts as u32) - num_sleeping;
        (num_awake_but_idle, num_sleeping)
    }

    #[inline]
    pub(super) fn start_looking(&self, worker_index: usize) -> IdleState {
        self.add_idle_thread();

        log!(GotIdle { worker: worker_index, });

        IdleState {
            worker_index,
            rounds: 0,
            sleepy_counter: SleepyCounter(std::u32::MAX),
        }
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
        if idle_state.rounds < ROUNDS_UNTIL_SLEEPY {
            thread::yield_now();
            idle_state.rounds += 1;
        } else if idle_state.rounds == ROUNDS_UNTIL_SLEEPY {
            idle_state.sleepy_counter = self.announce_sleepy(idle_state.worker_index);
            idle_state.rounds += 1;
            thread::yield_now();
        } else if idle_state.rounds < ROUNDS_UNTIL_SLEEPING {
            idle_state.rounds += 1;
            thread::yield_now();
        } else {
            debug_assert_eq!(idle_state.rounds, ROUNDS_UNTIL_SLEEPING);
            self.sleep(idle_state, latch);
        }
    }

    #[cold]
    fn sleep(&self, idle_state: &mut IdleState, latch: &CoreLatch) {
        let latch_addr = latch as *const CoreLatch as usize;
        let worker_index = idle_state.worker_index;

        log!(GetSleepy {
            worker: worker_index,
            latch_addr: latch_addr,
        });

        if !latch.get_sleepy() {
            log!(GotInterruptedByLatch {
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
            log!(GotInterruptedByLatch {
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
        // This is W_S_INC in `check_for_injected_job`.
        self.add_sleeping_thread();

        // We need to do this check that we are still sleepy *after*
        // we add a sleeping thread.
        if self.check_still_sleepy(idle_state.sleepy_counter) {
            // Flag ourselves as asleep.
            *is_asleep = true;

            is_asleep = sleep_state.condvar.wait(is_asleep).unwrap();

            // Flag ourselves as awake.
            *is_asleep = false;

            idle_state.rounds = 0;
            idle_state.sleepy_counter = INVALID_SLEEPY_COUNTER;
        } else {
            log!(GotInterruptedByInjectedJob {
                worker: worker_index,
            });

            idle_state.rounds = ROUNDS_UNTIL_SLEEPING;
            idle_state.sleepy_counter = INVALID_SLEEPY_COUNTER;
        }

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


    /// Signals that `num_jobs` new jobs were injected into the thread
    /// pool from outside. This function will ensure that there are
    /// threads available to process them, waking threads from sleep
    /// if necessary.
    ///
    /// # Parameters
    ///
    /// - `source_worker_index` -- index of the thread that did the
    ///   push, or `usize::MAX` if this came from outside the thread
    ///   pool -- it is used only for logging.
    /// - `num_jobs` -- lower bound on number of jobs available for stealing.
    ///   We'll try to get at least one thread per job.
    #[inline]
    pub(super) fn new_injected_jobs(
        &self,
        source_worker_index: usize,
        num_jobs: u32,
    ) {
        self.new_jobs(source_worker_index, num_jobs)
    }

    /// Signals that `num_jobs` new jobs were pushed onto a thread's
    /// local deque. This function will try to ensure that there are
    /// threads available to process them, waking threads from sleep
    /// if necessary. However, this is not guaranteed: under certain
    /// race conditions, the function may fail to wake any new
    /// threads; in that case the existing thread should eventually
    /// pop the job.
    ///
    /// # Parameters
    ///
    /// - `source_worker_index` -- index of the thread that did the
    ///   push, or `usize::MAX` if this came from outside the thread
    ///   pool -- it is used only for logging.
    /// - `num_jobs` -- lower bound on number of jobs available for stealing.
    ///   We'll try to get at least one thread per job.
    #[inline]
    pub(super) fn new_internal_jobs(
        &self,
        source_worker_index: usize,
        num_jobs: u32,
    ) {
        self.new_jobs(source_worker_index, num_jobs)
    }

    /// Common helper for `new_injected_jobs` and `new_internal_jobs`.
    #[inline]
    fn new_jobs(
        &self,
        source_worker_index: usize,
        num_jobs: u32,
    ) {
        log!(TickleAny {
            source_worker: source_worker_index,
        });

        self.announce_job(source_worker_index);

        // We can use relaxed here. Reasoning:
        //
        // - when going to sleep, we first increment the thread-count
        //   and then (seqcst) clear the sleepy worker (op CLEAR) in
        //   the same thread.
        // - if we failed to clear sleepy worker, then our load comes (op LOAD)
        //   after after the clear (op CLEAR).
        // - therefore, our load should see all writes visible to CLEAR,
        //   including the "sleeping thread" count increment.
        let (num_awake_but_idle, num_sleepers) = self.load_thread_counts(Ordering::Relaxed);

        if num_sleepers == 0 {
            // nobody to wake
            return;
        }

        if num_awake_but_idle >= num_jobs {
            // still have idle threads looking for work, don't go
            // waking up new ones
            return;
        }

        self.new_jobs_cold(source_worker_index, num_jobs, num_awake_but_idle, num_sleepers);
    }

    #[cold]
    fn new_jobs_cold(
        &self,
        source_worker_index: usize,
        num_jobs: u32,
        num_awake_but_idle: u32,
        num_sleepers: u32,
    ) {
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
