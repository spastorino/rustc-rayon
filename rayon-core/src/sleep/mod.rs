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

    /// Tracks the number of injected jobs (jobs inserted from outside
    /// the thread pool).  Unlike internal jobs, injected jobs cannot
    /// be "overlooked" or else we risk deadlock.  This means we have
    /// to keep a separate atomic counter to track them. We prefer not
    /// to do this for internal jobs so as to keep the `push`
    /// operation lightweight.
    injection_count: AtomicU64,
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
    rounds: u32,

    /// What was the injection count at the start of going idle?
    injection_count: u64,
}

/// The "sleep state" for an individual worker.
#[derive(Default)]
struct WorkerSleepState {
    is_asleep: Mutex<bool>,
    condvar: Condvar,
}

const ROUNDS_UNTIL_SLEEP: u32 = 16;

impl Sleep {
    pub(super) fn new(n_threads: usize) -> Sleep {
        Sleep {
            worker_sleep_states: (0..n_threads).map(|_| Default::default()).collect(),
            thread_counts: AtomicU64::new(0),
            injection_count: AtomicU64::new(0),
        }
    }

    fn add_idle_thread(&self) {
        // Relaxed suffices: we don't use these reads as a signal that
        // we can read some other memory.
        self.thread_counts.fetch_add(1, Ordering::Relaxed);
    }

    fn add_sleeping_thread(&self) {
        // NB: We need SeqCst ordering on *this operation* to avoid
        // deadlock. See `check_for_injected_job` for details.
        self.thread_counts.fetch_add(0x1_0000_0000, Ordering::SeqCst);
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

        // This is W_I_READ, see `check_for_injected_job`.
        let injection_count = self.injection_count.load(Ordering::SeqCst);

        log!(GotIdle { worker: worker_index, injection_count });

        IdleState {
            worker_index,
            rounds: 0,
            injection_count,
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

        // Flag ourselves as asleep.
        *is_asleep = true;

        if self.check_for_injected_job(idle_state) {
            is_asleep = sleep_state.condvar.wait(is_asleep).unwrap();
        }

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

    // Subtle: we have to be very careful to avoid deadlock around
    // injected jobs. Specifically, this scenario can arise. For
    // simplicity, imagine there is only one worker and one
    // non-worker, outside thread.
    //
    // - Worker thread: Go idle
    // - Worker thread: Search for injected jobs, find nothing
    // - Outside thread: Inject job, but no threads sleeping yet
    // - Worker thread: Increment sleeping counter, go to sleep
    // - Outside thread: Go to sleep waiting for job to be complete
    //
    // Now we are in a state of deadlock: the outside thread is
    // sleeping, waiting for the job, but the worker thread didn't see
    // it, and is also asleep. Ungreat. Note that this cannot happen
    // from a `push` from inside the thread pool, as in that case the
    // "outside thread" is itself a worker.
    //
    // To avoid this, we use the `injection_count` field. It is
    // incremented whenever a new job is injected. Note that this
    // field could wrap-around, that is not really a problem for
    // us. The only thing that's important is that it changes.
    //
    // The protocol now for a worker thread to go to sleep is that
    // it must do at least the following things. I'm going to name
    // these events for convenience:
    //
    // - W_I_READ: Read injection counter and store the result, I (SeqCst).
    //     - This occurs in `start_looking`, when the thread goes idle.
    // - W_SEARCH: Search injection queue at least once (event Search).
    // - W_S_INC: Increment the number of sleeping threads, S (SeqCst).
    // - W_I_CHECK: Check that injection counter still has the value I (SeqCst).
    //
    // Meanwhile, the outside thread will do the following:
    //
    // - O_PUSH: Push a new job onto the injection queue.
    // - O_I_INC: Increment the number of injected jobs (SeqCst)
    // - O_S_CHECK: Check the number of sleeping threads, waking them if needed (SeqCst)
    //
    // If we examine each of the SeqCst options, we see that we cannot
    // miss a wake-up now. For example, the scenario I outlined
    // above had a prefix like this:
    //
    // - W_I_READ -- worker reads counter
    // - W_SEARCH -- worker searches, sees nothing
    // - O_PUSH -- outside thread pushes onto queue
    // - O_I_INC -- increment number of injected jobs
    // - O_S_CHECK -- outside thread checks for num sleepers, sees none
    //
    // But now, when the worker tries to go to sleep, it will fail
    // the W_I_CHECK step, because the number of injected jobs
    // has changed:
    //
    // - W_S_INC -- increment number of sleepers
    // - W_I_CHECK -- check number of injected jobs, sees one, does not go to sleep
    //
    // The relevant orderings:
    //
    // - W_I_READ
    // - ...
    // - W_I_CHECK
    // - O_I_INC
    // - O_S_CHECK -- this will see the W_S_INC and wake the thread
    //
    // or
    //
    // - W_I_READ
    // - ...
    // - O_I_INC
    // - ...
    // - W_I_CHECK -- this will see the O_I_INC and wake the thread
    //
    // or
    //
    // - O_PUSH
    // - O_I_INC
    // - W_I_READ
    // - W_SEARCH -- this will see the O_PUSH and find the job
    fn check_for_injected_job(&self, idle_state: &mut IdleState) -> bool {
        // This is W_I_CHECK.
        let current_injection_count = self.injection_count.load(Ordering::SeqCst);

        if current_injection_count == idle_state.injection_count {
            // Actually sleep.
            true
        } else {
            // Wake up immediately.
            log!(GotInterruptedByInjectedJob {
                worker: idle_state.worker_index,
                injection_count: current_injection_count,
            });
            idle_state.injection_count = current_injection_count;
            false
        }
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
        // This is O_I_INC
        self.injection_count.fetch_add(1, Ordering::SeqCst);

        // This is O_S_CHECK
        let (num_awake_but_idle, num_sleepers) = self.load_thread_counts(Ordering::SeqCst);

        self.new_jobs(source_worker_index, num_jobs, num_awake_but_idle, num_sleepers);
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
        let (num_awake_but_idle, num_sleepers) = self.load_thread_counts(Ordering::Relaxed);

        self.new_jobs(source_worker_index, num_jobs, num_awake_but_idle, num_sleepers);
    }

    /// Common helper for `new_injected_jobs` and `new_internal_jobs`.
    #[inline]
    fn new_jobs(
        &self,
        source_worker_index: usize,
        num_jobs: u32,
        num_awake_but_idle: u32,
        num_sleepers: u32,
    ) {
        log!(TickleAny {
            source_worker: source_worker_index,
        });

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
