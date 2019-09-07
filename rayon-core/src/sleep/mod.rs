//! Code that decides when workers should go to sleep. See README.md
//! for an overview.

use crossbeam_utils::CachePadded;
use log::Event::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::usize;

pub(super) struct Sleep {
    /// Stores simultaneously whether any workers are *sleeping* and the thread-id
    /// of the sleepy worker:
    ///
    /// - The least significant bit stores if there **may** be workers *sleeping*
    /// - The remaining bits, if 0, indicate that there is no current sleepy worker
    /// - Otherwise, the remaining bits store N + 1 where N is the thread-id of the sleepy worker
    ///
    /// It is important to note that the SLEEPING bit is an
    /// approximation. In some race conditions, it is possible for a
    /// thread to set the bit, go to sleep, and then get awoken
    /// without the bit having been cleared. This is harmless: it can
    /// cause us to do a bit of extra work to acquire locks and
    /// things, but shouldn't lead to any further problems.  (The
    /// **definitive** value as to whether a thread is asleep is
    /// stored in the sleep state for the worker.)
    ///
    /// Example:
    ///
    /// A state value of 5, or `0b101` in binary, would mean:
    ///
    /// - There are sleepy workers (LSB is 1)
    /// - The worker with id 1 is sleepy (about to fall asleep)
    ///     - Determine by shifting right to `0b10` and then subtracting 1 to yield `0b1`
    state: AtomicUsize,

    /// One "sleep state" per worker. Used to track if a worker is sleeping and to have
    /// them block.
    worker_sleep_states: Vec<CachePadded<WorkerSleepState>>,
}

/// The "sleep state" for an individual worker.
#[derive(Default)]
struct WorkerSleepState {
    is_asleep: Mutex<bool>,
    condvar: Condvar,
}

/// The `state` value that indicates (a) no workers are sleeping and
/// (b) there is no sleepy worker presently.
const AWAKE: usize = 0;

/// The `state` value that indicates (a) there are workers sleeping
/// and (b) there is no sleepy worker presently.
const SLEEPING: usize = 1;

const ROUNDS_UNTIL_SLEEPY: usize = 32;
const ROUNDS_UNTIL_ASLEEP: usize = 64;

impl Sleep {
    pub(super) fn new(n_threads: usize) -> Sleep {
        Sleep {
            state: AtomicUsize::new(AWAKE),
            worker_sleep_states: (0..n_threads).map(|_| Default::default()).collect(),
        }
    }

    /// Test `state` to see if any workers have fallen asleep.
    fn anyone_sleeping(&self, state: usize) -> bool {
        state & SLEEPING != 0
    }

    /// Test `state` to see if we have a current sleepy worker.
    fn any_worker_is_sleepy(&self, state: usize) -> bool {
        (state >> 1) != 0
    }

    /// Test `state` to see if `worker_index` is the sleepy worker.
    fn worker_is_sleepy(&self, state: usize, worker_index: usize) -> bool {
        (state >> 1) == (worker_index + 1)
    }

    /// Return a new state value in which `worker_index` is the sleepy worker.
    /// There must not have been a sleepy worker before.
    fn with_sleepy_worker(&self, state: usize, worker_index: usize) -> usize {
        debug_assert!(state == AWAKE || state == SLEEPING);
        ((worker_index + 1) << 1) + state
    }

    /// Return a new state value in which there is no sleepy worker,
    /// but the presence or absence of sleeping workers is preserved.
    fn clear_sleepy_worker(&self, state: usize) -> usize {
        state & 1
    }

    #[inline]
    pub(super) fn work_found(&self, worker_index: usize, yields: usize) -> usize {
        log!(FoundWork {
            worker: worker_index,
            yields: yields,
        });
        if yields > ROUNDS_UNTIL_SLEEPY {
            self.caffeinate(worker_index);
        }
        0
    }

    #[inline]
    pub(super) fn no_work_found(&self, worker_index: usize, yields: usize) -> usize {
        log!(DidNotFindWork {
            worker: worker_index,
            yields: yields,
        });
        if yields < ROUNDS_UNTIL_SLEEPY {
            thread::yield_now();
            yields + 1
        } else if yields == ROUNDS_UNTIL_SLEEPY {
            thread::yield_now();
            if self.get_sleepy(worker_index) {
                yields + 1
            } else {
                yields
            }
        } else if yields < ROUNDS_UNTIL_ASLEEP {
            thread::yield_now();
            if self.still_sleepy(worker_index) {
                yields + 1
            } else {
                log!(GotInterrupted {
                    worker: worker_index
                });
                0
            }
        } else {
            debug_assert_eq!(yields, ROUNDS_UNTIL_ASLEEP);
            self.sleep(worker_index);
            0
        }
    }

    /// If `worker_index` is sleepy, this will return them to the
    /// fully awake state. Has no effect if the worker is not sleepy
    /// -- this includes if the worker is fully asleep!
    ///
    /// Returns true if the worker *was* sleepy but now is not.
    fn caffeinate(&self, worker_index: usize) -> bool {
        loop {
            // (*) Any ordering should suffice on this load: if we get
            // some outdated value, the `compare_exchange` below must
            // fail and we'll loop around again.
            let old_state = self.state.load(Ordering::Relaxed);
            if self.worker_is_sleepy(old_state, worker_index) {
                // At this point, we are the sleepy worker. We want to
                // set the state to have no sleepy worker -- but we
                // need to preserve the fact that someone may be
                // SLEEPING.
                let new_state = self.clear_sleepy_worker(old_state);
                if self
                    .state
                    .compare_exchange(old_state, new_state, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    return true;
                }
            } else {
                return false;
            }
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
        let old_state = self.state.load(Ordering::SeqCst);
        if old_state != AWAKE {
            self.tickle_one_cold(old_state, source_worker_index, target_worker_index);
        }
    }

    #[cold]
    fn tickle_one_cold(
        &self,
        old_state: usize,
        source_worker_index: usize,
        target_worker_index: usize,
    ) {
        log!(TickleOne {
            source_worker: source_worker_index,
            target_worker: target_worker_index,
            old_state: old_state,
        });

        // It's possible that `target_worker_index` is sleepy, in
        // which case we may be able to caffeinate them before they
        // fall asleep. In that case, we're all done here.
        if self.worker_is_sleepy(old_state, target_worker_index) {
            if self.caffeinate(target_worker_index) {
                return;
            }
        }

        // Otherwise, we need to acquire the worker's lock and check
        // whether they are sleeping. Note that we cannot rely on the
        // result of `self.anyone_sleeping(old_state)` here -- this is
        // because it is possible that our worker has come and gone to
        // sleep since we did that load. Without acquiring the lock,
        // we simply cannot make the check atomic.
        let sleep_state = &self.worker_sleep_states[target_worker_index];
        let is_asleep = sleep_state.is_asleep.lock().unwrap();
        if *is_asleep {
            sleep_state.condvar.notify_one();
        }

        // Note that we also cannot clear the SLEEPING bit from the
        // state here, as other works may be asleep. It's ok, that bit
        // is an over-approximation.
    }

    /// See `tickle_one` -- this method is used to tickle any single
    /// worker, but it doesn't matter which one. This occurs typically
    /// when a new bit of stealable work has arrived.
    pub(super) fn tickle_any(&self, source_worker_index: usize) {
        self.tickle_all(source_worker_index);
    }

    /// See `tickle_one` -- this method is used to tickle all workers, but it doesn't
    /// matter which one.
    pub(super) fn tickle_all(&self, source_worker_index: usize) {
        // As described in README.md, this load must be SeqCst so as to ensure that:
        // - if anyone is sleepy or asleep, we *definitely* see that now (and not eventually);
        // - if anyone after us becomes sleepy or asleep, they see memory events that
        //   precede the call to `tickle()`, even though we did not do a write.
        let old_state = self.state.load(Ordering::SeqCst);
        if old_state != AWAKE {
            self.tickle_all_cold(source_worker_index);
        }
    }

    #[cold]
    fn tickle_all_cold(&self, source_worker_index: usize) {
        // The `Release` ordering here suffices. The reasoning is that
        // the atomic's own natural ordering ensure that any attempt
        // to become sleepy/asleep either will come before/after this
        // swap. If it comes *after*, then Release is good because we
        // want it to see the action that generated this tickle. If it
        // comes *before*, then we will see it here (but not other
        // memory writes from that thread).  If the other worker was
        // becoming sleepy, the other writes don't matter. If they
        // were were going to sleep, we will acquire lock and hence
        // acquire their reads.
        let old_state = self.state.swap(AWAKE, Ordering::Release);
        log!(TickleAll {
            source_worker: source_worker_index,
            old_state: old_state,
        });

        // Now that we have changed the state, we also want to awaken
        // any threads that are asleep. Note that this a lot could
        // happen between us doing the AWAKE and actually waking
        // threads -- e.g., other thread *could* even go to sleep in
        // the meantime.
        //
        // This is actually *mildly* odd, in that the state could be
        // out of date -- it may indicate some thread is sleeping when
        // in fact they are not. This is because the thread would've been
        // notified by us, but we are not modifying the state variable.
        //
        // But this doesn't do really do any harm -- such a thread
        // would loop around and, if there's no work, eventually get
        // sleepy again. But it's good to remember that the sleepy bit
        // is not reliable.
        if self.anyone_sleeping(old_state) {
            for sleep_state in &self.worker_sleep_states {
                let is_asleep = sleep_state.is_asleep.lock().unwrap();
                if *is_asleep {
                    sleep_state.condvar.notify_one();
                }
            }
        }
    }

    fn get_sleepy(&self, worker_index: usize) -> bool {
        loop {
            // Acquire ordering suffices here. If some other worker
            // was sleepy but no longer is, we will eventually see
            // that, and until then it doesn't hurt to spin.
            // Otherwise, we will do a compare-exchange which will
            // assert a stronger order and acquire any reads etc that
            // we must see.
            let state = self.state.load(Ordering::Acquire);
            log!(GetSleepy {
                worker: worker_index,
                state: state,
            });
            if self.any_worker_is_sleepy(state) {
                // somebody else is already sleepy, so we'll just wait our turn
                debug_assert!(
                    !self.worker_is_sleepy(state, worker_index),
                    "worker {} called `is_sleepy()`, \
                     but they are already sleepy (state={})",
                    worker_index,
                    state
                );
                return false;
            } else {
                // make ourselves the sleepy one
                let new_state = self.with_sleepy_worker(state, worker_index);

                // This must be SeqCst on success because we want to
                // ensure:
                //
                // - That we observe any writes that preceded
                //   some prior tickle, and that tickle may have only
                //   done a SeqCst load on `self.state`.
                // - That any subsequent tickle *definitely* sees this store.
                //
                // See the section on "Ensuring Sequentially
                // Consistency" in README.md for more details.
                //
                // The failure ordering doesn't matter since we are
                // about to spin around and do a fresh load.
                if self
                    .state
                    .compare_exchange(state, new_state, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    log!(GotSleepy {
                        worker: worker_index,
                        old_state: state,
                        new_state: new_state,
                    });
                    return true;
                }
            }
        }
    }

    fn still_sleepy(&self, worker_index: usize) -> bool {
        let state = self.state.load(Ordering::SeqCst);
        self.worker_is_sleepy(state, worker_index)
    }

    fn sleep(&self, worker_index: usize) {
        loop {
            // Acquire here suffices. If we observe that the current worker is still
            // sleepy, then in fact we know that no writes have occurred, and anyhow
            // we are going to do a CAS which will synchronize.
            //
            // If we observe that the state has changed, it must be
            // due to a tickle, and then the Acquire means we also see
            // any events that occured before that.
            let state = self.state.load(Ordering::Acquire);
            if self.worker_is_sleepy(state, worker_index) {
                // It is important that we hold the lock when we do
                // the CAS. Otherwise, if we were to CAS first, then
                // the following sequence of events could occur:
                //
                // - Thread A (us) sets state to SLEEPING.
                // - Thread B sets state to AWAKE.
                // - Thread C sets state to SLEEPY(C).
                // - Thread C sets state to SLEEPING.
                // - Thread A reawakens, acquires lock, and goes to sleep.
                //
                // Now we missed the wake-up from thread B! But since
                // we have the lock when we set the state to sleeping,
                // that cannot happen. Note that the swap `tickle()`
                // is not part of the lock, though, so let's play that
                // out:
                //
                // # Scenario 1
                //
                // - A loads state and sees SLEEPY(A)
                // - B swaps to AWAKE.
                // - A locks, fails CAS
                //
                // # Scenario 2
                //
                // - A loads state and see SLEEPY(A)
                // - A locks, performs CAS
                // - B swaps to AWAKE.
                // - A waits (releasing lock)
                // - B locks, notifies
                //
                // In general, acquiring the lock inside the loop
                // seems like it could lead to bad performance, but
                // actually it should be ok. This is because the only
                // reason for the `compare_exchange` to fail is if an
                // awaken comes, in which case the next cycle around
                // the loop will just return.
                let sleep_state = &self.worker_sleep_states[worker_index];
                let mut is_asleep = sleep_state.is_asleep.lock().unwrap();
                debug_assert!(!*is_asleep);

                // This must be SeqCst on success because we want to
                // ensure:
                //
                // - That we observe any writes that preceded
                //   some prior tickle, and that tickle may have only
                //   done a SeqCst load on `self.state`.
                // - That any subsequent tickle *definitely* sees this store.
                //
                // See the section on "Ensuring Sequentially
                // Consistency" in README.md for more details.
                //
                // The failure ordering doesn't matter since we are
                // about to spin around and do a fresh load.
                if self
                    .state
                    .compare_exchange(state, SLEEPING, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    // Don't do this in a loop. If we do it in a loop, we need
                    // some way to distinguish the ABA scenario where the pool
                    // was awoken but before we could process it somebody went
                    // to sleep. Note that if we get a false wakeup it's not a
                    // problem for us, we'll just loop around and maybe get
                    // sleepy again.
                    log!(FellAsleep {
                        worker: worker_index
                    });
                    *is_asleep = true;
                    is_asleep = sleep_state.condvar.wait(is_asleep).unwrap();
                    *is_asleep = false;
                    log!(GotAwoken {
                        worker: worker_index
                    });
                    return;
                }
            } else {
                log!(GotInterrupted {
                    worker: worker_index
                });
                return;
            }
        }
    }
}
