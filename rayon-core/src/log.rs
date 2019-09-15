//! Debug Logging
//!
//! To use in a debug build, set the env var `RAYON_RS_LOG=1`.  In a
//! release build, logs are compiled out by default unless Rayon is built
//! with `--cfg rayon_rs_log` (try `RUSTFLAGS="--cfg rayon_rs_log"`).
//!
//! Note that logs are an internally debugging tool and their format
//! is considered unstable, as are the details of how to enable them.

#[cfg(any(debug_assertions,rayon_rs_log))]
use std::env;

#[cfg_attr(any(debug_assertions,rayon_rs_log), derive(Debug))]
#[cfg_attr(not(any(debug_assertions,rayon_rs_log)), allow(dead_code))]
pub(super) enum Event {
    TickleOne {
        source_worker: usize,
        target_worker: usize,
    },
    TickleAny {
        source_worker: usize,
    },
    TickleAnyTarget {
        source_worker: usize,
        target_worker: usize,
    },
    GotIdle {
        worker: usize,
    },
    AnnouncedSleepy {
        worker: usize,
        sleepy_counter: u32,
    },
    JobAnnounceEq {
        worker: usize,
        jobs_counter: u32,
    },
    JobAnnounceBump {
        worker: usize,
        jobs_counter: u32,
        sleepy_counter: u32,
    },
    GetSleepy {
        worker: usize,
        latch_addr: usize,
    },
    GotSleepy {
        worker: usize,
        latch_addr: usize,
    },
    GotAwoken {
        worker: usize,
        latch_addr: usize,
    },
    FellAsleep {
        worker: usize,
        latch_addr: usize,
    },
    GotInterruptedByLatch {
        worker: usize,
        latch_addr: usize,
    },
    GotInterruptedByInjectedJob {
        worker: usize,
    },
    FoundWork {
        worker: usize,
        yields: u32,
    },
    DidNotFindWork {
        worker: usize,
        yields: u32,
    },
    StoleWork {
        worker: usize,
        victim: usize,
    },
    UninjectedWork {
        worker: usize,
    },
    WaitUntil {
        worker: usize,
    },
    SawLatchSet {
        worker: usize,
        latch_addr: usize,
    },
    LatchSet {
        latch_addr: usize,
    },
    LockLatchSet {
        latch_addr: usize,
    },
    LockLatchWait {
        latch_addr: usize,
    },
    LockLatchWaitAndReset {
        latch_addr: usize,
    },
    LockLatchWaitComplete {
        latch_addr: usize,
    },
    LockLatchWaitAndResetComplete {
        latch_addr: usize,
    },
    InjectJobs {
        count: usize,
    },
    Join {
        worker: usize,
    },
    PoppedJob {
        worker: usize,
    },
    PoppedRhs {
        worker: usize,
    },
    LostJob {
        worker: usize,
    },
    JobCompletedOk {
        owner_thread: usize,
    },
    JobPanickedErrorStored {
        owner_thread: usize,
    },
    JobPanickedErrorNotStored {
        owner_thread: usize,
    },
    ScopeCompletePanicked {
        owner_thread: usize,
    },
    ScopeCompleteNoPanic {
        owner_thread: usize,
    },
    TerminateLatch {
        worker: usize,
        latch_addr: usize,
    },
}

#[cfg(any(debug_assertions,rayon_rs_log))]
lazy_static! {
    pub(super) static ref LOG_ENV: bool = env::var("RAYON_RS_LOG").is_ok();
}

#[cfg(any(debug_assertions,rayon_rs_log))]
macro_rules! log {
    ($event:expr) => {
        if *$crate::log::LOG_ENV {
            eprintln!("{:?}", $event);
        }
    };
}

#[cfg(not(any(debug_assertions,rayon_rs_log)))]
macro_rules! log {
    ($event:expr) => {
        if false {
            // Expand `$event` so it still appears used, but without
            // any of the formatting code to be optimized away.
            $event;
        }
    };
}
