//! Debug Logging
//!
//! To use in a debug build, set the env var `RAYON_RS_LOG=1`.  In a
//! release build, logs are compiled out by default unless Rayon is built
//! with `--cfg rayon_rs_log` (try `RUSTFLAGS="--cfg rayon_rs_log"`).
//!
//! Note that logs are an internally debugging tool and their format
//! is considered unstable, as are the details of how to enable them.

use crossbeam_channel::{self, Sender, Receiver};
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};

pub(super) const LOGS_COMPILED_IN: bool = cfg!(any(debug_assertions,rayon_rs_log));

#[derive(Debug)]
pub(super) enum Event {
    TickleOne {
        source_worker: usize,
        target_worker: usize,
    },
    TickleAny {
        source_worker: usize,
        num_jobs: u32,
    },
    TickleAnyThreadCounts {
        source_worker: usize,
        num_jobs: u32,
        num_awake_but_idle: u32,
        num_sleepers: u32,
    },
    TickleAnyWakeThread {
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

/// Handle to the logging thread, if any. You can use this to deliver
/// logs. You can also clone it freely.
#[derive(Clone)]
pub(super) struct Logger {
    sender: Option<Sender<Event>>,
}

impl Logger {
    pub(super) fn new() -> Logger {
        if !LOGS_COMPILED_IN {
            return Self::disabled();
        }

        let env_log = match env::var("RAYON_RS_LOG") {
            Ok(s) => s,
            Err(_) => return Self::disabled(),
        };

        let (sender, receiver) = crossbeam_channel::unbounded();
        ::std::thread::spawn(move || Self::logger_thread(env_log, receiver));

        return Logger { sender: Some(sender) };
    }

    fn disabled() -> Logger {
        Logger { sender: None }
    }

    #[inline]
    pub(super) fn log(&self, event: impl FnOnce() -> Event) {
        if !LOGS_COMPILED_IN {
            return;
        }

        if let Some(sender) = &self.sender {
            sender.send(event()).unwrap();
        }
    }

    fn logger_thread(
        log_filename: String,
        receiver: Receiver<Event>,
    ) {
        let file = File::create(&log_filename).unwrap_or_else(|err| {
            panic!("failed to open `{}`: {}", log_filename, err)
        });

        let mut writer = BufWriter::new(file);
        for event in receiver {
            write!(writer, "{:?}\n", event).unwrap();
            writer.flush().unwrap();
        }
    }
}
