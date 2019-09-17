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

/// True if logs are compiled in.
pub(super) const LOG_ENABLED: bool = cfg!(rayon_rs_log);

#[derive(Debug)]
pub(super) enum Event {
    /// Flushes events to disk, used to terminate benchmarking.
    Flush,

    /// Indicates that a worker thread started execution.
    ThreadStart { worker: usize, terminate_addr: usize },

    /// Indicates that a worker thread became idle, blocked on `latch_addr`.
    ThreadIdle { worker: usize, latch_addr: usize },

    /// Indicates that an idle worker thread found work to do, after
    /// yield rounds. It should no longer be considered idle.
    ThreadFoundWork { worker: usize, yields: u32 },

    /// Indicates that a worker blocked on a latch observed that it was set.
    ///
    /// Internal debugging event that does not affect the state
    /// machine.
    ThreadSawLatchSet { worker: usize, latch_addr: usize },

    /// Indicates that an idle worker thread searched for another round without finding work.
    ThreadNoWork { worker: usize, yields: u32 },

    /// Indicates that an idle worker is getting sleepy. `sleepy_counter` is the internal
    /// sleep state that we saw at the time.
    ThreadSleepy { worker: usize, sleepy_counter: u32 },

    /// Indicates that the thread's attempt to fall asleep was
    /// interrupted because the latch was set. (This is not, in and of
    /// itself, a change to the thread state.)
    ThreadSleepInterruptedByLatch { worker: usize, latch_addr: usize },

    /// Indicates that the thread's attempt to fall asleep was
    /// interrupted because a job was posted. (This is not, in and of
    /// itself, a change to the thread state.)
    ThreadSleepInterruptedByJob { worker: usize },

    /// Indicates that an idle worker has gone to sleep.
    ThreadSleeping { worker: usize, latch_addr: usize },

    /// Indicates that a sleeping worker has awoken.
    ThreadAwoken { worker: usize, latch_addr: usize },

    /// Indicates that the given worker thread was notified it should
    /// awaken because its latch was set.
    ThreadNotifyLatch { worker: usize },

    /// Indicates that the given worker thread was notified it should
    /// awake because new work may be available.
    ThreadNotifyJob { source_worker: usize, target_worker: usize },

    /// The given worker has pushed a job to its local deque.
    JobPushed { worker: usize },

    /// The given worker has popped a job from its local deque.
    JobPopped { worker: usize },

    /// The given worker has popped a job from its local deque, and
    /// the job was the RHS of the `join` we were blocked on.
    ///
    /// Identical to `JobPopped` but for debugging.
    JobPoppedRhs { worker: usize },

    /// The given worker has stolen a job from the deque of another.
    JobStolen { worker: usize, victim: usize },

    /// N jobs were injected into the global queue.
    JobsInjected { count: usize },

    /// A job was removed from the global queue.
    JobUninjected { worker: usize },

    /// A job was "announced", but no threads were sleepy.
    ///
    /// No effect on thread state, just a debugging event.
    JobAnnounceEq {
        worker: usize,
        jobs_counter: u32,
    },

    /// A job was "announced", and threads were sleepy. We equalized
    /// the counters.
    ///
    /// No effect on thread state, just a debugging event.
    JobAnnounceBump {
        worker: usize,
        jobs_counter: u32,
        sleepy_counter: u32,
    },

    /// When announcing a job, this was the value of the counters we observed.
    ///
    /// No effect on thread state, just a debugging event.
    JobThreadCounts {
        worker: usize,
        num_awake_but_idle: u32,
        num_sleepers: u32,
    },

    /// Indicates that a job completed "ok" as part of a scope.
    JobCompletedOk {
        owner_thread: usize,
    },

    /// Indicates that a job panicked as part of a scope, and the
    /// error was stored for later.
    ///
    /// Useful for debugging.
    JobPanickedErrorStored {
        owner_thread: usize,
    },

    /// Indicates that a job panicked as part of a scope, and the
    /// error was discarded.
    ///
    /// Useful for debugging.
    JobPanickedErrorNotStored {
        owner_thread: usize,
    },

    /// Indicates that a scope completed with a panic.
    ///
    /// Useful for debugging.
    ScopeCompletePanicked {
        owner_thread: usize,
    },

    /// Indicates that a scope completed with a panic.
    ///
    /// Useful for debugging.
    ScopeCompleteNoPanic {
        owner_thread: usize,
    },
}

struct StampedEvent {
    time_stamp: u64,
    event: Event,
}

/// Handle to the logging thread, if any. You can use this to deliver
/// logs. You can also clone it freely.
#[derive(Clone)]
pub(super) struct Logger {
    sender: Option<Sender<Event>>,
}

impl Logger {
    pub(super) fn new() -> Logger {
        if !LOG_ENABLED {
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
        if !LOG_ENABLED {
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

        const CAPACITY: usize = 1000;
        let mut writer = BufWriter::new(file);
        let mut events = Vec::with_capacity(CAPACITY);
        let mut incoming = receiver.into_iter();

        loop {
            while let Some(event) = incoming.next() {
                match event {
                    Event::Flush => break,
                    _ => events.push(event),
                }

                if events.len() == CAPACITY {
                    break;
                }
            }

            for event in events.drain(..) {
                write!(writer, "{:?}\n", event).unwrap();
                writer.flush().unwrap();
            }
        }
    }
}

enum State {
    WORKING,
    IDLE,
    SLEEPING,
}

struct SimulatorState {
    num_workers: usize,
    local_queue_size: Vec<usize>,
    thread_states: Vec<State>,
    injector_size: usize,
}

impl SimulatorState {
    fn simulate(&mut self, event: &Event) {
        match event {
            _ => { }
        }
    }
}
