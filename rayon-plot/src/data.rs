//! Parses the CSV values produced by rayon_core::log -- this format
//! is unstable, so this is tied to the specific rayon-core version
//! that we are built with. Also, these two files must be manually
//! integrated right now.

use anyhow::anyhow;
use anyhow::Context;
use std::io::Read;
use std::ops::Range;

#[derive(Default, Debug)]
pub(crate) struct Data {
    pub events: Vec<Event>,
    pub thread_states: Vec<ThreadState>,
}

#[derive(Debug)]
pub(crate) struct Event {
    pub time_stamp: u64,
    pub num_idle_threads: u32,
    pub num_sleeping_threads: u32,
    pub num_notified_threads: u32,
    pub num_pending_jobs: u32,
    pub injector_size: u32,
    pub event_description: String,

    // Range of indices in the thread-state vector
    pub thread_states: Range<usize>,
}

#[derive(Debug)]
pub(crate) struct ThreadState {
    pub code: char,
    pub queue_size: u32,
}

impl Data {
    pub(crate) fn parse(r: impl Read) -> anyhow::Result<Self> {
        let mut reader = csv::Reader::from_reader(r);
        let mut data = Self::default();
        for (result, index) in reader.records().zip(0..) {
            let result = match result {
                Ok(v) => v,
                Err(err) => match err.kind() {
                    csv::ErrorKind::UnequalLengths { .. } => {
                        eprintln!("record #{} is truncated", index);
                        break;
                    }

                    _ => {
                        return Err(err)
                            .with_context(|| format!("error after record #{}", index))
                            .into();
                    }
                },
            };

            // create an iterator over the individual fields on this
            // line; yields up `&str` values
            let mut fields = std::iter::from_fn({
                let mut i = 0;
                move || {
                    let c = i;
                    i += 1;
                    result.get(c).map(|s| s.to_string())
                }
            });

            // first grab the initial set of fields
            macro_rules! get_event_fields {
                ($($field:ident,)*) => {
                    Event {
                        $(
                            $field: fields.next()
                                .ok_or_else(|| anyhow!("no `{}`", stringify!($field)))?
                                .trim()
                                .parse()
                                .with_context(|| {
                                    format!("parsing `{}` on row {}", stringify!($field), index)
                                })?,
                        )*
                            thread_states: (0..0),
                    }
                }
            }
            let mut event = get_event_fields! {
                time_stamp,
                num_idle_threads,
                num_sleeping_threads,
                num_notified_threads,
                num_pending_jobs,
                injector_size,
                event_description,
            };

            // remaining fields are thread states, looking like
            //
            // Txx,C,Q
            //
            // where Txx is the thread identifier, C is the code, and Q is the
            // queue size.
            let start_index = data.thread_states.len();
            while let Some(thread_id) = fields.next() {
                if thread_id.trim().is_empty() {
                    break;
                }

                let code: char = fields
                    .next()
                    .ok_or_else(|| anyhow!("no code for thread `{}`", thread_id))?
                    .trim()
                    .parse()
                    .with_context(|| {
                        format!("parsing code for thread `{}` on row {}", thread_id, index)
                    })?;
                let queue_size_str = fields
                    .next()
                    .ok_or_else(|| anyhow!("no queue size for thread `{}`", thread_id))?;
                let queue_size_str = queue_size_str.trim();
                let queue_size = if queue_size_str.is_empty() {
                    0
                } else {
                    queue_size_str.parse().with_context(|| {
                        format!(
                            "parsing queue size for thread `{}` on row {}",
                            thread_id, index
                        )
                    })?
                };
                data.thread_states.push(ThreadState { code, queue_size });
            }
            event.thread_states = start_index..data.thread_states.len();

            data.events.push(event);
        }

        Ok(data)
    }

    pub(crate) fn truncate(&mut self, n: usize) {
        self.events.truncate(n);
    }
}
