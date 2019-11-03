use crate::data::Data;
use crate::Args;
use anyhow::Context;
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::VecDeque;

static JOB_PUSHED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"JobPushed \{ worker: (\d+) \}").unwrap());

static JOB_POPPED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"JobPopped(?:Rhs)? \{ worker: (\d+) \}").unwrap());

static JOB_STOLEN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"JobStolen \{ worker: (\d+), victim: (\d+) \}").unwrap());

static JOBS_INJECTED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"JobsInjected \{ count: (\d+) \}").unwrap());

static JOB_UNINJECTED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"JobUninjected \{ worker: (\d+) \}").unwrap());

pub(crate) fn stats(_args: &Args, data: &Data) -> anyhow::Result<()> {
    let mut overworked_count: usize = 0;

    // For each thread, track the deque of jobs. Each job is
    // represented by the index in which it was injected.
    let mut jobs: Vec<VecDeque<usize>> = (0..data.num_threads).map(|_| VecDeque::new()).collect();
    let mut injected_jobs: VecDeque<usize> = VecDeque::new();

    let mut total_job_wait_time = 0;
    let mut jobs_added = 0;
    let mut jobs_pushed = 0;
    let mut jobs_injected = 0;
    let mut jobs_removed = 0;
    let mut jobs_popped = 0;
    let mut jobs_stolen = 0;
    let mut jobs_uninjected = 0;

    for (event, index) in data.events.iter().zip(0..) {
        if event.is_overworked() {
            overworked_count += 1;
        }

        if let Some(cap) = JOB_PUSHED.captures(&event.event_description) {
            let worker: usize = cap[1]
                .parse()
                .with_context(|| format!("parsing {:?}", event.event_description))?;
            jobs[worker].push_back(index);
            jobs_added += 1;
            jobs_pushed += 1;
        } else if let Some(cap) = JOB_POPPED.captures(&event.event_description) {
            let worker: usize = cap[1].parse().with_context(|| {
                format!("parsing {:?} from {:?}", &cap[1], event.event_description)
            })?;
            let job_pushed_index = jobs[worker].pop_back().unwrap();
            total_job_wait_time += index - job_pushed_index;
            jobs_removed += 1;
            jobs_popped += 1;
        } else if let Some(cap) = JOB_STOLEN.captures(&event.event_description) {
            let victim: usize = cap[2]
                .parse()
                .with_context(|| format!("parsing {:?}", event.event_description))?;
            let job_pushed_index = jobs[victim].pop_front().unwrap();
            total_job_wait_time += index - job_pushed_index;
            jobs_removed += 1;
            jobs_stolen += 1;
        } else if let Some(cap) = JOBS_INJECTED.captures(&event.event_description) {
            let count: usize = cap[1]
                .parse()
                .with_context(|| format!("parsing {:?}", event.event_description))?;
            for _ in 0..count {
                injected_jobs.push_back(index);
                jobs_added += 1;
                jobs_injected += 1;
            }
        } else if let Some(_cap) = JOB_UNINJECTED.captures(&event.event_description) {
            let job_pushed_index = injected_jobs.pop_front().unwrap();
            total_job_wait_time += index - job_pushed_index;
            jobs_removed += 1;
            jobs_uninjected += 1;
        }
    }

    println!("total events: {}", data.events.len());
    println!("overworked #: {}", overworked_count);
    println!(
        "sample %    : {:.0}",
        (overworked_count as f64 / data.events.len() as f64) * 100.0
    );
    println!("jobs added  : {}", jobs_added);
    println!("     pushed : {}", jobs_pushed);
    println!("   injected : {}", jobs_injected);
    println!("jobs removed: {}", jobs_removed);
    println!("     popped : {}", jobs_popped);
    println!("     stolen : {}", jobs_stolen);
    println!(" uninjected : {}", jobs_uninjected);
    println!(
        "job latency : {:.2}",
        total_job_wait_time as f64 / jobs_removed as f64
    );

    Ok(())
}
