use crate::data::Data;
use crate::Args;

pub(crate) fn stats(_args: &Args, data: &Data) -> anyhow::Result<()> {
    let mut overworked_count: usize = 0;

    for event in &data.events {
        if event.is_overworked() {
            overworked_count += 1;
        }
    }

    println!("total events: {}", data.events.len());
    println!("overworked #: {}", overworked_count);
    println!(
        "sample %    : {:.0}",
        (overworked_count as f64 / data.events.len() as f64) * 100.0
    );

    Ok(())
}
