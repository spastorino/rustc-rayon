use anyhow::Context;
use std::fs::File;
use structopt::StructOpt;

mod data;
use data::Data;

mod plot;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(StructOpt, Debug)]
struct Args {
    data_path: std::path::PathBuf,
    output_path: std::path::PathBuf,

    #[structopt(long="width", help="width of the output image, in pixels")]
    width: Option<usize>,

    #[structopt(long="truncate", help="only consider the first N events")]
    truncate: Option<usize>,

    #[structopt(long="sample", help="only consider every Nth event")]
    sample: Option<usize>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::from_args();
    let file = File::open(&args.data_path)
        .with_context(|| format!("opening `{}`", args.data_path.display()))?;
    let mut data =
        Data::parse(file).with_context(|| format!("parsing `{}`", args.data_path.display()))?;

    println!("loaded {} events", data.events.len());

    if let Some(n) = args.sample {
        let sampled_events =
            data.events.iter()
            .zip(0..)
            .filter_map(|(event, index)| if index % n == 0 { Some(event.clone()) } else { None })
            .collect();
        data.events = sampled_events;
    }

    if let Some(n) = args.truncate {
        data.truncate(n);
    }

    plot::plot_stacked(&args, &data)?;

    Ok(())
}
