use crate::data::Data;
use crate::Args;
use anyhow::Context;
use svg::node;
use svg::node::element::Group;
use svg::Document;
use svg::Node;

const PER_EVENT_WIDTH: usize = 2;
const PER_CPU_HEIGHT: u32 = 2;

// Thanks to plotlib, which I used as a starting point for the svg api :)
//
// - nikomatsakis

pub(crate) fn plot_stacked(args: &Args, data: &Data) -> anyhow::Result<()> {
    let mut document = Document::new();

    let boxes = draw_boxes(args, data);
    document.append(boxes);

    document.append(svg::node::element::Style::new(
        r"
    .event_group {
        opacity: .75;
    }
    .event_group:hover {
        fill: #ec008c;
        opacity: 1;
    }
    ",
    ));

    svg::save(&args.output_path, &document)
        .with_context(|| format!("saving svg to `{}`", args.output_path.display()))?;

    Ok(())
}

fn draw_boxes(_args: &Args, data: &Data) -> Group {
    let mut group = Group::new();

    // figure out how many threads we ever observe at one time
    let num_threads: u32 = data
        .events
        .iter()
        .map(|e| e.num_idle_threads + e.num_sleeping_threads + e.num_notified_threads)
        .max()
        .unwrap_or(0);

    // figure out how many jobs we ever observe at one time
    let num_pending_jobs: u32 = data
        .events
        .iter()
        .map(|e| e.num_pending_jobs + e.injector_size)
        .max()
        .unwrap_or(0);

    let max_value = num_threads.max(num_pending_jobs) * PER_CPU_HEIGHT;

    for (event, index) in data.events.iter().zip(0..) {
        let x_start = index * PER_EVENT_WIDTH;

        let mut start = max_value;

        let mut next_rectangle = |unscaled_height: u32, color: &'static str| {
            let height = unscaled_height * PER_CPU_HEIGHT;
            start -= height;
            node::element::Rectangle::new()
                .set("x", x_start)
                .set("y", start)
                .set("width", PER_EVENT_WIDTH)
                .set("height", height)
                .set("fill", color)
                .set("stroke", "black")
        };

        let event_threads =
            event.num_sleeping_threads + event.num_notified_threads + event.num_idle_threads;
        group.append(
            Group::new()
                .set("class", "event_group")
                .add(next_rectangle(event.num_sleeping_threads, "grey"))
                .add(next_rectangle(event.num_notified_threads, "red"))
                .add(next_rectangle(event.num_idle_threads, "yellow"))
                .add(next_rectangle(num_threads - event_threads, "green")),
        );

        let circle = |unscaled_height: u32, color: &'static str| {
            let cy = max_value - unscaled_height * PER_CPU_HEIGHT;
            node::element::Circle::new()
                .set("cx", x_start + PER_EVENT_WIDTH / 2)
                .set("cy", cy)
                .set("r", PER_EVENT_WIDTH / 2)
                .set("fill", color)
        };

        group.append(circle(event.num_pending_jobs, "red"));
        group.append(circle(event.injector_size, "black"));
    }

    group
}
