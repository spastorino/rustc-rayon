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
        r#"
    .event_group {
        opacity: .75;
    }
    .tooltip_group {
        opacity: 0;
    }
    .event_group:hover {
        fill: #ec008c;
        opacity: 1;
    }
    .event_group:hover + .tooltip_group {
        opacity: 1;
    }
    .tooltip_group {
        font-family: "Courier";
        font-size: 10px;
    }
    "#,
    ));

    svg::save(&args.output_path, &document)
        .with_context(|| format!("saving svg to `{}`", args.output_path.display()))?;

    Ok(())
}

fn draw_boxes(args: &Args, data: &Data) -> Group {
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

        let mut event_group = Group::new().set("class", "event_group foo");

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
        event_group.append(next_rectangle(event.num_sleeping_threads, "grey"));
        event_group.append(next_rectangle(event.num_notified_threads, "red"));
        event_group.append(next_rectangle(event.num_idle_threads, "yellow"));
        event_group.append(next_rectangle(num_threads - event_threads, "green"));

        if event.is_overworked() {
            event_group.append(
                node::element::Rectangle::new()
                    .set("x", x_start)
                    .set("y", max_value)
                    .set("width", PER_EVENT_WIDTH)
                    .set("height", PER_CPU_HEIGHT)
                    .set("fill", "red")
                    .set("stroke", "black"),
            );
        }

        let circle = |unscaled_height: u32, color: &'static str| {
            let cy = max_value - unscaled_height * PER_CPU_HEIGHT;
            node::element::Circle::new()
                .set("cx", x_start + PER_EVENT_WIDTH / 2)
                .set("cy", cy)
                .set("r", PER_EVENT_WIDTH / 2)
                .set("fill", color)
        };

        event_group.append(circle(event.num_pending_jobs, "red"));
        event_group.append(circle(event.injector_size, "black"));

        let tooltip_text = format!(
            "\
line number     : {line_number}
active threads  : {active_threads}
idle threads    : {idle_threads}
sleeping threads: {sleeping_threads}
notified threads: {notified_threads}
pending jobs    : {pending_jobs}
injected jobs   : {injector_size}
event           : {event_description}
",
            line_number = event.line_number,
            active_threads = num_threads
                - event.num_idle_threads
                - event.num_sleeping_threads
                - event.num_notified_threads,
            idle_threads = event.num_idle_threads,
            sleeping_threads = event.num_sleeping_threads,
            notified_threads = event.num_notified_threads,
            pending_jobs = event.num_pending_jobs,
            injector_size = event.injector_size,
            event_description = event.event_description,
        );

        let max_line_len = tooltip_text.lines().map(|s| s.len()).max().unwrap_or(0);
        let num_lines = tooltip_text.lines().count();

        let opt_tooltip_group = if args.elide_text {
            None
        } else {
            let mut tooltip_group = node::element::Group::new()
                .set("class", "tooltip_group")
                .set(
                    "transform",
                    format!("translate({}, {})", x_start, max_value + PER_CPU_HEIGHT),
                )
                .add(
                    node::element::Rectangle::new()
                        .set("x", 0)
                        .set("y", 0)
                        .set("width", format!("{}em", max_line_len * 2 / 3))
                        .set("height", format!("{}ex", num_lines * 3 + 2))
                        .set("fill", "yellow")
                        .set("stroke", "black"),
                );

            let mut text_group = Group::new().set("transform", format!("translate(0, 2ex)"));
            for (line, line_num) in tooltip_text.lines().zip(0..) {
                text_group.append(
                    node::element::Text::new()
                        .set("x", 0)
                        .set("y", format!("{}ex", line_num * 3 + 2))
                        .set("width", "22em")
                        .set("height", "22em")
                        .set("fill", "yellow")
                        .set("stroke", "black")
                        .add(node::Text::new(line.to_string())),
                );
            }
            tooltip_group.append(text_group);
            Some(tooltip_group)
        };

        // it is important in the CSS that these are adjacent siblings!
        group.append(event_group);
        if let Some(tooltip_group) = opt_tooltip_group {
            group.append(tooltip_group);
        }
    }

    group
}
