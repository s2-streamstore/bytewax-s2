import json
import os
import signal
import sys
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import plotext as plt  # type: ignore
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    count_window,
)
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from rich.ansi import AnsiDecoder
from rich.console import Group
from rich.jupyter import JupyterMixin
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel

from bytewax_s2 import S2Config, S2Source

AUTH_TOKEN = os.environ["S2_AUTH_TOKEN"]
BASIN = os.environ["S2_BASIN"]
OFFSITE_KIND = os.environ["OFFSITE_KIND"]
OFFSITES_BY_KIND = {
    "ecommerce": ["amazon.com", "ebay.com", "etsy.com"],
    "social": ["facebook.com", "reddit.com", "youtube.com"],
    "news": ["bbc.com", "cbc.ca", "cnn.com"],
}
if OFFSITE_KIND not in OFFSITES_BY_KIND:
    raise ValueError("OFFSITE_KIND is invalid")


class PlotextMixin(JupyterMixin):
    def __init__(self, labels: list[str], values: list):
        self.decoder = AnsiDecoder()
        self.labels = labels
        self.values = values

    def __rich_console__(self, _console, _options):
        plt.simple_bar(self.labels, self.values, width=100)
        canvas = plt.build()
        self.rich_canvas = Group(*self.decoder.decode(canvas))
        yield self.rich_canvas


def panel(metrics: dict, labels: list[str], title: str) -> Panel:
    return Panel(
        PlotextMixin(labels, [metrics[label] for label in labels]),
        title=title,
    )


class StreamingBarChart(StatelessSinkPartition):
    def __init__(self, labels: list[str], title_prefix: str):
        self.last_window_id = None
        self.labels = labels
        self.recent_title = f"{title_prefix} (recent)"
        self.cumu_title = f"{title_prefix} (cumulative)"
        self.recent = {label: 0 for label in labels}
        self.cumu = {label: 0 for label in labels}
        self.layout = Layout(name="root")
        self.layout.split_column(
            Layout(name="recent"),
            Layout(name="cumu"),
        )
        self.layout["recent"].update(panel(self.recent, self.labels, self.recent_title))
        self.layout["cumu"].update(panel(self.cumu, self.labels, self.cumu_title))
        self.live = Live(self.layout, auto_refresh=False)
        self.live.start(refresh=True)

    def write_batch(self, items: list[dict]):
        for item in items:
            window_id = item["window_id"]
            if window_id < 0:
                continue
            if self.last_window_id is None:
                self.last_window_id = window_id
            if window_id > self.last_window_id:
                self.layout["recent"].update(
                    panel(self.recent, self.labels, self.recent_title)
                )
                self.layout["cumu"].update(
                    panel(self.cumu, self.labels, self.cumu_title)
                )
                self.live.refresh()
                self.last_window_id = window_id
                self.recent = {label: 0 for label in self.labels}
            self.recent[item["label"]] += item["value"]
            self.cumu[item["label"]] += item["value"]

    def close(self):
        self.live.stop()


class TerminalBarChartSink(DynamicSink):
    def __init__(self, labels: list[str], title_prefix: str):
        self.labels = labels
        self.title_prefix = title_prefix

    def build(self, _step_id, _worker_index, worker_count) -> StreamingBarChart:
        if worker_count != 1:
            raise ValueError(
                "TerminalBarChartSink must not be used with more than one worker"
            )
        self.chart = StreamingBarChart(
            labels=self.labels, title_prefix=self.title_prefix
        )
        return self.chart

    def close(self):
        self.chart.close()


def extract_timestamp(event: dict) -> datetime:
    return datetime.fromisoformat(event["observed_at"])


clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = TumblingWindower(
    length=timedelta(seconds=10),
    align_to=datetime.now(timezone.utc),
)


chart = TerminalBarChartSink(
    labels=OFFSITES_BY_KIND[OFFSITE_KIND], title_prefix="Offsite mentions in Bluesky"
)


def graceful_exit(_signum, _frame):
    chart.close()
    sys.exit(0)


signal.signal(signal.SIGTSTP, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

try:
    flow = Dataflow("s2_source_example")
    offsite_posts_raw = op.input(
        "s2_source",
        flow,
        S2Source(
            config=S2Config(auth_token=AUTH_TOKEN),
            basin=BASIN,
            stream_prefix=OFFSITE_KIND,
        ),
    )
    offsite_posts = op.map(
        "parse_offsite_posts", offsite_posts_raw, lambda sr: json.loads(sr.body)
    )
    windowed_offsite_mentions = count_window(
        "count_offsite_mentions",
        offsite_posts,
        clock,
        windower,
        lambda event: event["offsite"],
    )
    formatted_offsite_mentions = op.map(
        "format_offsite_mentions",
        windowed_offsite_mentions.down,
        lambda x: {"window_id": x[1][0], "label": x[0], "value": x[1][1]},
    )
    op.output("chart_sink", formatted_offsite_mentions, chart)
except KeyboardInterrupt:
    chart.close()
