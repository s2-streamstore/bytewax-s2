import os
import bytewax.operators as op
from bytewax.testing import TestingSink, run_main
from bytewax.dataflow import Dataflow
from bytewax_s2 import S2Config, S2Source

AUTH_TOKEN = os.getenv("S2_AUTH_TOKEN")
BASIN = os.getenv("S2_BASIN")
STREAM_PREFIX = os.getenv("S2_STREAM_PREFIX")

sink = []

flow = Dataflow("s2-source-test")
int_bytes = op.input(
    "input",
    flow,
    S2Source(
        config=S2Config(auth_token=AUTH_TOKEN),
        basin=BASIN,
        stream_prefix=STREAM_PREFIX,
        tail=False,
    ),
)
ints = op.map("parse-ints", int_bytes, lambda sr: int.from_bytes(sr.body))
op.output("output", ints, TestingSink(sink))

run_main(flow)
assert len(sink) > 0 and all(0 <= i <= 5 for i in sink)
