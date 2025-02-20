import os
import bytewax.operators as op
from bytewax.testing import TestingSource, run_main
from bytewax.dataflow import Dataflow
from bytewax_s2 import S2Config, S2SinkRecord, S2Sink

AUTH_TOKEN = os.getenv("S2_AUTH_TOKEN")
BASIN = os.getenv("S2_BASIN")
STREAM_PREFIX = os.getenv("S2_STREAM_PREFIX")

eof = 0

flow = Dataflow("s2-sink-test")
ints = op.input("input", flow, TestingSource([1, 2, 3, 4, 5, TestingSource.EOF]))
keyed_ints = op.map(
    "keying",
    ints,
    lambda i: (str(i % 2), S2SinkRecord(body=i.to_bytes(32)))
    if i != TestingSource.EOF
    else (str(eof % 2), S2SinkRecord(body=eof.to_bytes(32))),
)
op.output(
    "output",
    keyed_ints,
    S2Sink(
        config=S2Config(auth_token=AUTH_TOKEN), basin=BASIN, stream_prefix=STREAM_PREFIX
    ),
)

run_main(flow)
