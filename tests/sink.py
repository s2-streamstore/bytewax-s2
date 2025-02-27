import os

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

from bytewax_s2 import S2Config, S2Sink, S2SinkRecord

AUTH_TOKEN = os.environ["S2_AUTH_TOKEN"]
BASIN = os.environ["S2_BASIN"]
STREAM_PREFIX = os.environ["S2_STREAM_PREFIX"]

eof = 0


def key(i: object) -> tuple[str, S2SinkRecord]:
    if isinstance(i, int):
        return (str(i % 2), S2SinkRecord(body=i.to_bytes(32)))
    elif isinstance(i, TestingSource.EOF):
        return (str(eof % 2), S2SinkRecord(body=eof.to_bytes(32)))
    else:
        raise RuntimeError(f"Unexpected type: {type(i)} for input: {i}")


flow = Dataflow("s2_sink_test")
ints = op.input("input", flow, TestingSource([1, 2, 3, 4, 5, TestingSource.EOF()]))
keyed_ints = op.map(
    "key",
    ints,
    key,
)
op.output(
    "output",
    keyed_ints,
    S2Sink(
        config=S2Config(auth_token=AUTH_TOKEN), basin=BASIN, stream_prefix=STREAM_PREFIX
    ),
)

run_main(flow)
