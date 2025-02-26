import json
import os
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import websockets
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async

from bytewax_s2 import S2Config, S2Sink, S2SinkPartitionFn, S2SinkRecord

AUTH_TOKEN = os.environ["S2_AUTH_TOKEN"]
BASIN = os.environ["S2_BASIN"]
OFFSITE_TO_STREAM = {
    "amazon.com": "ecommerce/amazon",
    "ebay.com": "ecommerce/ebay",
    "etsy.com": "ecommerce/etsy",
    "facebook.com": "social/facebook",
    "reddit.com": "social/reddit",
    "youtube.com": "social/youtube",
    "bbc.com": "news/bbc",
    "cbc.ca": "news/cbc",
    "cnn.com": "news/cnn",
}


async def wss_agen(uri: str):
    async with websockets.connect(uri) as websocket:
        while True:
            event = await websocket.recv()
            yield json.loads(event)


class JetstreamSourcePartition(StatefulSourcePartition):
    def __init__(self, uri: str):
        self.batcher = batch_async(wss_agen(uri), timedelta(seconds=0.5), 1000)

    def next_batch(self):
        return next(self.batcher)

    def snapshot(self):
        pass


class JetstreamSource(FixedPartitionedSource):
    def __init__(self, wss_uri: str):
        self.uri = wss_uri

    def list_parts(self):
        return [self.uri]

    def build_part(self, _step_id, for_part, _resume_state):
        return JetstreamSourcePartition(uri=for_part)


def is_english_post(event: dict) -> bool:
    if (
        event.get("kind") == "commit"
        and event.get("commit", {}).get("operation") == "create"
        and event.get("commit", {}).get("collection") == "app.bsky.feed.post"
    ):
        langs = event.get("commit", {}).get("record", {}).get("langs", [])
        if "en" in langs:
            return True
    return False


def offsite_source(record: dict) -> tuple[str, str] | tuple[None, None]:
    for facet in record.get("facets", []):
        for feature in facet.get("features", []):
            offsite_uri = feature.get("uri")
            if offsite_uri:
                for offsite in OFFSITE_TO_STREAM:
                    if offsite in offsite_uri:
                        return (offsite, offsite_uri)
    return (None, None)


def offsite_english_posts(event: dict) -> tuple[str, S2SinkRecord] | None:
    if is_english_post(event):
        (offsite, offsite_uri) = offsite_source(
            event.get("commit", {}).get("record", {})
        )
        if offsite is not None:
            data = {
                "did": event.get("did"),
                "rkey": event.get("commit", {}).get("rkey"),
                "created_at": event.get("commit", {})
                .get("record", {})
                .get("createdAt"),
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "cid": event.get("commit", {}).get("cid"),
                "offsite": offsite,
                "offsite_uri": offsite_uri,
            }
            return (
                OFFSITE_TO_STREAM[offsite],
                S2SinkRecord(body=json.dumps(data).encode()),
            )
    return None


flow = Dataflow("s2_sink_example")
bsky_events = op.input(
    "bluesky_source",
    flow,
    JetstreamSource(wss_uri="wss://jetstream2.us-west.bsky.network/subscribe"),
)
offsite_en_posts = op.filter_map(
    "filter_offsite_english_posts", bsky_events, offsite_english_posts
)
op.output(
    "s2_sink",
    offsite_en_posts,
    S2Sink(
        config=S2Config(auth_token=AUTH_TOKEN),
        basin=BASIN,
        stream_prefix="",
        partition_fn=S2SinkPartitionFn.DIRECT,
    ),
)
