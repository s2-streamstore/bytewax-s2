import json
import random
import time
from base64 import b64decode, b64encode
from typing import Callable, Generator
from urllib.parse import quote

from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    HTTPError,
    RequestException,
)
from requests.models import Response
from sseclient import SSEClient

from bytewax_s2._types import S2SinkRecord, S2SourceRecord

_RETRYABLE_HTTP_STATUS_CODES = (500, 503, 504)


def _compute_backoffs(
    attempts: int,
    wait_min: float = 0.1,
    wait_max: float = 5.0,
) -> list[float]:
    backoffs = []
    for attempt in range(attempts):
        backoffs.append(random.uniform(wait_min, min(wait_max, 2**attempt)))
    return backoffs


def _should_retry_on(e: RequestException) -> bool:
    if any(
        (
            isinstance(e, ConnectionError),
            isinstance(e, ChunkedEncodingError),
            isinstance(e, HTTPError)
            and e.response.status_code in _RETRYABLE_HTTP_STATUS_CODES,
        )
    ):
        return True
    else:
        return False


class _Retrier:
    __slots__ = ("_should_retry_on", "_max_attempts")

    def __init__(
        self,
        should_retry_on: Callable[[RequestException], bool],
        max_attempts: int,
    ):
        self._should_retry_on = should_retry_on
        self._max_attempts = max_attempts

    def __call__(self, f: Callable, *args, **kwargs):
        backoffs = _compute_backoffs(attempts=self._max_attempts)
        attempt = 0
        while True:
            try:
                return f(*args, **kwargs)
            except RequestException as e:
                if attempt < self._max_attempts and self._should_retry_on(e):
                    time.sleep(backoffs[attempt])
                    attempt += 1
                else:
                    raise e


class _TimeoutHTTPAdapter(HTTPAdapter):
    __slots__ = "_timeout"

    def __init__(
        self, connection_timeout: float, request_timeout: float, *args, **kwargs
    ):
        self._timeout = (connection_timeout, request_timeout)
        super().__init__(*args, **kwargs)

    def send(self, request, timeout=None, **kwargs):
        return super().send(request, timeout=self._timeout, **kwargs)


class S2:
    __slots__ = (
        "_base_url",
        "_session",
        "_retrier",
        "_max_retries",
    )

    def __init__(
        self,
        basin: str,
        auth_token: str,
        connection_timeout: float,
        request_timeout: float,
        max_retries: int,
    ):
        self._base_url = f"https://{basin}.b.aws.s2.dev/v1alpha/streams"
        self._session = Session()
        self._session.mount(
            "https://", _TimeoutHTTPAdapter(connection_timeout, request_timeout)
        )
        self._session.headers.update(
            {
                "Authorization": f"Bearer {auth_token}",
            }
        )
        self._retrier = _Retrier(
            should_retry_on=_should_retry_on, max_attempts=max_retries
        )
        self._max_retries = max_retries

    def list_streams(self, stream_prefix: str) -> list[str]:
        start_after = ""
        streams = []
        while True:
            params = {
                "prefix": stream_prefix,
                "start_after": start_after,
            }
            response = self._retrier(
                self._session.get, self._base_url, params=params
            ).json()
            for stream in response["streams"]:
                if stream["deleted_at"] is None:
                    streams.append(stream["name"])
            if response["has_more"] is True and len(streams) > 0:
                start_after = streams[-1]
            else:
                break
        return streams

    def check_head(self, stream: str) -> int:
        url = f"{self._base_url}/{_quoted(stream)}/records"
        params = {"start_seq_num": 0}
        response = self._retrier(self._session.get, url, params=params).json()
        if "first_seq_num" in response.keys():
            return response["first_seq_num"]
        elif "batch" in response.keys():
            return 0
        else:
            raise RuntimeError(f"Unable to determine head of the stream: {stream}")

    def check_tail(self, stream: str) -> int:
        url = f"{self._base_url}/{_quoted(stream)}/records/tail"
        response = self._retrier(self._session.get, url).json()
        return response["next_seq_num"]

    def append(self, stream: str, records: list[S2SinkRecord]) -> None:
        url = f"{self._base_url}/{_quoted(stream)}/records"
        body = {
            "records": [
                {
                    "body": b64encode(record.body).decode(),
                    "headers": [
                        (b64encode(name).decode(), b64encode(value).decode())
                        for (name, value) in record.headers
                    ],
                }
                for record in records
            ]
        }
        headers = {"s2-format": "json-binsafe"}
        self._retrier(self._session.post, url, json=body, headers=headers)

    def read(
        self, stream: str, start_seq_num: int
    ) -> Generator[list[S2SourceRecord], None, None]:
        url = f"{self._base_url}/{_quoted(stream)}/records"
        params = {"start_seq_num": start_seq_num}
        headers = {
            "Accept": "text/event-stream",
            "s2-format": "json-binsafe",
        }
        max_attempts = self._max_retries
        backoffs = _compute_backoffs(max_attempts)
        attempt = 0
        while True:
            try:
                response = self._session.get(
                    url, params=params, headers=headers, stream=True
                )
                sse = SSEClient(_event_source(response))
                for event in sse.events():
                    if attempt > 0:
                        attempt = 0
                    data = json.loads(event.data)
                    records = [
                        S2SourceRecord(
                            body=b64decode(record["body"]),
                            headers=[
                                (b64decode(name), b64decode(value))
                                for (name, value) in record["headers"]
                            ],
                            seq_num=record["seq_num"],
                        )
                        for record in data["batch"]["records"]
                    ]
                    if len(records) > 0:
                        params["start_seq_num"] = records[-1].seq_num + 1
                    yield records
            except RequestException as e:
                if attempt < max_attempts and _should_retry_on(e):
                    time.sleep(backoffs[attempt])
                else:
                    raise e


def _event_source(response: Response) -> Generator[bytes, None, None]:
    for data in response:
        yield data


def _quoted(stream: str) -> str:
    return quote(stream, safe="")
