import threading
from queue import Empty, Queue

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition

from bytewax_s2._s2 import S2
from bytewax_s2._types import S2Config, S2SinkRecord, S2SourceRecord


def _s2_stream_reader(
    s2: S2,
    stream: str,
    start_seq_num: int,
    next_batch_queue: Queue[list[S2SourceRecord] | Exception],
):
    try:
        for batch in s2.read(stream, start_seq_num):
            next_batch_queue.put(batch)
    except Exception as exc:
        next_batch_queue.put(exc)


class _S2StreamReader:
    __slots__ = (
        "_next_batch_queue",
        "_reader_thread",
    )

    def __init__(self, s2: S2, stream: str, start_seq_num: int):
        self._next_batch_queue: Queue[list[S2SourceRecord] | Exception] = Queue()
        self._reader_thread = threading.Thread(
            target=_s2_stream_reader,
            args=(s2, stream, start_seq_num, self._next_batch_queue),
            daemon=True,
        )
        self._reader_thread.start()

    def next_batch(self) -> list[S2SourceRecord]:
        try:
            batch_or_exc = self._next_batch_queue.get_nowait()
            if isinstance(batch_or_exc, Exception):
                raise batch_or_exc
            return batch_or_exc
        except Empty:
            return []


class _S2SourcePartition(StatefulSourcePartition):
    __slots__ = (
        "_reader",
        "_tail",
        "_s2",
        "_stream",
    )

    def __init__(
        self,
        s2: S2,
        stream: str,
        next_seq_num: int | None,
        tail: bool,
    ):
        if next_seq_num is None:
            self._next_seq_num = s2.check_head(stream)
        else:
            self._next_seq_num = next_seq_num
        self._reader = _S2StreamReader(s2, stream, self._next_seq_num)
        self._tail = tail
        self._s2 = s2
        self._stream = stream

    def next_batch(self) -> list[S2SourceRecord]:
        batch = self._reader.next_batch()
        if not self._tail and len(batch) == 0:
            next_seq_num = self._s2.check_tail(self._stream)
            if self._next_seq_num == next_seq_num:
                raise StopIteration()
        if len(batch) > 0:
            self._next_seq_num = batch[-1].seq_num + 1
        return batch

    def snapshot(self) -> int:
        return self._next_seq_num


class S2Source(FixedPartitionedSource):
    """
    Use a set of [S2](https://s2.dev/docs/concepts) streams as an input source.

    Each stream is considered as a partition.

    Each item emitted downstream in the dataflow will be of type `S2SourceRecord`.

    Supports exactly-once processing.
    """

    __slots__ = (
        "_s2",
        "_partitions",
        "_tail",
    )

    def __init__(
        self, config: S2Config, basin: str, stream_prefix: str, tail: bool = True
    ):
        """
        Args:
            config(S2Config): Configuration for S2 client.
            basin(str): Name of the basin.
            stream_prefix(str): Matching prefix for the set of streams that needs to be considered
                as partitions.
            tail(bool): Whether to tail the streams or not. Default value is True.
        """
        self._s2 = S2(
            basin,
            config.auth_token,
            config.connection_timeout,
            config.request_timeout,
            config.max_retries,
        )
        self._partitions = self._s2.list_streams(stream_prefix)
        self._tail = tail

    def list_parts(self) -> list[str]:
        return self._partitions

    def build_part(
        self, _step_id: str, for_part: str, resume_state: int | None
    ) -> _S2SourcePartition:
        return _S2SourcePartition(
            s2=self._s2, stream=for_part, next_seq_num=resume_state, tail=self._tail
        )


class _S2SinkPartition(StatefulSinkPartition):
    __slots__ = (
        "_s2",
        "_stream",
    )

    def __init__(self, s2: S2, stream: str):
        self._s2 = s2
        self._stream = stream

    def write_batch(self, items: list[S2SinkRecord]):
        self._s2.append(stream=self._stream, records=items)

    def snapshot(self):
        pass


class S2Sink(FixedPartitionedSink):
    """
    Use a set of [S2](https://s2.dev/docs/concepts) streams as an output sink.

    Each stream is considered as a partition.

    Each item from upstream in the dataflow must be a `(key, value)` pair, where `key` must be a `str`
    that can be hashed for identifying the parition into which the value should get sinked. `value`
    must be of type `S2SinkRecord`.

    Supports at-least-once processing.
    """

    __slots__ = (
        "_s2",
        "_partitions",
    )

    def __init__(self, config: S2Config, basin: str, stream_prefix: str):
        """
        Args:
            config(S2Config): Configuration for S2 client.
            basin(str): Name of the basin.
            stream_prefix(str): Matching prefix for the set of streams that needs to be considered
                as partitions.
        """
        self._s2 = S2(
            basin,
            config.auth_token,
            config.connection_timeout,
            config.request_timeout,
            config.max_retries,
        )
        self._partitions = self._s2.list_streams(stream_prefix)

    def list_parts(self) -> list[str]:
        return self._partitions

    def build_part(self, _step_id, for_part, _resume_state):
        return _S2SinkPartition(s2=self._s2, stream=for_part)
