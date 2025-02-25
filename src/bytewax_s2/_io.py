import threading
from queue import Empty, Queue

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition

from bytewax_s2._s2 import S2
from bytewax_s2._types import S2Config, S2SinkPartitionFn, S2SinkRecord, S2SourceRecord


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

    Each item from upstream in the dataflow must be a `(key, value)` 2-tuple, where `key` must be a `str`
    and should either:
    - be hashable for identifying the partition to route the value if the chosen `partition_fn` was
        `S2SinkPartitionFn.HASHED`

    (or)

    - directly match the partition name (i.e. stream name) if the chosen `partition_fn` was
        `S2SinkPartitionFn.DIRECT`

    and `value` must be of type `S2SinkRecord`.

    Supports at-least-once processing.
    """

    __slots__ = (
        "_s2",
        "_partitions",
        "_partition_fn",
    )

    def __init__(
        self,
        config: S2Config,
        basin: str,
        stream_prefix: str,
        partition_fn: S2SinkPartitionFn = S2SinkPartitionFn.HASHED,
    ):
        """
        Args:
            config(S2Config): Configuration for S2 client.
            basin(str): Name of the basin.
            stream_prefix(str): Matching prefix for the set of streams that needs to be considered
                as partitions.
            partition_fn(S2SinkPartitionFn): Partition function kind.

                - If `S2SinkPartitionFn.DIRECT`, `key` in `(key, value)` 2-tuple passed to sink must match
                any of the partitions (i.e. stream names).
                - If `S2SinkPartitionFn.HASHED`, `key` must be a hashable `str`.

                Default value is `S2SinkPartitionFn.HASHED`.
        """
        self._s2 = S2(
            basin,
            config.auth_token,
            config.connection_timeout,
            config.request_timeout,
            config.max_retries,
        )
        self._partitions = self._s2.list_streams(stream_prefix)
        self._partition_fn = partition_fn
        match partition_fn:
            case S2SinkPartitionFn.DIRECT:
                self._partition_idx_map = {
                    part: idx for idx, part in enumerate(self._partitions)
                }
            case S2SinkPartitionFn.HASHED:
                self._partition_idx_map = {}
            case _:
                raise ValueError(f"Unexpected value for partition_fn: {partition_fn}")

    def list_parts(self) -> list[str]:
        return self._partitions

    def part_fn(self, item_key: str) -> int:
        match self._partition_fn:
            case S2SinkPartitionFn.DIRECT:
                idx = self._partition_idx_map.get(item_key)
                if idx is None:
                    raise RuntimeError(
                        f"item with key: {item_key} doesn't match any of the existing partitions: {self._partitions}"
                    )
                return idx
            case S2SinkPartitionFn.HASHED:
                return super().part_fn(item_key)
            case _:
                raise RuntimeError(
                    f"Unexpected value for partition_fn: {self._partition_fn}"
                )

    def build_part(self, _step_id, for_part, _resume_state):
        return _S2SinkPartition(s2=self._s2, stream=for_part)
