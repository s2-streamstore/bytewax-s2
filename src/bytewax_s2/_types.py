from dataclasses import dataclass, field
from enum import Enum


@dataclass(slots=True, frozen=True)
class S2Config:
    """
    Configuration for S2 client.

    Args:
        auth_token(str): Authentication token generated from [S2 dashboard](https://s2.dev/dashboard).
        connection_timeout(float): Timeout value in seconds for establishing connection to S2 service.
            Default value is 5 seconds.
        request_timeout(float): Timeout value in seconds for requests made to S2 service.
            Default value is 5 seconds.
        max_retries(int): Maximum number of retries when requests fail. Default value is 3.
    """

    auth_token: str
    connection_timeout: float = 5.0
    request_timeout: float = 5.0
    max_retries: int = 3


@dataclass(slots=True, frozen=True)
class S2SourceRecord:
    """
    Record read from an [S2](https://s2.dev/docs/concepts) stream.

    Attributes:
        body(bytes): Body of this record.
        headers(list[tuple[bytes, bytes]]): Series of name-value pairs for this record.
        seq_num(int): Sequence number for this record.
    """

    body: bytes
    headers: list[tuple[bytes, bytes]]
    seq_num: int


@dataclass(slots=True, frozen=True)
class S2SinkRecord:
    """
    Record to append to an [S2](https://s2.dev/docs/concepts) stream.

    Args:
        body(bytes): Body of this record.
        headers(list[tuple[bytes, bytes]]): Series of name-value pairs for this record, which can
            serve as metadata for this record. Default value is empty list.
    """

    body: bytes
    headers: list[tuple[bytes, bytes]] = field(default_factory=list)


class S2SinkPartitionFn(Enum):
    HASHED = 1
    DIRECT = 2
