from typing import Any, NamedTuple, Optional

from .types import Headers


class TopicPartitionOffset(NamedTuple):
    topic: str
    partition: int
    offset: int


class BatchEvent(NamedTuple):
    value: Any = None
    key: Any = None
    timestamp_ms: Optional[int] = None
    headers: Optional[Headers] = None
