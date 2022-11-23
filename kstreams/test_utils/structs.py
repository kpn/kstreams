from typing import NamedTuple


class RecordMetadata(NamedTuple):
    offset: int
    partition: int
    topic: str
    timestamp: int
