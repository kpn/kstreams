from typing import NamedTuple


class TopicPartition(NamedTuple):
    topic: str
    partition: int


class RecordMetadata(NamedTuple):
    offset: int
    partition: int
    topic: str
    timestamp: int
