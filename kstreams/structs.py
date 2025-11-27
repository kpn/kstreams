from typing import NamedTuple


class TopicPartitionOffset(NamedTuple):
    topic: str
    partition: int
    offset: int
