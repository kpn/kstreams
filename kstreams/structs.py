import typing
from typing import NamedTuple

from pydantic import BaseModel, NonNegativeInt, PositiveInt

from kstreams import TopicPartition


class TopicPartitionOffset(NamedTuple):
    topic: str
    partition: int
    offset: int


class GetMany(BaseModel):
    max_records: PositiveInt = 1
    timeout_ms: NonNegativeInt = 0
    partitions: typing.Optional[typing.List[TopicPartition]] = None
