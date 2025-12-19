import typing
from typing import NamedTuple

from pydantic import BaseModel, NonNegativeInt, PositiveInt

from kstreams import TopicPartition


class TopicPartitionOffset(NamedTuple):
    topic: str
    partition: int
    offset: int


class GetMany(BaseModel):
    """
    Get many multiple records at once based on the provided configuration.

    Attributes:
        max_records (PositiveInt): Maximum number of records to fetch
            (default: 1)
        timeout_ms (NonNegativeInt): Maximum time to wait in milliseconds
            (default: 0)
        partitions (List[TopicPartition] | None): List of topic partitions to fetch from
            If `None`, fetch from all assigned partitions.
    """

    max_records: PositiveInt = 1
    timeout_ms: NonNegativeInt = 0
    partitions: typing.Optional[typing.List[TopicPartition]] = None
