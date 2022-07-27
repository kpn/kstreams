from dataclasses import dataclass
from typing import Generic, NamedTuple, Optional, TypeVar

from kstreams.custom_types import KafkaHeaders

KT = TypeVar("KT")
VT = TypeVar("VT")


class TopicPartition(NamedTuple):
    topic: str
    partition: int


class RecordMetadata(NamedTuple):
    offset: int
    partition: int
    topic: str
    timestamp: int


@dataclass
class ConsumerRecord(Generic[KT, VT]):
    topic: str
    partition: int
    offset: int
    timestamp: int
    key: Optional[KT]
    value: Optional[VT]
    headers: KafkaHeaders
