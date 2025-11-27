from dataclasses import dataclass, field
from typing import Any, NamedTuple, Optional

from aiokafka.producer.message_accumulator import BatchBuilder
from aiokafka.structs import RecordMetadata

from .clients import Producer
from .types import EncodedHeaders, Headers


class BatchEvent(NamedTuple):
    value: Any = None
    key: Any = None
    timestamp_ms: Optional[int] = None
    headers: Optional[Headers] = None


@dataclass
class BatchAggregator:
    topic: str
    producer: Producer
    partition: Optional[int] = None
    all_partitions: list[int] = field(default_factory=list)
    available_partitions: list[int] = field(default_factory=list)
    batches: dict[int, BatchBuilder] = field(default_factory=dict)

    def __post_init__(self):
        if self.partition is None:
            self.all_partitions: list[int] = list(
                self.producer._metadata.partitions_for_topic(self.topic)
            )
            self.available_partitions: list[int] = list(
                self.producer._metadata.available_partitions_for_topic(self.topic)
            )
        else:
            self.batches[self.partition] = self.producer.create_batch()

    def append(
        self,
        *,
        key: Any,
        value: Any,
        timestamp: Optional[int] = None,
        headers: Optional[EncodedHeaders] = None,
    ):
        if self.partition is not None:
            # Use the default batch for the specified partition
            batch = self.batches[self.partition]
        else:
            encoded_key = self.producer._key_serializer(key)
            partition = self.producer._partitioner(
                encoded_key, self.all_partitions, self.available_partitions
            )

            if partition in self.batches:
                batch = self.batches[partition]
            else:
                batch = self.producer.create_batch()
                self.batches[partition] = batch

        batch.append(key=key, value=value, timestamp=timestamp, headers=headers)

    async def flush(self) -> list[RecordMetadata]:
        result = []
        for partition, batch in self.batches.items():
            fut = await self.producer.send_batch(batch, self.topic, partition=partition)
            metadata: RecordMetadata = await fut
            result.append(metadata)

        return result
