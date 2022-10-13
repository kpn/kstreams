from datetime import datetime
from typing import Any, Coroutine, Dict, List, Optional, Set, Tuple

from aiokafka.structs import ConsumerRecord

from kstreams.clients import Consumer, Producer
from kstreams.serializers import Serializer
from kstreams.types import Headers

from .structs import RecordMetadata, TopicPartition
from .topics import TopicManager


class Base:
    async def start(self):
        ...


class TestProducer(Base, Producer):
    async def send(
        self,
        topic_name: str,
        value: Any = None,
        key: Any = None,
        partition: int = 0,
        timestamp_ms: Optional[float] = None,
        headers: Optional[Headers] = None,
        serializer: Optional[Serializer] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> Coroutine:
        topic = TopicManager.get_or_create(topic_name)
        timestamp_ms = timestamp_ms or datetime.now().timestamp()
        total_partition_events = (
            topic.get_total_partition_events(partition=partition) + 1
        )

        consumer_record = ConsumerRecord(
            topic=topic_name,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp=timestamp_ms,
            offset=total_partition_events,
            timestamp_type=None,
            checksum=None,
            serialized_key_size=None,
            serialized_value_size=None,
        )

        await topic.put(consumer_record)

        async def fut():
            return RecordMetadata(
                topic=topic_name,
                partition=partition,
                timestamp=timestamp_ms,
                offset=total_partition_events,
            )

        return fut()


class TestConsumer(Base, Consumer):
    def __init__(self, *topics: str, group_id: Optional[str] = None, **kwargs) -> None:
        # copy the aiokafka behavior
        self.topics: Tuple[str, ...] = topics
        self._group_id: Optional[str] = group_id
        self._assignment: List[TopicPartition] = []
        self.partitions_committed: Dict[TopicPartition, int] = {}

        for topic_name in topics:
            TopicManager.get_or_create(topic_name, consumer=self)
            self._assignment.append(TopicPartition(topic=topic_name, partition=0))

        # Called to make sure that has all the kafka attributes like _coordinator
        # so it will behave like an real Kafka Consumer
        super().__init__()

    def assignment(self) -> List[TopicPartition]:
        return self._assignment

    def _check_partition_assignments(self, consumer_record: ConsumerRecord) -> None:
        """
        When an event is consumed the partition can be any positive int number
        because there is not limit in the producer side (only during testing of course).
        In case that the partition is not in the `_assignment` we need to register it.

        This is only during testing as in real use cases the assignments happens
        at the moment of kafka bootstrapping
        """
        topic_partition = TopicPartition(
            topic=consumer_record.topic,
            partition=consumer_record.partition,
        )

        if topic_partition not in self._assignment:
            self._assignment.append(topic_partition)

    def last_stable_offset(self, topic_partition: TopicPartition) -> int:
        topic = TopicManager.get(topic_partition.topic)

        if topic is not None:
            return topic.get_total_partition_events(partition=topic_partition.partition)
        return -1

    async def position(self, topic_partition: TopicPartition) -> int:
        return self.last_stable_offset(topic_partition)

    def highwater(self, topic_partition: TopicPartition) -> int:
        return self.last_stable_offset(topic_partition)

    async def commit(self, offsets: Optional[Dict[TopicPartition, int]] = None) -> None:
        if offsets is not None:
            for topic_partition, offset in offsets.items():
                self.partitions_committed[topic_partition] = offset
        return None

    async def committed(self, topic_partition: TopicPartition) -> Optional[int]:
        return self.partitions_committed.get(topic_partition)

    async def end_offsets(
        self, partitions: List[TopicPartition]
    ) -> Dict[TopicPartition, int]:
        topic = TopicManager.get(partitions[0].topic)
        end_offsets = {
            topic_partition: topic.get_total_partition_events(
                partition=topic_partition.partition
            )
            for topic_partition in partitions
        }
        return end_offsets

    def partitions_for_topic(self, topic: str) -> Set:
        """
        Return the partitions of all assigned topics. The `topic` argument is not used
        because in a testing enviroment the only topics are the ones declared by the end
        user.

        The AIOKafkaConsumer returns a Set, so we do the same.
        """
        partitions = [topic_partition.partition for topic_partition in self._assignment]
        return set(partitions)

    async def getone(
        self,
    ) -> Optional[ConsumerRecord]:  # The return type must be fixed later on
        topic = None
        for topic_partition in self._assignment:
            topic = TopicManager.get(topic_partition.topic)

            if not topic.consumed:
                break

        if topic is not None:
            consumer_record = await topic.get()
            self._check_partition_assignments(consumer_record)
            return consumer_record
        return None
