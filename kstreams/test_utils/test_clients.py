from datetime import datetime
from typing import Any, Coroutine, Dict, List, Optional, Sequence, Set, Union

from kstreams import RebalanceListener, TopicPartition
from kstreams.clients import Consumer, Producer
from kstreams.serializers import Serializer
from kstreams.types import ConsumerRecord, EncodedHeaders

from .structs import RecordMetadata
from .topics import TopicManager


class Base:
    async def start(self): ...


class TestProducer(Base, Producer):
    __test__ = False

    async def send(
        self,
        topic_name: str,
        value: Any = None,
        key: Any = None,
        partition: int = 0,
        timestamp_ms: Optional[int] = None,
        headers: Optional[EncodedHeaders] = None,
        serializer: Optional[Serializer] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> Coroutine:
        topic, _ = TopicManager.get_or_create(topic_name)
        timestamp_ms = timestamp_ms or datetime.now().toordinal()
        total_partition_events = topic.offset(partition=partition)
        partition = partition or 0

        serialized_key_size = -1 if key is None else len(key)
        serialized_value_size = -1 if value is None else len(value)
        consumer_record: ConsumerRecord = ConsumerRecord(
            topic=topic_name,
            value=value,
            key=key,
            headers=headers or [],
            partition=partition,
            timestamp=timestamp_ms,
            offset=total_partition_events + 1,
            timestamp_type=0,
            checksum=None,
            serialized_key_size=-serialized_key_size,
            serialized_value_size=serialized_value_size,
        )

        await topic.put(consumer_record)

        async def fut():
            return RecordMetadata(
                topic=topic_name,
                partition=partition,
                timestamp=timestamp_ms,
                offset=total_partition_events + 1,
            )

        return fut()


class TestConsumer(Base, Consumer):
    __test__ = False

    def __init__(self, group_id: Optional[str] = None, **kwargs) -> None:
        # copy the aiokafka behavior
        self.topics: Optional[Sequence[str]] = None
        self._group_id: Optional[str] = group_id
        self._assignment: List[TopicPartition] = []
        self.partitions_committed: Dict[TopicPartition, int] = {}

        # Called to make sure that has all the kafka attributes like _coordinator
        # so it will behave like an real Kafka Consumer
        super().__init__()

    def subscribe(
        self,
        *,
        topics: Optional[Sequence[str]] = None,
        listener: RebalanceListener,
        pattern: Optional[str] = None,
    ) -> None:
        self.topics = topics
        if topics is None:
            # then it is a pattern subscription, we need to get the current
            # topics (pre created) from the topic manager
            assert pattern
            topics = TopicManager.get_topics_by_pattern(pattern=pattern)

        for topic_name in topics:
            topic, created = TopicManager.get_or_create(topic_name, consumer=self)

            if not created:
                # It means that the topic already exist, so we are in
                # the situation where the topic hs events and the Stream
                # was added on runtime
                topic.consumer = self

            for partition_number in range(0, 3):
                self._assignment.append(
                    TopicPartition(topic=topic_name, partition=partition_number)
                )

        if listener.stream is not None:
            listener.stream.seek_to_initial_offsets()

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
            return topic.offset(partition=topic_partition.partition)
        return -1

    async def position(self, topic_partition: TopicPartition) -> int:
        """
        Get the offset of the *next record* that will be fetched,
        so it returns offset(topic_partition) + 1
        """
        return self.last_stable_offset(topic_partition) + 1

    def highwater(self, topic_partition: TopicPartition) -> int:
        """
        A highwater offset is the offset that will be assigned to
        the *next message* that is produced, so it returns
        offset(topic_partition) + 1
        """
        return self.last_stable_offset(topic_partition) + 1

    async def commit(self, offsets: Optional[Dict[TopicPartition, int]] = None) -> None:
        if offsets is not None:
            for topic_partition, offset in offsets.items():
                self.partitions_committed[topic_partition] = offset
        return None

    async def committed(self, topic_partition: TopicPartition) -> Optional[int]:
        return self.partitions_committed.get(topic_partition, 0)

    async def end_offsets(
        self, partitions: List[TopicPartition]
    ) -> Dict[TopicPartition, int]:
        topic = TopicManager.get(partitions[0].topic)
        end_offsets = {
            topic_partition: topic.offset(partition=topic_partition.partition) + 1
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
            try:
                consumer_record = await topic.get()
                self._check_partition_assignments(consumer_record)
                return consumer_record
            except RuntimeError:
                # Scenario when the event loop was closed in the end user test
                # and the current coroutine gets cancelled in the `getone`
                ...

        return None

    async def getmany(
        self,
        *partitions: List[TopicPartition],
        timeout_ms: int = 0,
        max_records: int = 1,
    ) -> Dict[TopicPartition, List[Union[ConsumerRecord, None]]]:
        """
        Basic getmany implementation.
        `partitions` and `timeout_ms` could be added to the logic
        but it seems unnecessary for now; if end users request them we
        can add it
        """
        return {
            self._assignment[0]: [await self.getone() for _ in range(0, max_records)]
        }

    def seek(self, *, partition: TopicPartition, offset: int) -> None:
        # This method intends to have the same signature as aiokafka but with kwargs
        # rather than positional arguments
        topics = self.topics or ()

        if partition.topic in topics:
            topic = TopicManager.get(name=partition.topic)
            partition_offset = topic.offset(partition=partition.partition)

            # only consume if the offset to seek if <= the parition total events
            if offset <= partition_offset:
                consumed_events = 0

                # keep consuming if the events to consume <= offset to seek
                while consumed_events < offset:
                    event = topic.get_nowait()
                    topic.task_done()

                    if event.partition == partition.partition:
                        # only decrease if the event.partition matches
                        # the partition that the user wants to seek
                        consumed_events += 1
                    else:
                        # ideally each partition should be a Queue
                        # for now just add the same event to the queue
                        topic.put_nowait(event=event)

                    # it means that this consumer can consume
                    # from the TopicPartition so we can add it
                    # to the _assignment
                    if partition not in self._assignment:
                        self._assignment.append(partition)
