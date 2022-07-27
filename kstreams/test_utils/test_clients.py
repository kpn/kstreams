from datetime import datetime
from typing import Any, Coroutine, Dict, List, Optional, Tuple, Union

from kstreams.clients import Consumer, Producer
from kstreams.custom_types import Headers
from kstreams.serializers import ValueSerializer

from .structs import ConsumerRecord, RecordMetadata, TopicPartition
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
        partition: int = 1,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Headers] = None,
        value_serializer: Optional[ValueSerializer] = None,
        value_serializer_kwargs: Optional[Dict] = None,
    ) -> Coroutine:
        topic = TopicManager.get_or_create_topic(topic_name)
        timestamp_ms = timestamp_ms or datetime.now().timestamp()
        total_messages = topic.total_messages + 1

        consumer_record = ConsumerRecord(
            topic=topic_name,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp=timestamp_ms,
            offset=total_messages,
        )

        await topic.put(consumer_record)

        async def fut():
            return RecordMetadata(
                topic=topic_name,
                partition=1,
                timestamp=timestamp_ms,
                offset=total_messages,
            )

        return fut()


class TestConsumer(Base, Consumer):
    def __init__(self, *topics: str, group_id: Optional[str] = None, **kwargs) -> None:
        # copy the aiokafka behavior
        self.topics: Tuple[str] = topics
        self._group_id: str = group_id
        self._assigments: List[TopicPartition] = []

        for topic_name in topics:
            TopicManager.create_topic(topic_name, consumer=self)
            self._assigments.append(TopicPartition(topic=topic_name, partition=1))

        # Called to make sure that has all the kafka attributes like _coordinator
        # so it will behave like an real Kafka Consumer
        super().__init__()

    def assignment(self) -> List[TopicPartition]:
        return self._assigments

    def last_stable_offset(self, topic_partition: TopicPartition) -> int:
        topic = TopicManager.get_topic(topic_partition.topic)
        return topic.total_messages

    async def position(self, topic_partition: TopicPartition) -> int:
        return self.last_stable_offset(topic_partition)

    def highwater(self, topic_partition: TopicPartition) -> int:
        return self.last_stable_offset(topic_partition)

    async def getone(
        self,
    ) -> Union[bytes, Dict]:  # The return type must be fixed later on
        for topic_partition in self._assigments:
            topic = TopicManager.get_topic(topic_partition.topic)
            if topic is None:
                raise AttributeError("There should be a topic")

            if not topic.consumed:
                break

        return await topic.get()
