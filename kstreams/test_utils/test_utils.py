from types import TracebackType
from typing import Any, Dict, List, Optional, Type

from kstreams import Consumer, Producer
from kstreams.engine import StreamEngine
from kstreams.prometheus.monitor import PrometheusMonitor
from kstreams.serializers import Serializer
from kstreams.streams import Stream
from kstreams.types import ConsumerRecord, Headers

from .structs import RecordMetadata
from .test_clients import TestConsumer, TestProducer
from .topics import Topic, TopicManager


class TestMonitor(PrometheusMonitor):
    __test__ = False

    async def start(self, *args, **kwargs) -> None: ...

    async def stop(self, *args, **kwargs) -> None: ...

    def add_topic_partition_offset(self, *args, **kwargs) -> None: ...

    def clean_stream_consumer_metrics(self, *args, **kwargs) -> None: ...

    def add_producer(self, *args, **kwargs): ...

    def add_streams(self, *args, **kwargs): ...


class TestStreamClient:
    __test__ = False

    def __init__(
        self,
        stream_engine: StreamEngine,
        monitoring_enabled: bool = True,
        topics: Optional[List[str]] = None,
        test_producer_class: Type[Producer] = TestProducer,
        test_consumer_class: Type[Consumer] = TestConsumer,
    ) -> None:
        self.stream_engine = stream_engine
        self.test_producer_class = test_producer_class
        self.test_consumer_class = test_consumer_class

        # Extra topics' names defined by the end user which must be created
        # before the cycle test starts
        self.extra_user_topics = topics

        # store the user clients to restore them later
        self.monitor = stream_engine.monitor
        self.engine_producer_class = self.stream_engine.producer_class
        self.engine_consumer_class = self.stream_engine.consumer_class

        self.stream_engine.producer_class = self.test_producer_class
        self.stream_engine.consumer_class = self.test_consumer_class

        if not monitoring_enabled:
            self.stream_engine.monitor = TestMonitor()

        self.create_extra_topics()

    def mock_streams(self) -> None:
        streams: List[Stream] = self.stream_engine._streams
        for stream in streams:
            stream.consumer_class = self.test_consumer_class
            # subscribe before entering the test to make sure that the
            # consumer is created
            stream.subscribe()

    def setup_mocks(self) -> None:
        self.mock_streams()

    def create_extra_topics(self) -> None:
        if self.extra_user_topics is not None:
            for topic_name in self.extra_user_topics:
                TopicManager.create(topic_name)

    async def start(self) -> None:
        self.setup_mocks()
        await self.stream_engine.start()

    async def stop(self) -> None:
        # If there are streams, we must wait until all the messages are consumed
        if self.stream_engine._streams:
            await TopicManager.join()
        await self.stream_engine.stop()

        # restore original config
        self.stream_engine.producer_class = self.engine_producer_class
        self.stream_engine.consumer_class = self.engine_consumer_class
        self.stream_engine.monitor = self.monitor

        # clean the topics after finishing the test to make sure that
        # no data is left tover
        TopicManager.clean()

    async def __aenter__(self) -> "TestStreamClient":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_t: Optional[Type[BaseException]],
        exc_v: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.stop()

    async def send(
        self,
        topic: str,
        value: Any = None,
        key: Optional[Any] = None,
        partition: int = 0,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Headers] = None,
        serializer: Optional[Serializer] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> RecordMetadata:
        return await self.stream_engine.send(
            topic,
            value=value,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            serializer=serializer,
            serializer_kwargs=serializer_kwargs,
        )

    def get_topic(self, *, topic_name: str) -> Topic:
        return TopicManager.get(topic_name)

    async def get_event(self, *, topic_name: str) -> ConsumerRecord:
        topic = TopicManager.get(topic_name)
        return await topic.get()
