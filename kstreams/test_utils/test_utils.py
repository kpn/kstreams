from types import TracebackType
from typing import Any, Dict, List, Optional, Type

from kstreams import ConsumerRecord
from kstreams.engine import StreamEngine
from kstreams.prometheus.monitor import PrometheusMonitor
from kstreams.serializers import Serializer
from kstreams.streams import Stream
from kstreams.types import Headers

from .structs import RecordMetadata
from .test_clients import TestConsumer, TestProducer
from .topics import Topic, TopicManager


class TestMonitor(PrometheusMonitor):
    __test__ = False

    def start(self, *args, **kwargs) -> None:
        print("herte....")
        # ...

    async def stop(self, *args, **kwargs) -> None:
        ...

    def add_topic_partition_offset(self, *args, **kwargs) -> None:
        ...

    def clean_stream_consumer_metrics(self, *args, **kwargs) -> None:
        ...

    def add_producer(self, *args, **kwargs):
        ...

    def add_streams(self, *args, **kwargs):
        ...


class TestStreamClient:
    __test__ = False

    def __init__(
        self, stream_engine: StreamEngine, monitoring_enabled: bool = True
    ) -> None:
        self.stream_engine = stream_engine

        # store the user clients to restore them later
        self.monitor = stream_engine.monitor
        self.producer_class = self.stream_engine.producer_class
        self.consumer_class = self.stream_engine.consumer_class

        self.stream_engine.producer_class = TestProducer
        self.stream_engine.consumer_class = TestConsumer

        if not monitoring_enabled:
            self.stream_engine.monitor = TestMonitor()

    def mock_streams(self) -> None:
        streams: List[Stream] = self.stream_engine._streams
        for stream in streams:
            stream.consumer_class = TestConsumer

    def setup_mocks(self) -> None:
        self.mock_streams()

    async def start(self) -> None:
        self.setup_mocks()
        await self.stream_engine.start()

    async def stop(self) -> None:
        # If there are streams, we must wait until all the messages are consumed
        if self.stream_engine._streams:
            await TopicManager.join()
        await self.stream_engine.stop()

        # restore original config
        self.stream_engine.producer_class = self.producer_class
        self.stream_engine.consumer_class = self.consumer_class
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
