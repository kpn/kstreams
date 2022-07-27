import asyncio
from types import TracebackType
from typing import Any, Dict, List, Optional, Type

from kstreams.create import create_engine
from kstreams.custom_types import Headers
from kstreams.serializers import ValueSerializer
from kstreams.streams import Stream

from .structs import RecordMetadata
from .test_clients import TestConsumer, TestProducer
from .topics import TopicManager


class TestStreamClient:
    def __init__(self) -> None:
        self.stream_engine = create_engine()

        # store the user clients to restore them later
        self.producer_class = self.stream_engine.producer_class
        self.consumer_class = self.stream_engine.consumer_class

        self.stream_engine.producer_class = TestProducer
        self.stream_engine.consumer_class = TestConsumer

    def mock_streams(self) -> None:
        streams: List[Stream] = self.stream_engine._streams
        for stream in streams:
            stream.consumer_class = TestConsumer

    def mock_producer(self) -> None:
        producer = TestProducer()
        self.stream_engine._producer = producer

    def setup_mocks(self) -> None:
        self.mock_producer()
        self.mock_streams()

    async def __aenter__(self) -> "TestStreamClient":
        self.setup_mocks()
        await self.stream_engine.start()
        self.stream_engine._stop_metrics_task()
        return self

    async def __aexit__(
        self,
        exc_t: Optional[Type[BaseException]],
        exc_v: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        # If there are streams, we must wait until all the messages are consumed
        if self.stream_engine._streams:
            while not TopicManager.all_messages_consumed():
                await asyncio.sleep(1)

        await self.stream_engine.stop()

        # restore original config
        self.stream_engine.producer_class = self.producer_class
        self.stream_engine.consumer_class = self.consumer_class

    async def send(
        self,
        topic: str,
        value: Optional[Dict] = None,
        key: Optional[Any] = None,
        partition: Optional[str] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Headers] = None,
        value_serializer: Optional[ValueSerializer] = None,
        value_serializer_kwargs: Optional[Dict] = None,
    ) -> RecordMetadata:
        return await self.stream_engine.send(
            topic,
            value=value,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            value_serializer=value_serializer,
            value_serializer_kwargs=value_serializer_kwargs,
        )
