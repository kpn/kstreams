import asyncio
import inspect
import logging
from typing import Any, Coroutine, Dict, List, Optional, Type, Union

from .backends.kafka import Kafka
from .clients import ConsumerType, ProducerType
from .custom_types import DecoratedCallable, Headers
from .exceptions import DuplicateStreamException
from .prometheus.monitor import PrometheusMonitor
from .prometheus.tasks import metrics_task
from .serializers import ValueDeserializer, ValueSerializer
from .singlenton import Singleton
from .streams import Stream
from .utils import encode_headers

logger = logging.getLogger(__name__)


class StreamEngine(metaclass=Singleton):
    def __init__(
        self,
        *,
        backend: Kafka,
        consumer_class: Type[ConsumerType],
        producer_class: Type[ProducerType],
        monitor: PrometheusMonitor,
        title: Optional[str] = None,
        value_deserializer: Optional[ValueDeserializer] = None,
        value_serializer: Optional[ValueSerializer] = None,
    ) -> None:
        self.title = title
        self.backend = backend
        self.consumer_class = consumer_class
        self.producer_class = producer_class
        self.value_deserializer = value_deserializer
        self.value_serializer = value_serializer
        self.monitor = monitor
        self._producer: Optional[Type[ProducerType]] = None
        self._streams: List[Stream] = []
        self.metrics_task: Optional[asyncio.Task] = None

    async def send(
        self,
        topic: str,
        value: Any = None,
        key: Any = None,
        partition: Optional[str] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Headers] = None,
        value_serializer: Optional[ValueSerializer] = None,
        value_serializer_kwargs: Optional[Dict] = None,
    ):
        value_serializer = value_serializer or self.value_serializer

        # serialize only when value and value_serializer are present
        if value is not None and value_serializer is not None:
            value = await value_serializer.serialize(
                value, headers=headers, value_serializer_kwargs=value_serializer_kwargs
            )

        if headers is not None:
            headers = encode_headers(headers)

        fut = await self._producer.send(
            topic,
            value=value,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
        )
        metadata = await fut
        self.monitor.add_topic_partition_offset(
            topic, metadata.partition, metadata.offset
        )

        return metadata

    async def start(self) -> None:
        await self.start_producer()
        await self.start_streams()

        # add the producer and streams to the Monitor
        self.monitor.add_producer(self._producer)
        self.monitor.add_streams(self._streams)
        self._start_metrics_task()

    async def stop(self) -> None:
        self._stop_metrics_task()
        await self.stop_streams()
        await self.stop_producer()

    async def stop_producer(self):
        logger.info("Waiting Producer to STOP....")
        if self._producer is not None:
            await self._producer.stop()

    async def start_producer(self, **kwargs) -> None:
        if self.producer_class is None:
            return None
        config = {**self.backend.dict(), **kwargs}
        self._producer = self.producer_class(**config)
        if self._producer is None:
            return None
        await self._producer.start()

    async def start_streams(self) -> None:
        # Only start the Streams that are not async_generators
        streams = [
            stream
            for stream in self._streams
            if not inspect.isasyncgenfunction(stream.func)
        ]
        for stream in streams:
            await stream.start()

    def _start_metrics_task(self):
        self.metrics_task = asyncio.create_task(
            metrics_task(self._streams, self.monitor)
        )

    def _stop_metrics_task(self):
        if self.metrics_task is not None:
            self.metrics_task.cancel()

    async def stop_streams(self) -> None:
        logger.info("Waiting for Streams to STOP....")
        for stream in self._streams:
            await stream.stop()

    async def clean_streams(self):
        await self.stop_streams()
        self._streams = []

    def exist_stream(self, name: str) -> bool:
        stream = self.get_stream(name)
        return True if stream is not None else False

    def get_stream(self, name: str) -> Optional[Stream]:
        stream = next((stream for stream in self._streams if stream.name == name), None)

        return stream

    def add_stream(self, stream: Stream) -> None:
        if self.exist_stream(stream.name):
            raise DuplicateStreamException(name=stream.name)
        self._streams.append(stream)

    def _create_stream(
        self,
        topics: Union[List[str], str],
        *,
        func: Coroutine[Stream, Any, Any],
        name: Optional[str] = None,
        value_deserializer: Optional[ValueDeserializer] = None,
        **kwargs,
    ) -> Stream:
        """
        Create a Stream processor and add it to the stream List.
        This method should not be used by the end user
        """
        if name is not None and self.exist_stream(name):
            raise DuplicateStreamException(name=name)

        stream = Stream(
            topics=topics,
            func=func,
            name=name,
            config=kwargs,
            consumer_class=self.consumer_class,
            value_deserializer=value_deserializer or self.value_deserializer,
            backend=self.backend,
        )
        self._streams.append(stream)
        return stream

    def stream(
        self,
        topics: Union[List[str], str],
        *,
        name: Optional[str] = None,
        value_deserializer: Optional[ValueDeserializer] = None,
        **kwargs,
    ) -> DecoratedCallable:
        def decorator(func: Coroutine[Stream, Any, Any]) -> Stream:
            stream = self._create_stream(
                topics,
                func=func,
                name=name,
                value_deserializer=value_deserializer,
                **kwargs,
            )
            return stream

        return decorator
