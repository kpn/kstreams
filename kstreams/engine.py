import inspect
import logging
from typing import Any, Callable, Dict, List, Optional, Type, Union

from aiokafka.structs import RecordMetadata

from kstreams.structs import TopicPartitionOffset

from .backends.kafka import Kafka
from .clients import ConsumerType, ProducerType
from .exceptions import DuplicateStreamException, EngineNotStartedException
from .prometheus.monitor import PrometheusMonitor
from .rebalance_listener import MetricsRebalanceListener, RebalanceListener
from .serializers import Deserializer, Serializer
from .streams import Stream, StreamFunc, stream
from .types import Headers
from .utils import encode_headers

logger = logging.getLogger(__name__)


class StreamEngine:
    """
    Attributes:
        backend kstreams.backends.Kafka: Backend to connect. Default `Kafka`
        consumer_class kstreams.Consumer: The consumer class to use when
            instanciate a consumer. Default kstreams.Consumer
        producer_class kstreams.Producer: The producer class to use when
            instanciate the producer. Default kstreams.Producer
        monitor kstreams.PrometheusMonitor: Prometheus monitor that holds
            the [metrics](https://kpn.github.io/kstreams/metrics/)
        title str | None: Engine name
        serializer kstreams.serializers.Serializer | None: Serializer to
            use when an event is produced.
        deserializer kstreams.serializers.Deserializer | None: Deserializer
            to be used when an event is consumed.
            If provided it will be used in all Streams instances as a general one.
            To override it per Stream, you can provide one per Stream

    !!! Example
        ```python title="Usage"
        import kstreams

        stream_engine = kstreams.create_engine(
            title="my-stream-engine"
        )

        @kstreams.stream("local--hello-world", group_id="example-group")
        async def consume(stream: kstreams.Stream) -> None:
            async for cr in stream:
                print(f"showing bytes: {cr.value}")


        await stream_engine.start()
        ```
    """

    def __init__(
        self,
        *,
        backend: Kafka,
        consumer_class: Type[ConsumerType],
        producer_class: Type[ProducerType],
        monitor: PrometheusMonitor,
        title: Optional[str] = None,
        deserializer: Optional[Deserializer] = None,
        serializer: Optional[Serializer] = None,
    ) -> None:
        self.title = title
        self.backend = backend
        self.consumer_class = consumer_class
        self.producer_class = producer_class
        self.deserializer = deserializer
        self.serializer = serializer
        self.monitor = monitor
        self._producer: Optional[Type[ProducerType]] = None
        self._streams: List[Stream] = []

    async def send(
        self,
        topic: str,
        value: Any = None,
        key: Any = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Headers] = None,
        serializer: Optional[Serializer] = None,
        serializer_kwargs: Optional[Dict] = None,
    ):
        """
        Attributes:
            topic str: Topic name to send the event to
            value Any: Event value
            key str | None: Event key
            partition int | None: Topic partition
            timestamp_ms int | None: Event timestamp in miliseconds
            headers Dict[str, str] | None: Event headers
            serializer kstreams.serializers.Serializer | None: Serializer to
                encode the event
            serializer_kwargs Dict[str, Any] | None: Serializer kwargs
        """
        if self._producer is None:
            raise EngineNotStartedException()

        serializer = serializer or self.serializer

        # serialize only when value and serializer are present
        if value is not None and serializer is not None:
            value = await serializer.serialize(
                value, headers=headers, serializer_kwargs=serializer_kwargs
            )

        encoded_headers = None
        if headers is not None:
            encoded_headers = encode_headers(headers)

        fut = await self._producer.send(
            topic,
            value=value,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=encoded_headers,
        )
        metadata: RecordMetadata = await fut
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
        self.monitor.start()

    async def stop(self) -> None:
        await self.stop_streams()
        await self.stop_producer()
        self.monitor.stop()

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
        stream.backend = self.backend
        if stream.deserializer is None:
            stream.deserializer = self.deserializer
        self._streams.append(stream)

        if stream.rebalance_listener is None:
            # set the stream to the listener to it will be available
            # when the callbacks are called
            stream.rebalance_listener = MetricsRebalanceListener()

        stream.rebalance_listener.stream = stream  # type: ignore
        stream.rebalance_listener.engine = self  # type: ignore

    async def remove_stream(self, stream: Stream) -> None:
        await stream.stop()
        self._streams.remove(stream)

    def stream(
        self,
        topics: Union[List[str], str],
        *,
        name: Optional[str] = None,
        deserializer: Optional[Deserializer] = None,
        initial_offsets: Optional[List[TopicPartitionOffset]] = None,
        rebalance_listener: Optional[RebalanceListener] = None,
        **kwargs,
    ) -> Callable[[StreamFunc], Stream]:
        def decorator(func: StreamFunc) -> Stream:
            stream_from_func = stream(
                topics,
                name=name,
                deserializer=deserializer,
                initial_offsets=initial_offsets,
                rebalance_listener=rebalance_listener,
                **kwargs,
            )(func)
            self.add_stream(stream_from_func)

            return stream_from_func

        return decorator
