import asyncio
import inspect
import logging
import typing

from aiokafka.structs import RecordMetadata

from kstreams.structs import TopicPartitionOffset

from .backends.kafka import Kafka
from .clients import Consumer, Producer
from .exceptions import DuplicateStreamException, EngineNotStartedException
from .middleware import Middleware
from .middleware.udf_middleware import UdfHandler
from .prometheus.monitor import PrometheusMonitor
from .rebalance_listener import MetricsRebalanceListener, RebalanceListener
from .serializers import Deserializer, Serializer
from .streams import Stream, StreamFunc
from .streams import stream as stream_func
from .streams_utils import StreamErrorPolicy, UDFType
from .types import Deprecated, EngineHooks, Headers, NextMiddlewareCall
from .utils import encode_headers, execute_hooks

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
        async def consume(stream: kstreams.ConsumerRecord) -> None:
            print(f"showing bytes: {cr.value}")


        await stream_engine.start()
        ```
    """

    def __init__(
        self,
        *,
        backend: Kafka,
        consumer_class: typing.Type[Consumer],
        producer_class: typing.Type[Producer],
        monitor: PrometheusMonitor,
        title: typing.Optional[str] = None,
        deserializer: Deprecated[typing.Optional[Deserializer]] = None,
        serializer: typing.Optional[Serializer] = None,
        on_startup: typing.Optional[EngineHooks] = None,
        on_stop: typing.Optional[EngineHooks] = None,
        after_startup: typing.Optional[EngineHooks] = None,
        after_stop: typing.Optional[EngineHooks] = None,
    ) -> None:
        self.title = title
        self.backend = backend
        self.consumer_class = consumer_class
        self.producer_class = producer_class
        self.deserializer = deserializer
        self.serializer = serializer
        self.monitor = monitor
        self._producer: typing.Optional[typing.Type[Producer]] = None
        self._streams: typing.List[Stream] = []
        self._on_startup = [] if on_startup is None else list(on_startup)
        self._on_stop = [] if on_stop is None else list(on_stop)
        self._after_startup = [] if after_startup is None else list(after_startup)
        self._after_stop = [] if after_stop is None else list(after_stop)

    async def send(
        self,
        topic: str,
        value: typing.Any = None,
        key: typing.Any = None,
        partition: typing.Optional[int] = None,
        timestamp_ms: typing.Optional[int] = None,
        headers: typing.Optional[Headers] = None,
        serializer: typing.Optional[Serializer] = None,
        serializer_kwargs: typing.Optional[typing.Dict] = None,
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
        # Execute on_startup hooks
        await execute_hooks(self._on_startup)

        # add the producer and streams to the Monitor
        self.monitor.add_producer(self._producer)
        self.monitor.add_streams(self._streams)

        await self.start_producer()
        await self.start_streams()

        # Execute after_startup hooks
        await execute_hooks(self._after_startup)

    def on_startup(
        self,
        func: typing.Callable[[], typing.Any],
    ) -> typing.Callable[[], typing.Any]:
        """
        A list of callables to run before the engine starts.
        Handler are callables that do not take any arguments, and may be either
        standard functions, or async functions.

        Attributes:
            func typing.Callable[[], typing.Any]: Func to callable before engine starts

        !!! Example
            ```python title="Engine before startup"

            import kstreams

            stream_engine = kstreams.create_engine(
                title="my-stream-engine"
            )

            @stream_engine.on_startup
            async def init_db() -> None:
                print("Initializing Database Connections")
                await init_db()


            @stream_engine.on_startup
            async def start_background_task() -> None:
                print("Some background task")
            ```
        """
        self._on_startup.append(func)
        return func

    def on_stop(
        self,
        func: typing.Callable[[], typing.Any],
    ) -> typing.Callable[[], typing.Any]:
        """
        A list of callables to run before the engine stops.
        Handler are callables that do not take any arguments, and may be either
        standard functions, or async functions.

        Attributes:
            func typing.Callable[[], typing.Any]: Func to callable before engine stops

        !!! Example
            ```python title="Engine before stops"

            import kstreams

            stream_engine = kstreams.create_engine(
                title="my-stream-engine"
            )

            @stream_engine.on_stop
            async def close_db() -> None:
                print("Closing Database Connections")
                await db_close()
            ```
        """
        self._on_stop.append(func)
        return func

    def after_startup(
        self,
        func: typing.Callable[[], typing.Any],
    ) -> typing.Callable[[], typing.Any]:
        """
        A list of callables to run after the engine starts.
        Handler are callables that do not take any arguments, and may be either
        standard functions, or async functions.

        Attributes:
            func typing.Callable[[], typing.Any]: Func to callable after engine starts

        !!! Example
            ```python title="Engine after startup"

            import kstreams

            stream_engine = kstreams.create_engine(
                title="my-stream-engine"
            )

            @stream_engine.after_startup
            async def after_startup() -> None:
                print("Set pod as healthy")
                await mark_healthy_pod()
            ```
        """
        self._after_startup.append(func)
        return func

    def after_stop(
        self,
        func: typing.Callable[[], typing.Any],
    ) -> typing.Callable[[], typing.Any]:
        """
        A list of callables to run after the engine stops.
        Handler are callables that do not take any arguments, and may be either
        standard functions, or async functions.

        Attributes:
            func typing.Callable[[], typing.Any]: Func to callable after engine stops

        !!! Example
            ```python title="Engine after stops"

            import kstreams

            stream_engine = kstreams.create_engine(
                title="my-stream-engine"
            )

            @stream_engine.after_stop
            async def after_stop() -> None:
                print("Finishing backgrpund tasks")
            ```
        """
        self._after_stop.append(func)
        return func

    async def stop(self) -> None:
        # Execute on_startup hooks
        await execute_hooks(self._on_stop)

        await self.monitor.stop()
        await self.stop_producer()
        await self.stop_streams()

        # Execute after_startup hooks
        await execute_hooks(self._after_stop)

    async def stop_producer(self):
        if self._producer is not None:
            await self._producer.stop()
        logger.info("Producer has STOPPED....")

    async def start_producer(self, **kwargs) -> None:
        if self.producer_class is None:
            return None
        config = {**self.backend.model_dump(), **kwargs}
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

        await self._start_streams_on_background_mode(streams)

    async def _start_streams_on_background_mode(
        self, streams: typing.List[Stream]
    ) -> None:
        # start all the streams
        for stream in streams:
            asyncio.create_task(stream.start())

        # start monitoring
        asyncio.create_task(self.monitor.start())

    async def stop_streams(self) -> None:
        for stream in self._streams:
            await stream.stop()
        logger.info("Streams have STOPPED....")

    async def clean_streams(self):
        await self.stop_streams()
        self._streams = []

    def exist_stream(self, name: str) -> bool:
        stream = self.get_stream(name)
        return True if stream is not None else False

    def get_stream(self, name: str) -> typing.Optional[Stream]:
        stream = next((stream for stream in self._streams if stream.name == name), None)

        return stream

    def add_stream(
        self, stream: Stream, error_policy: typing.Optional[StreamErrorPolicy] = None
    ) -> None:
        """
        Add a stream to the engine.

        This method registers a new stream with the engine, setting up necessary
        configurations and handlers. If a stream with the same name already exists,
        a DuplicateStreamException is raised.

        Args:
            stream: The stream to be added.
            error_policy: An optional error policy to be applied to the stream.
                You should probably set directly when instanciating a Stream, not here.

        Raises:
            DuplicateStreamException: If a stream with the same name already exists.

        Notes:
            - If the stream does not have a deserializer, the engine's deserializer
              is assigned to it.
            - If the stream does not have a rebalance listener, a default
              MetricsRebalanceListener is assigned.
            - The stream's UDF handler is set up with the provided function and
              engine's send method.
            - If the stream's UDF handler type is not NO_TYPING, a middleware stack
              is built for the stream's function.
        """
        if self.exist_stream(stream.name):
            raise DuplicateStreamException(name=stream.name)

        if error_policy is not None:
            stream.error_policy = error_policy

        stream.backend = self.backend
        if stream.deserializer is None:
            stream.deserializer = self.deserializer
        self._streams.append(stream)

        if stream.rebalance_listener is None:
            # set the stream to the listener to it will be available
            # when the callbacks are called
            stream.rebalance_listener = MetricsRebalanceListener()

        stream.rebalance_listener.stream = stream
        stream.rebalance_listener.engine = self

        stream.udf_handler = UdfHandler(
            next_call=stream.func,
            send=self.send,
            stream=stream,
        )

        # NOTE: When `no typing` support is deprecated this check can
        # be removed
        if stream.udf_handler.type != UDFType.NO_TYPING:
            stream.func = self._build_stream_middleware_stack(stream=stream)

    def _build_stream_middleware_stack(self, *, stream: Stream) -> NextMiddlewareCall:
        assert stream.udf_handler, "UdfHandler can not be None"

        middlewares = stream.get_middlewares(self)
        next_call = stream.udf_handler
        for middleware, options in reversed(middlewares):
            next_call = middleware(
                next_call=next_call, send=self.send, stream=stream, **options
            )
        return next_call

    async def remove_stream(self, stream: Stream) -> None:
        consumer = stream.consumer
        self._streams.remove(stream)
        await stream.stop()

        if consumer is not None:
            self.monitor.clean_stream_consumer_metrics(consumer=consumer)

    def stream(
        self,
        topics: typing.Union[typing.List[str], str],
        *,
        name: typing.Optional[str] = None,
        deserializer: Deprecated[typing.Optional[Deserializer]] = None,
        initial_offsets: typing.Optional[typing.List[TopicPartitionOffset]] = None,
        rebalance_listener: typing.Optional[RebalanceListener] = None,
        middlewares: typing.Optional[typing.List[Middleware]] = None,
        subscribe_by_pattern: bool = False,
        error_policy: StreamErrorPolicy = StreamErrorPolicy.STOP,
        **kwargs,
    ) -> typing.Callable[[StreamFunc], Stream]:
        def decorator(func: StreamFunc) -> Stream:
            stream_from_func = stream_func(
                topics,
                name=name,
                deserializer=deserializer,
                initial_offsets=initial_offsets,
                rebalance_listener=rebalance_listener,
                middlewares=middlewares,
                subscribe_by_pattern=subscribe_by_pattern,
                **kwargs,
            )(func)
            self.add_stream(stream_from_func, error_policy=error_policy)

            return stream_from_func

        return decorator
