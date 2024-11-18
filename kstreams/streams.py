import asyncio
import inspect
import logging
import typing
import uuid
import warnings
from functools import update_wrapper

from aiokafka import errors

from kstreams import TopicPartition
from kstreams.exceptions import BackendNotSet
from kstreams.middleware.middleware import ExceptionMiddleware
from kstreams.structs import TopicPartitionOffset

from .backends.kafka import Kafka
from .clients import Consumer
from .middleware import Middleware, udf_middleware
from .rebalance_listener import RebalanceListener
from .serializers import Deserializer
from .streams_utils import StreamErrorPolicy, UDFType
from .types import ConsumerRecord, Deprecated, StreamFunc

if typing.TYPE_CHECKING:
    from kstreams import StreamEngine

logger = logging.getLogger(__name__)


class Stream:
    """
    Attributes:
        name Optional[str]: Stream name. Default is a generated uuid4
        topics List[str]: List of topics to consume
        subscribe_by_pattern bool: Whether subscribe to topics by pattern
        backend kstreams.backends.Kafka: backend kstreams.backends.kafka.Kafka:
            Backend to connect. Default `Kafka`
        func Callable[["Stream"], Awaitable[Any]]: Coroutine fucntion or generator
            to be called when an event arrives
        config Dict[str, Any]: Stream configuration. Here all the
         [properties](https://aiokafka.readthedocs.io/en/stable/api.html#consumer-class)
            can be passed in the dictionary
        deserializer kstreams.serializers.Deserializer: Deserializer to be used
            when an event is consumed
        initial_offsets List[kstreams.TopicPartitionOffset]: List of
            TopicPartitionOffset that will `seek` the initial offsets to
        rebalance_listener kstreams.rebalance_listener.RebalanceListener: Listener
            callbacks when partition are assigned or revoked

    ## Subscribe to a topic

    !!! Example
        ```python
        import aiorun
        from kstreams import create_engine, ConsumerRecord

        stream_engine = create_engine(title="my-stream-engine")


        @stream_engine.stream("local--kstreams", group_id="my-group-id")
        async def stream(cr: ConsumerRecord) -> None:
            print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")


        async def start():
            await stream_engine.start()


        async def shutdown(loop):
            await stream_engine.stop()


        if __name__ == "__main__":
            aiorun.run(
                start(),
                stop_on_unhandled_errors=True,
                shutdown_callback=shutdown
            )
        ```

    ## Subscribe to multiple topics

    Consuming from multiple topics using one `stream` is possible. A `List[str]`
    of topics must be provided.

    !!! Example
        ```python
        import aiorun
        from kstreams import create_engine, ConsumerRecord

        stream_engine = create_engine(title="my-stream-engine")


        @stream_engine.stream(
            ["local--kstreams", "local--hello-world"],
            group_id="my-group-id",
        )
        async def consume(cr: ConsumerRecord) -> None:
            print(f"Event from {cr.topic}: headers: {cr.headers}, payload: {cr.value}")
        ```

    ## Subscribe to topics by pattern

    In the following example the stream will subscribe to any topic that matches
    the regex `^dev--customer-.*`, for example `dev--customer-invoice` or
    `dev--customer-profile`. The `subscribe_by_pattern` flag must be set to `True`.

    !!! Example
        ```python
        import aiorun
        from kstreams import create_engine, ConsumerRecord

        stream_engine = create_engine(title="my-stream-engine")


        @stream_engine.stream(
            topics="^dev--customer-.*$",
            subscribe_by_pattern=True,
            group_id="my-group-id",
        )
        async def stream(cr: ConsumerRecord) -> None:
            if cr.topic == "dev--customer-invoice":
                print("Event from topic dev--customer-invoice"
            elif cr.topic == "dev--customer-profile":
                print("Event from topic dev--customer-profile"
            else:
                raise ValueError(f"Invalid topic {cr.topic}")


        async def start():
            await stream_engine.start()

        async def shutdown(loop):
            await stream_engine.stop()


        if __name__ == "__main__":
            aiorun.run(
                start(),
                stop_on_unhandled_errors=True,
                shutdown_callback=shutdown
            )
        ```
    """

    def __init__(
        self,
        topics: typing.Union[typing.List[str], str],
        *,
        subscribe_by_pattern: bool = False,
        func: StreamFunc,
        backend: typing.Optional[Kafka] = None,
        consumer_class: typing.Type[Consumer] = Consumer,
        name: typing.Optional[str] = None,
        config: typing.Optional[typing.Dict] = None,
        deserializer: Deprecated[typing.Optional[Deserializer]] = None,
        initial_offsets: typing.Optional[typing.List[TopicPartitionOffset]] = None,
        rebalance_listener: typing.Optional[RebalanceListener] = None,
        middlewares: typing.Optional[typing.List[Middleware]] = None,
        error_policy: StreamErrorPolicy = StreamErrorPolicy.STOP,
    ) -> None:
        self.func = func
        self.backend = backend
        self.consumer_class = consumer_class
        self.consumer: typing.Optional[Consumer] = None
        self.config = config or {}
        self.name = name or str(uuid.uuid4())
        self.deserializer = deserializer
        self.running = False
        self.is_processing = asyncio.Lock()
        self.initial_offsets = initial_offsets
        self.seeked_initial_offsets = False
        self.rebalance_listener = rebalance_listener
        self.middlewares = middlewares or []
        self.udf_handler: typing.Optional[udf_middleware.UdfHandler] = None
        self.topics = [topics] if isinstance(topics, str) else topics
        self.subscribe_by_pattern = subscribe_by_pattern
        self.error_policy = error_policy

    def _create_consumer(self) -> Consumer:
        if self.backend is None:
            raise BackendNotSet("A backend has not been set for this stream")
        config = {**self.backend.model_dump(), **self.config}
        return self.consumer_class(**config)

    def get_middlewares(self, engine: "StreamEngine") -> typing.Sequence[Middleware]:
        """
        Retrieve the list of middlewares for the stream engine.

        Use this instead of the `middlewares` attribute to get the list of middlewares.

        Args:
            engine: The stream engine instance.

        Returns:
            A sequence of Middleware instances.
            Including the ExceptionMiddleware with the specified error policy and any
            additional middlewares.
        """
        return [
            Middleware(
                ExceptionMiddleware, engine=engine, error_policy=self.error_policy
            )
        ] + self.middlewares

    async def stop(self) -> None:
        if self.running:
            # Don't run anymore to prevent new events comming
            self.running = False

            async with self.is_processing:
                # Only enter this block when all the events have been
                # proccessed in the middleware chain
                if self.consumer is not None:
                    await self.consumer.stop()

                    # we have to do this operations because aiokafka bug
                    # https://github.com/aio-libs/aiokafka/issues/1010
                    self.consumer.unsubscribe()
                    self.consumer = None

                logger.info(
                    f"Stream consuming from topics {self.topics} has stopped!!! \n\n"
                )

    def subscribe(self) -> None:
        """
        Create Consumer and subscribe to topics

        Subsciptions uses cases:

        Case 1:
            self.topics Topics is a List, which means that the end user wants to
            subscribe to multiple topics explicitly:

                Stream(topics=["local--hello-kpn", "local--hello-kpn-2", ...], ...)

        Case 2:
            self.topics is a string which represents a single topic (explicit)
            to subscribe:

                Stream(topics="local--hello-kpn", ...)

            It is also possible to use the `subscribe_by_pattern` but in practice
            it does not have any difference because the pattern will match
            explicitly the topic name:

                Stream(topics="local--hello-kpn", subscribe_by_pattern=True, ...)

        Case 3:
            self.topics is a pattern, then we subscribe to N number
            of topics. The flag `self.subscribe_by_pattern` must be True

                Stream(topics="^dev--customer-.*$", subscribe_by_pattern=True, ...)

        It is important to notice that in `aiokafka` both `topics` and `pattern`
        can not be used at the same time, so when calling self.consumer.subscribe(...)
        we set only one of them according to the flag `self.subscribe_by_pattern`.
        """

        if self.consumer is None:
            # Only create a consumer if it was not previously created
            self.consumer = self._create_consumer()

        self.consumer.subscribe(
            topics=self.topics if not self.subscribe_by_pattern else None,
            listener=self.rebalance_listener,
            pattern=self.topics[0] if self.subscribe_by_pattern else None,
        )

    async def commit(
        self, offsets: typing.Optional[typing.Dict[TopicPartition, int]] = None
    ):
        await self.consumer.commit(offsets=offsets)  # type: ignore

    async def getone(self) -> ConsumerRecord:
        consumer_record: ConsumerRecord = await self.consumer.getone()  # type: ignore

        # call deserializer if there is one regarless consumer_record.value
        # as the end user might want to do something extra with headers or metadata
        if self.deserializer is not None:
            msg = (
                "Deserializers are deprecated and don't have any support.\n"
                "Use middlewares instead:\nhttps://kpn.github.io/kstreams/middleware/"
            )
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return await self.deserializer.deserialize(consumer_record)

        return consumer_record

    async def getmany(
        self,
        partitions: typing.Optional[typing.List[TopicPartition]] = None,
        timeout_ms: int = 0,
        max_records: typing.Optional[int] = None,
    ) -> typing.Dict[TopicPartition, typing.List[ConsumerRecord]]:
        """
        Get a batch of events from the assigned TopicPartition.

        Prefetched events are returned in batches by topic-partition.
        If messages is not available in the prefetched buffer this method waits
        `timeout_ms` milliseconds.

        Attributes:
            partitions List[TopicPartition] | None: The partitions that need
                fetching message. If no one partition specified then all
                subscribed partitions will be used
            timeout_ms int | None: milliseconds spent waiting if
                data is not available in the buffer. If 0, returns immediately
                with any records that are available currently in the buffer,
                else returns empty. Must not be negative.
            max_records int | None: The amount of records to fetch.
                if `timeout_ms` was defined and reached and the fetched records
                has not reach `max_records` then returns immediately
                with any records that are available currently in the buffer

        Returns:
            Topic to list of records

        !!! Example
            ```python
            @stream_engine.stream(topic, ...)
            async def stream(stream: Stream):
                while True:
                    data = await stream.getmany(max_records=5)
                    print(data)
            ```
        """
        partitions = partitions or []
        return await self.consumer.getmany(  # type: ignore
            *partitions, timeout_ms=timeout_ms, max_records=max_records
        )

    async def start(self) -> None:
        if self.running:
            return None

        self.subscribe()

        if self.consumer is not None:
            await self.consumer.start()
            self.running = True

            if self.udf_handler is not None:
                if self.udf_handler.type == UDFType.NO_TYPING:
                    # deprecated use case
                    msg = (
                        "Streams with `async for in` loop approach are deprecated.\n"
                        "Migrate to typed function.\n"
                        "Read more:\n\n"
                        "\thttps://kpn.github.io/kstreams/stream/#dependency-injection"
                    )
                    warnings.warn(msg, DeprecationWarning, stacklevel=2)

                    func = self.udf_handler.next_call(self)  # type: ignore
                    await func
                else:
                    # Typing cases
                    if not inspect.isasyncgenfunction(self.udf_handler.next_call):
                        # Is not an async_generator, then create `await` the func
                        await self.func_wrapper_with_typing()

        return None

    async def func_wrapper_with_typing(self) -> None:
        while self.running:
            try:
                cr = await self.getone()
                async with self.is_processing:
                    await self.func(cr)
            except errors.ConsumerStoppedError:
                # This exception is only raised when we are inside the `getone`
                # coroutine waiting for an event and `await consumer.stop()`
                # is called. If this happens it means that the end users has called
                # `engine.stop()` or has been another exception that causes all the
                # streams to stop. In any case the exception should not be re raised.
                logger.info(
                    f"Stream {self} stopped after Coordinator was closed {self.topics}"
                )

    def seek_to_initial_offsets(self) -> None:
        if not self.seeked_initial_offsets and self.consumer is not None:
            assignments: typing.Set[TopicPartition] = self.consumer.assignment()
            if self.initial_offsets is not None:
                topicPartitionOffset: TopicPartitionOffset
                for topicPartitionOffset in self.initial_offsets:
                    tp = TopicPartition(
                        topic=topicPartitionOffset.topic,
                        partition=topicPartitionOffset.partition,
                    )
                    if tp in assignments:
                        self.consumer.seek(
                            partition=tp, offset=topicPartitionOffset.offset
                        )
                    else:
                        logger.warning(
                            "You are attempting to seek on an TopicPartitionOffset "
                            "that isn't in the consumer assignments. "
                            "The code will simply ignore the seek request "
                            f"on this partition. {tp} is not in the partition "
                            f"assignment. The partition assignment is {assignments}."
                        )
            self.seeked_initial_offsets = True

    async def __aenter__(self) -> "Stream":
        """
        Start the kafka Consumer and return an `async_gen` so it can be iterated

        !!! Example
            ```python title="Usage"
            @stream_engine.stream(topic, group_id=group_id, ...)
            async def stream(stream):
                yield cr.value


            # Iterate the stream:
            async with stream as stream_flow:
                async for value in stream_flow:
                    ...
            ```
        """
        logger.info("Starting async_gen Stream....")
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        logger.info("Stopping async_gen Stream....")
        await self.stop()

    def __aiter__(self):
        return self

    async def __anext__(self) -> ConsumerRecord:
        try:
            cr = await self.getone()

            if (
                self.udf_handler is not None
                and self.udf_handler.type == UDFType.NO_TYPING
            ):
                return cr
            return await self.func(cr)
        except errors.ConsumerStoppedError:
            raise StopAsyncIteration  # noqa: F821

    async def __call__(self, *args, **kwargs) -> typing.Any:
        """Make stream work as the wrapped func to keep behavior.

        This allows for testing the stream as a normal function.
        """
        return self.func(*args, **kwargs)


def stream(
    topics: typing.Union[typing.List[str], str],
    *,
    subscribe_by_pattern: bool = False,
    name: typing.Optional[str] = None,
    deserializer: Deprecated[typing.Optional[Deserializer]] = None,
    initial_offsets: typing.Optional[typing.List[TopicPartitionOffset]] = None,
    rebalance_listener: typing.Optional[RebalanceListener] = None,
    middlewares: typing.Optional[typing.List[Middleware]] = None,
    **kwargs,
) -> typing.Callable[[StreamFunc], Stream]:
    def decorator(func: StreamFunc) -> Stream:
        s = Stream(
            topics=topics,
            func=func,
            name=name,
            deserializer=deserializer,
            initial_offsets=initial_offsets,
            rebalance_listener=rebalance_listener,
            middlewares=middlewares,
            subscribe_by_pattern=subscribe_by_pattern,
            config=kwargs,
        )
        update_wrapper(s, func)
        return s

    return decorator
