import asyncio
import inspect
import logging
import sys
import typing
import uuid
from functools import update_wrapper

from aiokafka import errors

from kstreams import ConsumerRecord, TopicPartition
from kstreams.exceptions import BackendNotSet
from kstreams.structs import TopicPartitionOffset

from .backends.kafka import Kafka
from .clients import Consumer
from .middleware import Middleware
from .rebalance_listener import RebalanceListener
from .serializers import Deserializer
from .streams_utils import UDFType, inspect_udf
from .types import StreamFunc

logger = logging.getLogger(__name__)


if sys.version_info < (3, 10):

    async def anext(async_gen: typing.AsyncGenerator):
        return await async_gen.__anext__()


class Stream:
    """
    Attributes:
        name str: Stream name
        topics List[str]: List of topics to consume
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

    !!! Example
        ```python title="Usage"
        import aiorun
        from kstreams import create_engine, ConsumerRecord

        stream_engine = create_engine(title="my-stream-engine")


        # here you can add any other AIOKafkaConsumer config
        @stream_engine.stream("local--kstreams", group_id="de-my-partition")
        async def stream(cr: ConsumerRecord) -> None:
            print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")


        async def start():
            await stream_engine.start()
            await produce()


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
        func: StreamFunc,
        backend: typing.Optional[Kafka] = None,
        consumer_class: typing.Type[Consumer] = Consumer,
        name: typing.Optional[str] = None,
        config: typing.Optional[typing.Dict] = None,
        deserializer: typing.Optional[Deserializer] = None,
        initial_offsets: typing.Optional[typing.List[TopicPartitionOffset]] = None,
        rebalance_listener: typing.Optional[RebalanceListener] = None,
        middlewares: typing.Optional[typing.List[Middleware]] = None,
    ) -> None:
        self.func = func
        self.backend = backend
        self.consumer_class = consumer_class
        self.consumer: typing.Optional[Consumer] = None
        self.config = config or {}
        self._consumer_task: typing.Optional[asyncio.Task] = None
        self.name = name or str(uuid.uuid4())
        self.deserializer = deserializer
        self.running = False
        self.is_processing = asyncio.Lock()
        self.initial_offsets = initial_offsets
        self.seeked_initial_offsets = False
        self.rebalance_listener = rebalance_listener
        self.middlewares = middlewares or []
        self.udf_handler = UdfHandler(handler=func, stream=self)

        # aiokafka expects topic names as arguments, meaning that
        # can receive N topics -> N arguments,
        # so we always create a list and then we expand it with *topics
        self.topics = [topics] if isinstance(topics, str) else topics

    def _create_consumer(self) -> Consumer:
        if self.backend is None:
            raise BackendNotSet("A backend has not been set for this stream")
        config = {**self.backend.model_dump(), **self.config}
        return self.consumer_class(**config)

    async def stop(self) -> None:
        if self.running:
            # Don't run anymore to prevent new events comming
            self.running = False

            async with self.is_processing:
                # Only enter this block when all the events have been
                # proccessed in the middleware chain
                if self.consumer is not None:
                    await self.consumer.stop()

                if self._consumer_task is not None:
                    self._consumer_task.cancel()

    async def _subscribe(self) -> None:
        # Always create a consumer on stream.start
        self.consumer = self._create_consumer()

        # add the chech tp avoid `mypy` complains
        if self.consumer is not None:
            await self.consumer.start()
            self.consumer.subscribe(
                topics=self.topics, listener=self.rebalance_listener
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

        await self._subscribe()
        self.running = True

        if self.udf_handler.type == UDFType.NO_TYPING:
            # normal use case
            logging.warn(
                "Streams with `async for in` loop approach might be deprecated. "
                "Consider migrating to a typing approach."
            )

            func = self.udf_handler.handler(self)
            # create an asyncio.Task with func
            self._consumer_task = asyncio.create_task(self.func_wrapper(func))
        else:
            # Typing cases
            if not inspect.isasyncgenfunction(self.udf_handler.handler):
                # Is not an async_generator, then create an asyncio.Task with func
                self._consumer_task = asyncio.create_task(
                    self.func_wrapper_with_typing()
                )
        return None

    async def func_wrapper(self, func: typing.Awaitable) -> None:
        try:
            # await for the end user coroutine
            # we do this to show a better error message to the user
            # when the coroutine fails
            await func
        except Exception as e:
            logger.exception(f"CRASHED Stream!!! Task {self._consumer_task} \n\n {e}")

    async def func_wrapper_with_typing(self) -> None:
        while self.running:
            cr = await self.getone()
            async with self.is_processing:
                await self.func(cr)

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

            if self.udf_handler.type == UDFType.NO_TYPING:
                return cr
            return await self.func(cr)
        except errors.ConsumerStoppedError:
            raise StopAsyncIteration  # noqa: F821


class UdfHandler:
    def __init__(
        self,
        *,
        handler: StreamFunc,
        stream: Stream,
    ) -> None:
        self.handler = handler
        self.stream = stream
        self.type = inspect_udf(self.handler, Stream)

    def get_udf_params(self, cr: ConsumerRecord) -> typing.Tuple:
        if self.type == UDFType.CR_ONLY_TYPING:
            return (cr,)
        return (
            cr,
            self.stream,
        )

    async def __call__(self, cr: ConsumerRecord) -> typing.Any:
        """
        Call the coroutine `async def my_function(...)` defined by the end user
        in a proper way according to its parameters. The `handler` is the
        coroutine defined by the user.

        Use cases:
            1. UDFType.CR_ONLY_TYPING: Only ConsumerRecord with typing

            @stream_engine.stream(topic, name="my-stream")
                async def consume(cr: ConsumerRecord):
                    ...

            2. UDFType.ALL_TYPING: ConsumerRecord and Stream with typing.
                The order is important as they are arguments and not kwargs

            @stream_engine.stream(topic, name="my-stream")
                async def consume(cr: ConsumerRecord, stream: Stream):
                    ...

        """
        params = self.get_udf_params(cr)

        if inspect.isasyncgenfunction(self.handler):
            return await anext(self.handler(*params))
        return await self.handler(*params)


def stream(
    topics: typing.Union[typing.List[str], str],
    *,
    name: typing.Optional[str] = None,
    deserializer: typing.Optional[Deserializer] = None,
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
            config=kwargs,
        )
        update_wrapper(s, func)
        return s

    return decorator
