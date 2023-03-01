import asyncio
import inspect
import logging
import uuid
from functools import update_wrapper
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Type,
    Union,
)

from aiokafka import errors

from kstreams import ConsumerRecord, TopicPartition
from kstreams.exceptions import BackendNotSet
from kstreams.structs import TopicPartitionOffset

from .backends.kafka import Kafka
from .clients import Consumer, ConsumerType
from .rebalance_listener import RebalanceListener
from .serializers import Deserializer

logger = logging.getLogger(__name__)


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
        from kstreams import create_engine

        stream_engine = create_engine(title="my-stream-engine")


        # here you can add any other AIOKafkaConsumer config
        @stream_engine.stream("local--kstreams", group_id="de-my-partition")
        async def stream(stream: Stream) -> None:
            async for cr in stream:
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
        topics: Union[List[str], str],
        *,
        func: Callable[["Stream"], Awaitable[Any]],
        backend: Optional[Kafka] = None,
        consumer_class: Type[ConsumerType] = Consumer,
        name: Optional[str] = None,
        config: Optional[Dict] = None,
        model: Optional[Any] = None,
        deserializer: Optional[Deserializer] = None,
        initial_offsets: Optional[List[TopicPartitionOffset]] = None,
        rebalance_listener: Optional[RebalanceListener] = None,
    ) -> None:
        self.func = func
        self.backend = backend
        self.consumer_class = consumer_class
        self.consumer: Optional[Type[ConsumerType]] = None
        self.config = config or {}
        self._consumer_task: Optional[asyncio.Task] = None
        self.name = name or str(uuid.uuid4())
        self.model = model
        self.deserializer = deserializer
        self.running = False
        self.initial_offsets = initial_offsets
        self.rebalance_listener = rebalance_listener

        # aiokafka expects topic names as arguments, meaning that
        # can receive N topics -> N arguments,
        # so we always create a list and then we expand it with *topics
        self.topics = [topics] if isinstance(topics, str) else topics

    def _create_consumer(self) -> Type[ConsumerType]:
        if self.backend is None:
            raise BackendNotSet("A backend has not been set for this stream")
        config = {**self.backend.dict(), **self.config}
        return self.consumer_class(**config)

    async def stop(self) -> None:
        if not self.running:
            return None

        if self.consumer is not None:
            await self.consumer.stop()
            self.running = False

        if self._consumer_task is not None:
            self._consumer_task.cancel()

    async def _subscribe(self) -> None:
        # Always create a consumer on stream.start
        self.consumer = self._create_consumer()
        await self.consumer.start()
        self.running = True

        self.consumer.subscribe(topics=self.topics, listener=self.rebalance_listener)

    async def commit(self, offsets: Optional[Dict[TopicPartition, int]] = None):
        await self.consumer.commit(offsets=offsets)  # type: ignore

    async def start(self) -> Optional[AsyncGenerator]:
        if self.running:
            return None

        async def func_wrapper(func):
            try:
                # await for the end user coroutine
                # we do this to show a better error message to the user
                # when the coroutine fails
                await func
            except Exception as e:
                logger.exception(
                    f"CRASHED Stream!!! Task {self._consumer_task} \n\n {e}"
                )

        await self._subscribe()
        self._seek_to_initial_offsets()

        func = self.func(self)
        if inspect.isasyncgen(func):
            return func
        else:
            # It is not an async_generator so we need to
            # create an asyncio.Task with func
            self._consumer_task = asyncio.create_task(func_wrapper(func))
            return None

    def _seek_to_initial_offsets(self):
        assignments: Set[TopicPartition] = self.consumer.assignment()
        if self.initial_offsets is not None:
            topicPartitionOffset: TopicPartitionOffset
            for topicPartitionOffset in self.initial_offsets:
                tp = TopicPartition(
                    topic=topicPartitionOffset.topic,
                    partition=topicPartitionOffset.partition,
                )
                if tp in assignments:
                    self.consumer.seek(partition=tp, offset=topicPartitionOffset.offset)
                else:
                    logger.warning(
                        f"""You are attempting to seek on an TopicPartitionOffset that isn't in the
                    consumer assignments. The code will simply ignore the seek request
                    on this partition. {tp} is not in the partition assignment.
                    The partition assignment is {assignments}."""
                    )

    async def __aenter__(self) -> AsyncGenerator:
        """
        Start the kafka Consumer and return an `async_gen` so it can be iterated

        !!! Example
            ```python title="Usage"
            @stream_engine.stream(topic, group_id=group_id, ...)
            async def stream(consumer):
                async for cr, value, headers in consumer:
                    yield value


            # Iterate the stream:
            async with stream as stream_flow:
                async for value in stream_flow:
                    ...
            ```
        """
        logger.info("Starting async_gen Stream....")
        async_gen = await self.start()
        # For now ignoring the typing issue. The start method might be splited
        return async_gen  # type: ignore

    async def __aexit__(self, exc_type, exc, tb) -> None:
        logger.info("Stopping async_gen Stream....")
        await self.stop()

    def __aiter__(self):
        return self

    async def __anext__(self) -> ConsumerRecord:
        # This will be used only with async generators
        if not self.running:
            await self.start()

        try:
            # value is a ConsumerRecord, which is a dataclass
            consumer_record: ConsumerRecord = (
                await self.consumer.getone()  # type: ignore
            )

            # call deserializer if there is one regarless consumer_record.value
            # as the end user might want to do something extra with headers or metadata
            if self.deserializer is not None:
                return await self.deserializer.deserialize(consumer_record)

            return consumer_record
        except errors.ConsumerStoppedError:
            raise StopAsyncIteration  # noqa: F821


# Function required by the `stream` decorator
StreamFunc = Callable[[Stream], Awaitable[Any]]


def stream(
    topics: Union[List[str], str],
    *,
    name: Optional[str] = None,
    deserializer: Optional[Deserializer] = None,
    initial_offsets: Optional[List[TopicPartitionOffset]] = None,
    rebalance_listener: Optional[RebalanceListener] = None,
    **kwargs,
) -> Callable[[StreamFunc], Stream]:
    def decorator(func: StreamFunc) -> Stream:
        s = Stream(
            topics=topics,
            func=func,
            name=name,
            deserializer=deserializer,
            initial_offsets=initial_offsets,
            rebalance_listener=rebalance_listener,
            config=kwargs,
        )
        update_wrapper(s, func)
        return s

    return decorator
