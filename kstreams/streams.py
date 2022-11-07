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
    Type,
    Union,
)

from aiokafka import errors, structs

from kstreams.exceptions import BackendNotSet

from .backends.kafka import Kafka
from .clients import Consumer, ConsumerType
from .serializers import Deserializer

logger = logging.getLogger(__name__)


class Stream:
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

        # aiokafka expects topic names as arguments, meaning that
        # can receive N topics -> N arguments,
        # so we always create a list and then we expand it with *topics
        self.topics = [topics] if isinstance(topics, str) else topics

    def _create_consumer(self) -> Type[ConsumerType]:
        if self.backend is None:
            raise BackendNotSet("A backend has not been set for this stream")
        config = {**self.backend.dict(), **self.config}
        return self.consumer_class(*self.topics, **config)

    async def stop(self) -> None:
        if not self.running:
            return None

        if self.consumer is not None:
            await self.consumer.stop()
            self.running = False

        if self._consumer_task is not None:
            self._consumer_task.cancel()

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

        if self.consumer is None:
            self.consumer = self._create_consumer()

        func = self.func(self)
        await self.consumer.start()
        self.running = True

        if inspect.isasyncgen(func):
            return func
        else:
            # It is not an async_generator so we need to
            # create an asyncio.Task with func
            self._consumer_task = asyncio.create_task(func_wrapper(func))
            return None

    async def __aenter__(self) -> AsyncGenerator:
        """
        Start the kafka Consumer and return an async_gen so it can be iterated

        Usage:
            @stream_engine.stream(topic, group_id=group_id, ...)
            async def stream(consumer):
                async for cr, value, headers in consumer:
                    yield value


            # Iterate the stream:
            async with stream as stream_flow:
                async for value in stream_flow:
                    ...
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

    async def __anext__(self) -> structs.ConsumerRecord:
        # This will be used only with async generators
        if not self.running:
            await self.start()

        try:
            # value is a ConsumerRecord, which is a dataclass
            consumer_record: structs.ConsumerRecord = (
                await self.consumer.getone()  # type: ignore
            )

            # deserialize only when value and deserializer are present
            if consumer_record.value is not None and self.deserializer is not None:
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
    **kwargs,
) -> Callable[[StreamFunc], Stream]:
    def decorator(func: StreamFunc) -> Stream:
        s = Stream(
            topics=topics,
            func=func,
            name=name,
            deserializer=deserializer,
            config=kwargs,
        )
        update_wrapper(s, func)
        return s

    return decorator
