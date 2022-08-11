import asyncio
import inspect
import logging
import uuid
from typing import Any, AsyncGenerator, Coroutine, Dict, List, Optional, Type, Union

from aiokafka import errors, structs

from .backends.kafka import Kafka
from .clients import Consumer, ConsumerType
from .serializers import ValueDeserializer

logger = logging.getLogger(__name__)


class Stream:
    def __init__(
        self,
        topics: Union[List[str], str],
        *,
        backend: Kafka,
        func: Union[Coroutine[Any, Any, Type[ConsumerType]], AsyncGenerator],
        consumer_class: Type[ConsumerType] = Consumer,
        name: Optional[str] = None,
        config: Optional[Dict] = None,
        model: Optional[Any] = None,
        value_deserializer: Optional[ValueDeserializer] = None,
    ) -> None:
        self.func = func
        self.backend = backend
        self.consumer_class = consumer_class
        self.config = config or {}
        self._consumer_task: Optional[asyncio.Task] = None
        self.name = name or str(uuid.uuid4())
        self.model = model
        self.value_deserializer = value_deserializer
        self.running = False

        # aiokafka expects topic names as arguments, meaning that
        # can receive N topics -> N arguments,
        # so we always create a list and then we expand it with *topics
        self.topics = [topics] if isinstance(topics, str) else topics

    def _create_consumer(self) -> ConsumerType:
        config = {**self.backend.dict(), **self.config}
        return self.consumer_class(*self.topics, **config)

    async def stop(self) -> None:
        if not self.running:
            return None

        await self.consumer.stop()
        self.running = False

        if self._consumer_task is not None:
            self._consumer_task.cancel()

    async def start(self) -> None:
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
        return async_gen

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
            # value is a ConsumerRecord:
            # namedtuple["topic", "partition", "offset", "key", "value"]
            consumer_record: structs.ConsumerRecord = await self.consumer.getone()

            # deserialize only when value and value_deserializer are present
            if (
                consumer_record.value is not None
                and self.value_deserializer is not None
            ):
                return await self.value_deserializer.deserialize(consumer_record)

            return consumer_record
        except errors.ConsumerStoppedError:
            raise StopAsyncIteration  # noqa: F821
