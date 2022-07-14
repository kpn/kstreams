from .clients import Consumer, ConsumerType
from .serializers import ValueDeserializer
from aiokafka import errors, structs
from typing import Any, Coroutine, Dict, List, Optional, Type, TypeVar, Union

import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)

KafkaStream = TypeVar("KafkaStream", bound="Stream")
KafkaConsumer = TypeVar("KafkaConsumer", bound=Consumer)


class BaseStream:
    def __init__(
        self,
        *,
        func: Coroutine[Any, Any, Type[KafkaConsumer]],
        consumer_class: Any,
        name: Optional[str] = None,
        kafka_config: Optional[Dict] = None,
        model: Optional[Any] = None,
        value_deserializer: Optional[ValueDeserializer] = None,
    ) -> None:
        self.func = func
        self.consumer_class = consumer_class
        self.kafka_config = kafka_config or {}
        self._consumer_task: Optional[asyncio.Task] = None
        self.name = name or str(uuid.uuid4())
        self.model = model
        self.value_deserializer = value_deserializer
        self._running = False

    def _create_consumer(self):
        ...

    async def stop(self) -> None:
        if self._running:
            await self.consumer.stop()
            self._running = False

    async def start(self) -> None:
        self.consumer = self._create_consumer()

        async def func_wrapper():
            try:
                # await for the end user coroutine
                # we do this to show a better error message to the user
                # when the coroutine fails
                await self.func(self)
            except Exception as e:
                logger.exception(
                    f"CRASHED Stream!!! Task {self._consumer_task} \n\n {e}"
                )

        await self.consumer.start()
        self._running = True
        self._consumer_task = asyncio.create_task(func_wrapper())

    def __aiter__(self):
        return self

    async def __anext__(self) -> structs.ConsumerRecord:
        # This will be used only with async generators
        if not self._running:
            await self.start()

        try:
            # value is a ConsumerRecord:
            # namedtuple["topic", "partition", "offset", "key", "value"]
            consumer_record = await self.consumer.getone()

            if self.value_deserializer is not None:
                return await self.value_deserializer.deserialize(consumer_record)

            return consumer_record
        except errors.ConsumerStoppedError:
            raise StopAsyncIteration  # noqa: F821


class Stream(BaseStream):
    def __init__(
        self,
        topics: Union[List[str], str],
        *,
        func: Coroutine[Any, Any, Type[ConsumerType]],
        consumer_class: Type[ConsumerType] = Consumer,
        name: Optional[str] = None,
        kafka_config: Optional[Dict] = None,
        model: Optional[Any] = None,
        value_deserializer: Optional[ValueDeserializer] = None,
    ) -> None:

        super().__init__(
            func=func,
            consumer_class=consumer_class,
            name=name,
            kafka_config=kafka_config,
            model=model,
            value_deserializer=value_deserializer,
        )
        # aiokafka expects topic names as arguments, meaning that
        # can receive N topics -> N arguments,
        # so we always create a list and then we expand it with *topics
        self.topics = [topics] if isinstance(topics, str) else topics

    def _create_consumer(self) -> ConsumerType:
        return self.consumer_class(*self.topics, **self.kafka_config)

    async def stop(self) -> None:
        await super().stop()

        if self._consumer_task is not None:
            self._consumer_task.cancel()
