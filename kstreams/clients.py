import logging
from typing import Callable, Optional, TypeVar

import aiokafka

logger = logging.getLogger(__name__)


class Consumer(aiokafka.AIOKafkaConsumer):
    def __init__(
        self,
        *args,
        key_deserializer: Optional[Callable] = None,
        **kwargs,
    ):
        self.config = kwargs

        if key_deserializer is None:
            key_deserializer = lambda k: None if k is None else k.decode()

        super().__init__(*args, key_deserializer=key_deserializer, **self.config)


class Producer(aiokafka.AIOKafkaProducer):
    def __init__(
        self,
        *args,
        key_serializer=None,
        **kwargs,
    ):
        self.config = kwargs

        if key_serializer is None:
            key_serializer = lambda k: None if k is None else k.encode("utf-8")

        super().__init__(*args, key_serializer=key_serializer, **self.config)


ConsumerType = TypeVar("ConsumerType", bound=Consumer)
ProducerType = TypeVar("ProducerType", bound=Producer)
