import logging
from typing import Callable, Optional, TypeVar

import aiokafka

from kstreams.backends.kafka import Kafka

logger = logging.getLogger(__name__)

ConsumerType = TypeVar("ConsumerType", bound="Consumer")
ProducerType = TypeVar("ProducerType", bound="Producer")


class Consumer(aiokafka.AIOKafkaConsumer):
    def __init__(
        self,
        *args,
        backend: Optional[Kafka] = None,
        key_deserializer: Optional[Callable] = None,
        **kwargs,
    ):
        if backend is None:
            backend = Kafka()

        self.backend = backend
        self.kafka_config = {**backend.dict(), **kwargs}

        if key_deserializer is None:
            key_deserializer = lambda k: None if k is None else k.decode()

        super().__init__(*args, key_deserializer=key_deserializer, **self.kafka_config)


class Producer(aiokafka.AIOKafkaProducer):
    def __init__(
        self,
        *args,
        backend: Optional[Kafka] = None,
        key_serializer=None,
        **kwargs,
    ):
        if backend is None:
            backend = Kafka()

        self.backend = backend
        self.kafka_config = {**backend.dict(), **kwargs}

        if key_serializer is None:
            key_serializer = lambda k: None if k is None else k.encode("utf-8")

        super().__init__(*args, key_serializer=key_serializer, **self.kafka_config)
