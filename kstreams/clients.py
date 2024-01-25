import logging
from typing import Callable, Optional

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

            def key_deserializer(key):
                if key is not None:
                    return key.decode()

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

            def key_serializer(key):
                if key is not None:
                    return key.encode("utf-8")

        super().__init__(*args, key_serializer=key_serializer, **self.config)
