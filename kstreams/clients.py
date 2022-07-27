import logging
from typing import Callable, Optional, TypeVar

import aiokafka
from pkgsettings import PrefixedSettings

from . import conf, utils

logger = logging.getLogger(__name__)

ConsumerType = TypeVar("ConsumerType", bound="Consumer")
ProducerType = TypeVar("ProducerType", bound="Producer")


class Consumer(aiokafka.AIOKafkaConsumer):
    def __init__(
        self,
        *args,
        settings_prefix: str = "SERVICE_KSTREAMS_",
        key_deserializer: Optional[Callable] = None,
        **kwargs,
    ):
        self.settings = PrefixedSettings(conf.settings, prefix=settings_prefix)

        if key_deserializer is None:
            key_deserializer = lambda k: None if k is None else k.decode()  # noqa: E731

        kafka_config = utils.retrieve_kafka_config(settings_prefix=settings_prefix)
        kafka_config.update(kwargs)

        super().__init__(*args, key_deserializer=key_deserializer, **kafka_config)


class Producer(aiokafka.AIOKafkaProducer):
    def __init__(
        self,
        *args,
        settings_prefix: str = "SERVICE_KSTREAMS_",
        key_serializer=None,
        **kwargs,
    ):
        # do settings crap + SSL setup
        self.settings = PrefixedSettings(conf.settings, prefix=settings_prefix)
        if key_serializer is None:
            key_serializer = (
                lambda k: None if k is None else k.encode("utf-8")
            )  # noqa: E731

        kafka_config = utils.retrieve_kafka_config(settings_prefix=settings_prefix)
        kafka_config.update(kwargs)

        super().__init__(*args, key_serializer=key_serializer, **kafka_config)
