from typing import Optional, Type

from .backends.kafka import Kafka
from .clients import Consumer, ConsumerType, Producer, ProducerType
from .engine import StreamEngine
from .serializers import Deserializer, Serializer


def create_engine(
    title: Optional[str] = None,
    backend: Optional[Kafka] = None,
    consumer_class: Type[ConsumerType] = Consumer,
    producer_class: Type[ProducerType] = Producer,
    serializer: Optional[Serializer] = None,
    deserializer: Optional[Deserializer] = None,
) -> StreamEngine:


    if backend is None:
        backend = Kafka()

    return StreamEngine(
        backend=backend,
        title=title,
        consumer_class=consumer_class,
        producer_class=producer_class,
        serializer=serializer,
        deserializer=deserializer,
    )
