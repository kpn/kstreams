from typing import Optional, Type

from .backends.kafka import Kafka
from .clients import Consumer, Producer
from .engine import StreamEngine
from .prometheus.monitor import PrometheusMonitor
from .serializers import Deserializer, Serializer


def create_engine(
    title: Optional[str] = None,
    backend: Optional[Kafka] = None,
    consumer_class: Type[Consumer] = Consumer,
    producer_class: Type[Producer] = Producer,
    serializer: Optional[Serializer] = None,
    deserializer: Optional[Deserializer] = None,
    monitor: Optional[PrometheusMonitor] = None,
) -> StreamEngine:
    if monitor is None:
        monitor = PrometheusMonitor()

    if backend is None:
        backend = Kafka()

    return StreamEngine(
        backend=backend,
        title=title,
        consumer_class=consumer_class,
        producer_class=producer_class,
        serializer=serializer,
        deserializer=deserializer,
        monitor=monitor,
    )
