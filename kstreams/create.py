from typing import Optional, Type

from .backends.kafka import Consumer, Kafka, Producer, ProducerSettings
from .backends.protocol import Backend
from .engine import StreamEngine
from .prometheus.monitor import PrometheusMonitor
from .serializers import Deserializer, Serializer
from .types import EngineHooks


def create_engine(
    title: Optional[str] = None,
    backend: Optional[Backend] = None,
    # consumer_class and producer_class to be removed in the future, use backend instead
    consumer_class: Optional[Type[Consumer]] = None,
    producer_class: Optional[Type[Producer]] = None,
    producer_settings: Optional[ProducerSettings] = None,
    serializer: Optional[Serializer] = None,
    deserializer: Optional[Deserializer] = None,
    monitor: Optional[PrometheusMonitor] = None,
    on_startup: Optional[EngineHooks] = None,
    on_stop: Optional[EngineHooks] = None,
    after_startup: Optional[EngineHooks] = None,
    after_stop: Optional[EngineHooks] = None,
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
        producer_settings=producer_settings,
        serializer=serializer,
        deserializer=deserializer,
        monitor=monitor,
        on_startup=on_startup,
        on_stop=on_stop,
        after_startup=after_startup,
        after_stop=after_stop,
    )
