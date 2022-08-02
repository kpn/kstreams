from typing import Optional, Type

from .clients import Consumer, ConsumerType, Producer, ProducerType
from .engine import StreamEngine
from .prometheus.monitor import PrometheusMonitor, PrometheusMonitorType
from .serializers import ValueDeserializer, ValueSerializer


def create_engine(
    title: Optional[str] = None,
    consumer_class: Type[ConsumerType] = Consumer,
    producer_class: Type[ProducerType] = Producer,
    value_serializer: Optional[ValueSerializer] = None,
    value_deserializer: Optional[ValueDeserializer] = None,
    monitor: Optional[PrometheusMonitorType] = None,
) -> StreamEngine:

    if monitor is None:
        monitor = PrometheusMonitor()

    return StreamEngine(
        title=title,
        consumer_class=consumer_class,
        producer_class=producer_class,
        value_serializer=value_serializer,
        value_deserializer=value_deserializer,
        monitor=monitor,
    )
