import typing as t

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


class Backend(t.Protocol):
    producer_class: t.Type[AIOKafkaProducer]
    consumer_class: t.Type[AIOKafkaConsumer]

    def to_dict(self) -> t.Dict[str, t.Any]:
        """
        Returns a dictionary representation of the backend configuration.
        """
        ...
