import typing as t

from .kafka.clients import Consumer, Producer


class Backend(t.Protocol):
    producer_class: t.Type[Producer]
    consumer_class: t.Type[Consumer]

    def to_dict(self) -> t.Dict[str, t.Any]:
        """
        Returns a dictionary representation of the backend configuration.
        """
        ...
