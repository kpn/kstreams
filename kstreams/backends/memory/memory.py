from typing import Type

from . import clients


class InMemory:
    """
    InMemory backend for KStreams.
    This backend is useful for testing and development purposes, as it does not
    require any external dependencies. Do not use this backend in production,
    as it does not provide any durability guarantees.

    !!! Example
        ```python
        from kstreams.backends.memory import InMemory
        from kstreams import create_engine


        backend = InMemory()
        stream_engine = create_engine(title="my-stream-engine", backend=backend)
        ```

        !!! note
            Full example of how to use the InMemory backend can be found [here](https://github.com/kpn/kstreams/tree/master/examples/memory-backend)
    """

    producer_class: Type[clients.InMemoryProducer] = clients.InMemoryProducer
    consumer_class: Type[clients.InMemoryConsumer] = clients.InMemoryConsumer

    def to_dict(self):
        return {}
