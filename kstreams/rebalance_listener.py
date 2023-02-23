from typing import List

from aiokafka.abc import ConsumerRebalanceListener

from kstreams import TopicPartition


# Can not use a Protocol here because aiokafka forces to have a concrete instance
# that inherits from ConsumerRebalanceListener, if we use a protocol we will
# have to force the end users to import the class and inherit from it,
# then we will mix protocols and inheritance
class RebalanceListener(ConsumerRebalanceListener):
    """
    A callback interface that the user can implement to trigger custom actions
    when the set of partitions are assigned or revoked to the `Stream`.

    !!! Example
        ```python
        from kstreams import RebalanceListener, TopicPartition
        from .resource import stream_engine


        class MyRebalanceListener(RebalanceListener):

            async def on_partitions_revoked(
                self, revoked: List[TopicPartition]
            ) -> None:
                # Do something with the revoked partitions
                # or with the Stream
                print(self.stream)

            async def on_partitions_assigned(
                self, assigned: List[TopicPartition]
            ) -> None:
                # Do something with the assigned partitions
                # or with the Stream
                print(self.stream)


        @stream_engine.stream(topic, rebalance_listener=MyRebalanceListener())
        async def my_stream(stream: Stream):
            async for event in stream:
                ...
        ```
    """

    def __init__(self) -> None:
        self.stream = None

    async def on_partitions_revoked(self, revoked: List[TopicPartition]) -> None:
        """
        Coroutine to be called *before* a rebalance operation starts and
        *after* the consumer stops fetching data.

        If you are using manual commit you have to commit all consumed offsets
        here, to avoid duplicate message delivery after rebalance is finished.

        Use cases:
            - cleanup or custom state save on the start of a rebalance operation
            - saving offsets in a custom store

        Attributes:
            revoked List[TopicPartitions]: Partitions that were assigned
                to the consumer on the last rebalance

        !!! note
            The `Stream` is available using `self.stream`
        """
        ...  # pragma: no cover

    async def on_partitions_assigned(self, assigned: List[TopicPartition]) -> None:
        """
        Coroutine to be called *after* partition re-assignment completes
        and *before* the consumer starts fetching data again.

        It is guaranteed that all the processes in a consumer group will
        execute their `on_partitions_revoked` callback before any instance
        executes its `on_partitions_assigned` callback.

        Use cases:
            - Load a state or cache warmup on completion of a successful
            partition re-assignment.

        Attributes:
            assigned List[TopicPartition]: Partitions assigned to the
                consumer (may include partitions that were previously assigned)

        !!! note
            The `Stream` is available using `self.stream`
        """
        ...  # pragma: no cover
