import asyncio
import logging
from typing import Set

from aiokafka.abc import ConsumerRebalanceListener

from kstreams import TopicPartition

logger = logging.getLogger(__name__)


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
                self, revoked: Set[TopicPartition]
            ) -> None:
                # Do something with the revoked partitions
                # or with the Stream
                print(self.stream)

            async def on_partitions_assigned(
                self, assigned: Set[TopicPartition]
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
        # engine added so it can react on rebalance events
        self.engine = None

    async def on_partitions_revoked(self, revoked: Set[TopicPartition]) -> None:
        """
        Coroutine to be called *before* a rebalance operation starts and
        *after* the consumer stops fetching data.

        If you are using manual commit you have to commit all consumed offsets
        here, to avoid duplicate message delivery after rebalance is finished.

        Use cases:
            - cleanup or custom state save on the start of a rebalance operation
            - saving offsets in a custom store

        Attributes:
            revoked Set[TopicPartitions]: Partitions that were assigned
                to the consumer on the last rebalance

        !!! note
            The `Stream` is available using `self.stream`
        """
        ...  # pragma: no cover

    async def on_partitions_assigned(self, assigned: Set[TopicPartition]) -> None:
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
            assigned Set[TopicPartition]: Partitions assigned to the
                consumer (may include partitions that were previously assigned)

        !!! note
            The `Stream` is available using `self.stream`
        """
        ...  # pragma: no cover


class MetricsRebalanceListener(RebalanceListener):
    async def on_partitions_revoked(self, revoked: Set[TopicPartition]) -> None:
        """
        Coroutine to be called *before* a rebalance operation starts and
        *after* the consumer stops fetching data.

        This will method will clean up the `Prometheus` metrics

        Attributes:
            revoked Set[TopicPartitions]: Partitions that were assigned
                to the consumer on the last rebalance
        """
        # lock all asyncio Tasks so no new metrics will be added to the Monitor
        if revoked and self.engine is not None:
            async with asyncio.Lock():
                self.engine.monitor.stop()

    async def on_partitions_assigned(self, assigned: Set[TopicPartition]) -> None:
        """
        Coroutine to be called *after* partition re-assignment completes
        and *before* the consumer starts fetching data again.

        This method will start the `Prometheus` metrics

        Attributes:
            assigned Set[TopicPartition]: Partitions assigned to the
                consumer (may include partitions that were previously assigned)
        """
        # lock all asyncio Tasks so no new metrics will be added to the Monitor
        if assigned and self.engine is not None:
            async with asyncio.Lock():
                self.engine.monitor.start()


class ManualCommitRebalanceListener(MetricsRebalanceListener):
    async def on_partitions_revoked(self, revoked: Set[TopicPartition]) -> None:
        """
        Coroutine to be called *before* a rebalance operation starts and
        *after* the consumer stops fetching data.

        If manual commit is enabled, `commit` is called before the consumers
        partitions are revoked to prevent the error `CommitFailedError`
        and duplicate message delivery after a rebalance.

        Attributes:
            revoked Set[TopicPartitions]: Partitions that were assigned
                to the consumer on the last rebalance
        """
        if (
            revoked
            and self.stream is not None
            and not self.stream.consumer._enable_auto_commit
        ):
            logger.info(
                f"Manual commit enabled for stream {self.stream}. "
                "Performing `commit` before revoking partitions"
            )
            async with asyncio.Lock():
                await self.stream.commit()

            await super().on_partitions_revoked(revoked=revoked)
