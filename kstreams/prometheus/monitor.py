import asyncio
import logging
from typing import Any, DefaultDict, Dict, Optional, TypeVar

from prometheus_client import Gauge

from kstreams.clients import ConsumerType

from .singleton import Singleton

logger = logging.getLogger(__name__)

PrometheusMonitorType = TypeVar("PrometheusMonitorType", bound="PrometheusMonitor")


class PrometheusMonitor(metaclass=Singleton):
    """
    Functionality for Producers to generate Prometheus metrics
    """

    def __init__(self):
        self._producer = None
        self._streams = []
        self._task: Optional[asyncio.Task] = None

        # Producer metrics
        self.met_offsets = Gauge(
            "topic_partition_offsets", "help producer offsets", ["topic", "partition"]
        )

        # Consumer metrics
        self.met_committed = Gauge(
            "consumer_committed",
            "help consumer committed",
            ["topic", "partition", "consumer_group"],
        )
        self.met_position = Gauge(
            "consumer_position",
            "help consumer position",
            ["topic", "partition", "consumer_group"],
        )
        self.met_highwater = Gauge(
            "consumer_highwater",
            "help consumer highwater",
            ["topic", "partition", "consumer_group"],
        )
        self.met_lag = Gauge(
            "consumer_lag",
            "help consumer lag",
            ["topic", "partition", "consumer_group"],
        )

    def start(self) -> None:
        logger.info("Starting Prometheus metrics...")
        self._task = asyncio.create_task(self._metrics_task())

    def stop(self) -> None:
        logger.info("Stoping Prometheus metrics...")
        if self._task is not None:
            self._task.cancel()
        self._clean_consumer_metrics()

    def add_topic_partition_offset(
        self, topic: str, partition: int, offset: int
    ) -> None:
        """
        Add a simple metric to metric dict. Useful for debugging.
        If Prometheus is enabled, this metrics will be collected
        """
        self.met_offsets.labels(topic=topic, partition=partition).set(offset)

    def _add_consumer_metrics(self, metrics_dict: Dict):
        for topic, partitions_metadata in metrics_dict.items():
            partition = partitions_metadata["partition"]
            group_id = partitions_metadata["group_id"]

            position = partitions_metadata["position"]
            committed = partitions_metadata["committed"]
            highwater = partitions_metadata["highwater"]
            lag = partitions_metadata["lag"]

            self.met_committed.labels(
                topic=topic, partition=partition, consumer_group=group_id
            ).set(committed or 0)
            self.met_position.labels(
                topic=topic, partition=partition, consumer_group=group_id
            ).set(position or -1)
            self.met_highwater.labels(
                topic=topic, partition=partition, consumer_group=group_id
            ).set(highwater or 0)
            self.met_lag.labels(
                topic=topic, partition=partition, consumer_group=group_id
            ).set(lag or 0)

    def _clean_consumer_metrics(self) -> None:
        """
        This method should be called when a rebalance takes place
        to clean all consumers metrics. When the rebalance finishes
        new metrics will be generated per consumer based on the
        consumer assigments
        """
        self.met_lag.clear()
        self.met_committed.clear()
        self.met_position.clear()
        self.met_highwater.clear()

    def add_producer(self, producer):
        self._producer = producer

    def add_streams(self, streams):
        self._streams = streams

    async def _generate_consumer_metrics(self, consumer: ConsumerType):
        """
        Generate Consumer Metrics for Prometheus

        Format:
            {
                "topic-1": {
                    "1": (
                        [topic-1, partition-number, 'group-id-1'],
                        committed, position, highwater, lag
                    )
                    "2": (
                        [topic-1, partition-number, 'group-id-1'],
                        committed, position, highwater, lag
                    )
                },
                ...
                "topic-n": {
                    "1": (
                        [topic-n, partition-number, 'group-id-n'],
                        committed, position, highwater, lag
                    )
                    "2": (
                        [topic-n, partition-number, 'group-id-n'],
                        committed, position, highwater, lag
                    )
                }
            }
        """
        metrics: DefaultDict[Any, dict] = DefaultDict(dict)

        topic_partitions = consumer.assignment()

        for topic_partition in topic_partitions:
            committed = consumer.last_stable_offset(topic_partition)
            position = await consumer.position(topic_partition)
            highwater = consumer.highwater(topic_partition)

            lag = None
            if highwater:
                lag = highwater - position

            metrics[topic_partition.topic] = {
                "partition": topic_partition.partition,
                "group_id": consumer._group_id,
                "committed": committed,
                "position": position,
                "highwater": highwater,
                "lag": lag,
            }

        self._add_consumer_metrics(metrics)

    async def _metrics_task(self) -> None:
        """
        Asyncio Task that runs in `backgroud` to generate
        consumer metrics
        """
        while True:
            await asyncio.sleep(3)
            for stream in self._streams:
                if stream.consumer is not None:
                    await self._generate_consumer_metrics(stream.consumer)
