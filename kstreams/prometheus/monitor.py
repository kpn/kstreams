import asyncio
import logging
from typing import DefaultDict, Dict, Optional, TypeVar

from prometheus_client import Gauge

from kstreams import TopicPartition
from kstreams.clients import ConsumerType
from kstreams.streams import Stream

logger = logging.getLogger(__name__)

PrometheusMonitorType = TypeVar("PrometheusMonitorType", bound="PrometheusMonitor")
MetricsType = Dict[TopicPartition, Dict[str, Optional[int]]]


class PrometheusMonitor:
    """
    Metrics monitor to keep track of Producers and Consumers.
    """

    # Producer metrics
    MET_OFFSETS = Gauge(
        "topic_partition_offsets", "help producer offsets", ["topic", "partition"]
    )

    # Consumer metrics
    MET_COMMITTED = Gauge(
        "consumer_committed",
        "help consumer committed",
        ["topic", "partition", "consumer_group"],
    )
    MET_POSITION = Gauge(
        "consumer_position",
        "help consumer position",
        ["topic", "partition", "consumer_group"],
    )
    MET_HIGHWATER = Gauge(
        "consumer_highwater",
        "help consumer highwater",
        ["topic", "partition", "consumer_group"],
    )
    MET_LAG = Gauge(
        "consumer_lag",
        "help consumer lag calculated using the last commited offset",
        ["topic", "partition", "consumer_group"],
    )
    MET_POSITION_LAG = Gauge(
        "position_lag",
        "help consumer position lag calculated using the consumer position",
        ["topic", "partition", "consumer_group"],
    )

    def __init__(self):
        self._producer = None
        self._streams = []
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        logger.info("Starting Prometheus metrics...")
        self._task = asyncio.create_task(self._metrics_task())

    async def stop(self) -> None:
        logger.info("Stoping Prometheus metrics...")
        if self._task is not None:
            self._task.cancel()

            # we need to make sure that the task is cancelled
            # to clean up properly
            while not self._task.cancelled():
                await asyncio.sleep(0.1)

        self._clean_consumer_metrics()

    def add_topic_partition_offset(
        self, topic: str, partition: int, offset: int
    ) -> None:
        self.MET_OFFSETS.labels(topic=topic, partition=partition).set(offset)

    def _add_consumer_metrics(self, metrics_dict: MetricsType):
        for topic_partition, partitions_metadata in metrics_dict.items():
            group_id = partitions_metadata["group_id"]
            position = partitions_metadata["position"]
            committed = partitions_metadata["committed"]
            highwater = partitions_metadata["highwater"]
            lag = partitions_metadata["lag"]
            position_lag = partitions_metadata["position_lag"]

            self.MET_COMMITTED.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=group_id,
            ).set(committed or 0)
            self.MET_POSITION.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=group_id,
            ).set(position or -1)
            self.MET_HIGHWATER.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=group_id,
            ).set(highwater or 0)
            self.MET_LAG.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=group_id,
            ).set(lag or 0)
            self.MET_POSITION_LAG.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=group_id,
            ).set(position_lag or 0)

    def _clean_consumer_metrics(self) -> None:
        """
        This method should be called when a rebalance takes place
        to clean all consumers metrics. When the rebalance finishes
        new metrics will be generated per consumer based on the
        consumer assigments
        """
        self.MET_LAG.clear()
        self.MET_POSITION_LAG.clear()
        self.MET_COMMITTED.clear()
        self.MET_POSITION.clear()
        self.MET_HIGHWATER.clear()

    def clean_stream_consumer_metrics(self, stream: Stream) -> None:
        if stream.consumer is not None:
            topic_partitions = stream.consumer.assignment()
            group_id = stream.consumer._group_id
            for topic_partition in topic_partitions:
                topic = topic_partition.topic
                partition = topic_partition.partition
                self.MET_LAG.remove(topic, partition, group_id)
                self.MET_POSITION_LAG.remove(topic, partition, group_id)
                self.MET_COMMITTED.remove(topic, partition, group_id)
                self.MET_POSITION.remove(topic, partition, group_id)
                self.MET_HIGHWATER.remove(topic, partition, group_id)

    def add_producer(self, producer):
        self._producer = producer

    def add_streams(self, streams):
        self._streams = streams

    async def generate_consumer_metrics(self, consumer: ConsumerType):
        """
        Generate Consumer Metrics for Prometheus

        Format:
            {
                "topic-1": {
                    "1": (
                        [topic-1, partition-number, 'group-id-1'],
                        committed, position, highwater, lag, position_lag
                    )
                    "2": (
                        [topic-1, partition-number, 'group-id-1'],
                        committed, position, highwater, lag, position_lag
                    )
                },
                ...
                "topic-n": {
                    "1": (
                        [topic-n, partition-number, 'group-id-n'],
                        committed, position, highwater, lag, position_lag
                    )
                    "2": (
                        [topic-n, partition-number, 'group-id-n'],
                        committed, position, highwater, lag, position_lag
                    )
                }
            }
        """
        metrics: MetricsType = DefaultDict(dict)

        topic_partitions = consumer.assignment()

        for topic_partition in topic_partitions:
            committed = consumer.last_stable_offset(topic_partition)
            position = await consumer.position(topic_partition)
            highwater = consumer.highwater(topic_partition)

            lag = position_lag = None
            if highwater:
                lag = highwater - committed
                position_lag = highwater - position

            metrics[topic_partition] = {
                "group_id": consumer._group_id,
                "committed": committed,
                "position": position,
                "highwater": highwater,
                "lag": lag,
                "position_lag": position_lag,
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
                    await self.generate_consumer_metrics(stream.consumer)
