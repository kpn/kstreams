from typing import Dict, TypeVar

from prometheus_client import Gauge

from .singleton import Singleton

PrometheusMonitorType = TypeVar("PrometheusMonitorType", bound="PrometheusMonitor")


class PrometheusMonitor(metaclass=Singleton):
    """
    Functionality for Producers to generate Prometheus metrics
    """

    def __init__(self):
        self._producer = None
        self._streams = []

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

    def add_topic_partition_offset(
        self, topic: str, partition: int, offset: int
    ) -> None:
        """
        Add a simple metric to metric dict. Useful for debugging.
        If Prometheus is enabled, this metrics will be collected
        """
        self.met_offsets.labels(topic=topic, partition=partition).set(offset)

    def add_consumer_metrics(self, metrics_dict: Dict):
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

    def add_producer(self, producer):
        self._producer = producer

    def add_streams(self, streams):
        self._streams = streams
