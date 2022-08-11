import asyncio
from typing import Any, DefaultDict, List, Type

from kstreams.clients import ConsumerType
from kstreams.streams import Stream

from .monitor import PrometheusMonitorType


async def metrics_task(streams: List[Stream], monitor: PrometheusMonitorType):
    while True:
        for stream in streams:
            if stream.consumer is not None:
                await generate_consumer_metrics(stream.consumer, monitor=monitor)
        await asyncio.sleep(3)


async def generate_consumer_metrics(
    consumer: Type[ConsumerType], monitor: PrometheusMonitorType
):
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

    monitor.add_consumer_metrics(metrics)
