from kstreams.prometheus import monitor, tasks
from kstreams.streams import Stream

import pytest


@pytest.mark.asyncio
async def test_consumer_metrics(mock_consumer_class):
    async def my_coroutine(_):
        pass

    stream = Stream(
        "local--hello-kpn", consumer_class=mock_consumer_class, func=my_coroutine
    )
    await stream.start()

    prometheus_monitor = monitor.PrometheusMonitor()
    await tasks.generate_consumer_metrics(stream.consumer, monitor=prometheus_monitor)

    consumer = stream.consumer
    topic_partition = consumer.assignment()[0]

    # super ugly notation but for now is the only way to get the metrics
    met_committed = (
        prometheus_monitor.met_committed.labels(
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            consumer_group=consumer._group_id,
        )
        .collect()[0]
        .samples[0]
        .value
    )

    met_position = (
        prometheus_monitor.met_position.labels(
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            consumer_group=consumer._group_id,
        )
        .collect()[0]
        .samples[0]
        .value
    )

    met_highwater = (
        prometheus_monitor.met_highwater.labels(
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            consumer_group=consumer._group_id,
        )
        .collect()[0]
        .samples[0]
        .value
    )

    met_lag = (
        prometheus_monitor.met_lag.labels(
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            consumer_group=consumer._group_id,
        )
        .collect()[0]
        .samples[0]
        .value
    )

    consumer_position = await consumer.position(topic_partition)

    assert met_committed == consumer.last_stable_offset(topic_partition)
    assert met_position == consumer_position
    assert met_highwater == consumer.highwater(topic_partition)
    assert met_lag == consumer.highwater(topic_partition) - consumer_position
