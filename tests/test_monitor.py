import pytest

from kstreams import Stream, StreamEngine
from kstreams.backends.kafka import Kafka


@pytest.mark.asyncio
async def test_consumer_metrics(mock_consumer_class, stream_engine: StreamEngine):
    async def my_coroutine(_):
        pass

    backend = Kafka()
    stream = Stream(
        "local--hello-kpn",
        backend=backend,
        consumer_class=mock_consumer_class,
        func=my_coroutine,
    )
    stream_engine.add_stream(stream=stream)
    await stream.start()

    await stream_engine.monitor._generate_consumer_metrics(stream.consumer)

    consumer = stream.consumer
    topic_partition = consumer.assignment()[0]

    # super ugly notation but for now is the only way to get the metrics
    met_committed = (
        stream_engine.monitor.met_committed.labels(
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            consumer_group=consumer._group_id,
        )
        .collect()[0]
        .samples[0]
        .value
    )

    met_position = (
        stream_engine.monitor.met_position.labels(
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            consumer_group=consumer._group_id,
        )
        .collect()[0]
        .samples[0]
        .value
    )

    met_highwater = (
        stream_engine.monitor.met_highwater.labels(
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            consumer_group=consumer._group_id,
        )
        .collect()[0]
        .samples[0]
        .value
    )

    met_lag = (
        stream_engine.monitor.met_lag.labels(
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
