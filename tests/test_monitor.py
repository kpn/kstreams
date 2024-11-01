import pytest
from prometheus_client import Counter

from kstreams import PrometheusMonitor, Stream, StreamEngine
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

    await stream_engine.monitor.generate_consumer_metrics(stream.consumer)
    consumer = stream.consumer

    for topic_partition in consumer.assignment():
        # super ugly notation but for now is the only way to get the metrics
        met_committed = (
            stream_engine.monitor.MET_COMMITTED.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        met_position = (
            stream_engine.monitor.MET_POSITION.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        met_highwater = (
            stream_engine.monitor.MET_HIGHWATER.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        met_lag = (
            stream_engine.monitor.MET_LAG.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        met_position_lag = (
            stream_engine.monitor.MET_POSITION_LAG.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        consumer_position = await consumer.position(topic_partition)
        commited_position = await consumer.committed(topic_partition)

        assert met_committed == commited_position
        assert met_position == consumer_position
        assert met_highwater == consumer.highwater(topic_partition)
        assert met_lag == consumer.highwater(topic_partition) - commited_position
        assert (
            met_position_lag == consumer.highwater(topic_partition) - consumer_position
        )


@pytest.mark.asyncio
async def test_shared_default_metrics_between_monitors():
    class MyMonitor(PrometheusMonitor):
        MY_COUNTER = Counter("my_failures", "Description of counter")

    default_monitor = PrometheusMonitor()
    my_monitor = MyMonitor()

    # no more Singlenton
    assert default_monitor != my_monitor

    assert default_monitor.MET_OFFSETS == my_monitor.MET_OFFSETS
    assert default_monitor.MET_COMMITTED == my_monitor.MET_COMMITTED
    assert default_monitor.MET_POSITION == my_monitor.MET_POSITION
    assert default_monitor.MET_HIGHWATER == my_monitor.MET_HIGHWATER
    assert default_monitor.MET_LAG == my_monitor.MET_LAG


@pytest.mark.asyncio
async def test_clean_stream_consumer_metrics(
    mock_consumer_class, stream_engine: StreamEngine
):
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

    assert stream.consumer is not None

    await stream_engine.monitor.generate_consumer_metrics(stream.consumer)
    consumer = stream.consumer

    for topic_partition in consumer.assignment():
        # super ugly notation but for now is the only way to get the metrics
        met_committed = (
            stream_engine.monitor.MET_COMMITTED.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        met_position = (
            stream_engine.monitor.MET_POSITION.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        met_highwater = (
            stream_engine.monitor.MET_HIGHWATER.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        met_lag = (
            stream_engine.monitor.MET_LAG.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        met_position_lag = (
            stream_engine.monitor.MET_POSITION_LAG.labels(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                consumer_group=consumer._group_id,
            )
            .collect()[0]
            .samples[0]
            .value
        )

        consumer_position = await consumer.position(topic_partition)
        commited_position = await consumer.committed(topic_partition)

        assert met_committed == commited_position
        assert met_position == consumer_position
        assert met_highwater == consumer.highwater(topic_partition)
        assert met_lag == consumer.highwater(topic_partition) - commited_position
        assert (
            met_position_lag == consumer.highwater(topic_partition) - consumer_position
        )

    assert len(stream_engine.monitor.MET_POSITION_LAG.collect()[0].samples) == 2
    await stream_engine.remove_stream(stream)
    assert len(stream_engine.monitor.MET_POSITION_LAG.collect()[0].samples) == 0


@pytest.mark.asyncio
async def test_skip_clean_stream_consumer_metrics(
    mock_consumer_class, stream_engine: StreamEngine, caplog
):
    async def my_coroutine(_):
        pass

    backend = Kafka()
    stream = Stream(
        "local--hello-kpn",
        name="my-stream-name",
        backend=backend,
        consumer_class=mock_consumer_class,
        func=my_coroutine,
    )
    stream_engine.add_stream(stream=stream)
    await stream.start()

    assert len(stream_engine.monitor.MET_POSITION_LAG.collect()[0].samples) == 0
    await stream_engine.remove_stream(stream)
    assert "Metrics for consumer with group-id: my-group not found" in caplog.text
