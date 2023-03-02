from typing import Set
from unittest import mock

import pytest

from kstreams import (
    ManualCommitRebalanceListener,
    MetricsRebalanceListener,
    RebalanceListener,
    TopicPartition,
    create_engine,
)
from kstreams.backends.kafka import Kafka
from kstreams.clients import Consumer
from kstreams.engine import Stream, StreamEngine


@pytest.mark.asyncio
async def test_consumer():
    with mock.patch(
        "kstreams.clients.aiokafka.AIOKafkaConsumer.start"
    ) as mock_start_super:
        consumer = Consumer()

        await consumer.start()
        mock_start_super.assert_called()


@pytest.mark.asyncio
async def test_consumer_with_ssl(ssl_context):
    backend = Kafka(security_protocol="SSL", ssl_context=ssl_context)
    consumer = Consumer(**backend.dict())
    assert consumer._client._ssl_context


@pytest.mark.asyncio
async def test_init_consumer_with_multiple_topics():
    topics = ["my-topic", "my-topic-2"]
    consumer = Consumer(*topics)

    assert consumer._client._topics == set(topics)


@pytest.mark.asyncio
async def test_consumer_custom_kafka_config():
    kafka_config = {
        "bootstrap_servers": ["localhost:9093", "localhost:9094"],
        "group_id": "my-group-consumer",
    }

    consumer = Consumer("my-topic", **kafka_config)

    # ugly checking of private attributes
    assert consumer._client._bootstrap_servers == kafka_config["bootstrap_servers"]
    assert consumer._group_id == kafka_config["group_id"]


@pytest.mark.asyncio
async def test_add_stream_with_rebalance_listener(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    class MyRebalanceListener(RebalanceListener):
        async def on_partitions_revoked(self, revoked: Set[TopicPartition]) -> None:
            ...

        async def on_partitions_assigned(self, assigned: Set[TopicPartition]) -> None:
            ...

    rebalance_listener = MyRebalanceListener()

    with mock.patch("kstreams.clients.aiokafka.AIOKafkaConsumer.start"), mock.patch(
        "kstreams.clients.aiokafka.AIOKafkaProducer.start"
    ):

        @stream_engine.stream(topic, rebalance_listener=rebalance_listener)
        async def my_stream(stream: Stream):
            async for _ in stream:
                ...

        await stream_engine.start()
        await stream_engine.stop()

        assert my_stream.rebalance_listener == rebalance_listener
        assert rebalance_listener.stream == my_stream

        # checking that the subscription has also the rebalance_listener
        assert my_stream.consumer._subscription._listener == rebalance_listener


@pytest.mark.asyncio
async def test_stream_with_default_rebalance_listener():
    topic = "local--hello-kpn"
    topic_partitions = set(TopicPartition(topic=topic, partition=0))

    with mock.patch("kstreams.clients.aiokafka.AIOKafkaConsumer.start"), mock.patch(
        "kstreams.clients.aiokafka.AIOKafkaProducer.start"
    ), mock.patch("kstreams.PrometheusMonitor.start") as monitor_start, mock.patch(
        "kstreams.PrometheusMonitor.stop"
    ) as monitor_stop:
        # use this function so we can mock PrometheusMonitor
        stream_engine = create_engine()

        @stream_engine.stream(topic)
        async def my_stream(stream: Stream):
            async for _ in stream:
                ...

        await stream_engine.start()
        rebalance_listener = my_stream.rebalance_listener

        assert isinstance(rebalance_listener, MetricsRebalanceListener)
        # checking that the subscription has also the rebalance_listener
        assert isinstance(
            my_stream.consumer._subscription._listener, MetricsRebalanceListener
        )
        assert rebalance_listener.engine == stream_engine

        await rebalance_listener.on_partitions_revoked(revoked=topic_partitions)
        await rebalance_listener.on_partitions_assigned(assigned=topic_partitions)

        monitor_stop.assert_called_once()

        # called twice: When the engine starts and on_partitions_assigned
        monitor_start.assert_has_calls([mock.call(), mock.call()])

        await stream_engine.stop()


@pytest.mark.asyncio
async def test_stream_manual_commit_rebalance_listener(stream_engine: StreamEngine):
    topic = "local--hello-kpn"
    topic_partitions = set(TopicPartition(topic=topic, partition=0))

    with mock.patch("kstreams.clients.aiokafka.AIOKafkaConsumer.start"), mock.patch(
        "kstreams.clients.aiokafka.AIOKafkaConsumer.commit"
    ) as commit_mock, mock.patch("kstreams.clients.aiokafka.AIOKafkaProducer.start"):

        @stream_engine.stream(
            topic,
            group_id="example-group",
            enable_auto_commit=False,
            rebalance_listener=ManualCommitRebalanceListener(),
        )
        async def hello_stream(stream: Stream):
            async for _ in stream:
                ...

        await stream_engine.start()
        await stream_engine.stop()

        rebalance_listener = hello_stream.rebalance_listener

        assert isinstance(rebalance_listener, ManualCommitRebalanceListener)
        # checking that the subscription has also the rebalance_listener
        assert isinstance(
            hello_stream.consumer._subscription._listener, ManualCommitRebalanceListener
        )

        await rebalance_listener.on_partitions_revoked(revoked=topic_partitions)
        commit_mock.assert_awaited_once()

        await stream_engine.clean_streams()
