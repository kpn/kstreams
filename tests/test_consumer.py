from typing import List
from unittest import mock

import pytest

from kstreams import RebalanceListener, TopicPartition
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
        async def on_partitions_revoked(self, revoked: List[TopicPartition]) -> None:
            ...

        async def on_partitions_assigned(self, assigned: List[TopicPartition]) -> None:
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

    assert my_stream.rebalance_listener == rebalance_listener
    assert rebalance_listener.stream == my_stream

    # checking that the subscription has also the rebalance_listener
    assert my_stream.consumer._subscription._listener == rebalance_listener
    await stream_engine.stop()
