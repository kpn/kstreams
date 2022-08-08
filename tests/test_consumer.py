from unittest import mock

import pytest

from kstreams.backends.kafka import Kafka
from kstreams.clients import Consumer


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
