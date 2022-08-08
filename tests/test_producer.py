from unittest.mock import patch

import pytest

from kstreams.backends.kafka import Kafka
from kstreams.clients import Producer


@pytest.mark.asyncio
async def test_producer():
    with patch("kstreams.clients.aiokafka.AIOKafkaProducer.start") as mock_start_super:
        prod = Producer()

        await prod.start()
        mock_start_super.assert_called()


@pytest.mark.asyncio
async def test_producer_with_ssl(ssl_context):
    backend = Kafka(ssl_context=ssl_context)
    producer = Producer(backend=backend)
    assert producer.client._ssl_context

    await producer.client.close()


@pytest.mark.asyncio
async def test_producer_custom_kafka_config():
    kafka_config = {
        "bootstrap_servers": ["localhost:9093", "localhost:9094"],
        "client_id": "my-client",
    }
    producer = Producer(**kafka_config)

    assert producer.client._bootstrap_servers == kafka_config["bootstrap_servers"]
    assert producer.client._client_id == kafka_config["client_id"]

    await producer.client.close()


@pytest.mark.asyncio
async def test_two_producers():
    kafka_config_1 = {
        "bootstrap_servers": ["localhost:9093"],
        "group_id": "my-group-consumer",
    }
    backend_1 = Kafka(bootstrap_servers=kafka_config_1["bootstrap_servers"])
    producer_1 = Producer(backend=backend_1, client_id="my-client")

    kafka_config_2 = {
        "bootstrap_servers": ["otherhost:9092"],
        "group_id": "my-group-consumer",
    }

    backend_2 = Kafka(bootstrap_servers=kafka_config_2["bootstrap_servers"])
    producer_2 = Producer(backend=backend_2, client_id="client_id2")

    assert producer_1.client._bootstrap_servers == kafka_config_1["bootstrap_servers"]
    assert producer_1.client._client_id == "my-client"

    assert producer_2.client._bootstrap_servers == kafka_config_2["bootstrap_servers"]
    assert producer_2.client._client_id == "client_id2"

    await producer_1.client.close()
    await producer_2.client.close()
