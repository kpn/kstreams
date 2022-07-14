from kstreams.clients import Producer
from kstreams.conf import settings
from unittest.mock import patch

import pytest


@pytest.mark.asyncio
async def test_producer():
    with patch("kstreams.clients.aiokafka.AIOKafkaProducer.start") as mock_start_super:
        prod = Producer()

        await prod.start()
        mock_start_super.assert_called()


@pytest.mark.asyncio
async def test_producer_with_ssl(ssl_data):
    settings.configure(
        SERVICE_KSTREAMS_KAFKA_CONFIG_SECURITY_PROTOCOL="SSL",
        SERVICE_KSTREAMS_KAFKA_SSL_CERT_DATA=ssl_data.cert,
        SERVICE_KSTREAMS_KAFKA_SSL_KEY_DATA=ssl_data.key,
    )

    producer = Producer()
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
    settings.configure(
        TEST_KSTREAMS_KAFKA_CONFIG_BOOTSTRAP_SERVERS=["otherhost:9092"],
        TEST_KSTREAMS_KAFKA_CONFIG_SECURITY_PROTOCOL="PLAINTEXT",
        TEST_KSTREAMS_KAFKA_CONFIG_CLIENT_ID="client_id2",
        TEST_KSTREAMS_KAFKA_SSL_CERT_DATA=None,
        TEST_KSTREAMS_KAFKA_SSL_KEY_DATA=None,
        TEST_KSTREAMS_KAFKA_TOPIC_PREFIX="",
    )
    kafka_config = {
        "bootstrap_servers": ["localhost:9093", "localhost:9094"],
        "client_id": "my-client",
    }
    producer = Producer(**kafka_config)
    producer2 = Producer(settings_prefix="TEST_KSTREAMS_")

    assert producer.client._bootstrap_servers == kafka_config["bootstrap_servers"]
    assert producer.client._client_id == kafka_config["client_id"]

    assert producer2.client._bootstrap_servers == ["otherhost:9092"]
    assert producer2.client._client_id == "client_id2"

    await producer.client.close()
    await producer2.client.close()
