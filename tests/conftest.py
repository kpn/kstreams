import logging
from collections import namedtuple
from dataclasses import field
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

import pytest
import pytest_asyncio
from faker import Faker
from pytest_httpserver import HTTPServer

from kstreams import ConsumerRecord, clients, create_engine
from kstreams.utils import create_ssl_context_from_mem

# Silence faker DEBUG logs
logger = logging.getLogger("faker")
logger.setLevel(logging.INFO)


class RecordMetadata(NamedTuple):
    offset: int = 1
    partition: int = 1
    topic: str = "my-topic"
    timestamp: int = 1616671352653
    event: Dict = field(default_factory=lambda: {"message": "test"})


class TopicPartition(NamedTuple):
    topic: str
    partition: int


class Base:
    async def start(self):
        pass

    async def stop(self):
        pass


class MockProducer(Base):
    def __init__(self, settings_prefix: str = "SERVICE_KSTREAMS_", **kwargs) -> None:
        self.settings_prefix = settings_prefix

    async def send(self, *args, **kwargs):
        return RecordMetadata()


class MockConsumer(Base):
    def __init__(
        self,
        topic: str,
        group_id: str = "my-group",
        assigments: Optional[List[TopicPartition]] = None,
        **kwargs
    ) -> None:
        self.topic = topic
        self._group_id = group_id
        if assigments is None:
            self._assigments = [TopicPartition(topic="my-topic", partition=1)]

    def assignment(self):
        return self._assigments

    def last_stable_offset(self, topic_partition: TopicPartition):
        return 10

    async def position(self, topic_partition: TopicPartition):
        return 10

    def highwater(self, topic_partition: TopicPartition):
        return 10


@pytest.fixture
def record_metadata():
    return RecordMetadata()


@pytest.fixture
def mock_consumer_class():
    return MockConsumer


@pytest.fixture
def mock_producer_class():
    return MockProducer


@pytest.fixture
def topics():
    return ["my_topic", "topic_one", "topic_two"]


@pytest_asyncio.fixture
async def stream_engine():
    stream_engine = create_engine(
        title="test-engine",
        consumer_class=clients.Consumer,
        producer_class=clients.Producer,
    )
    yield stream_engine
    await stream_engine.clean_streams()


SSLData = namedtuple("SSLData", ["cabundle", "cert", "key"])


@pytest.fixture
def ssl_data():
    with open("tests/fixtures/ssl/cabundle.pem") as cabundle, open(
        "tests/fixtures/ssl/certificate.pem"
    ) as cert, open("tests/fixtures/ssl/certificate.key") as key:
        return SSLData(cabundle.read(), cert.read(), key.read())


@pytest.fixture
def ssl_context(ssl_data):
    ssl_context = create_ssl_context_from_mem(
        ssl_data.cabundle,
        ssl_data.cert,
        ssl_data.key,
    )
    return ssl_context


AVRO_SCHEMA_V1 = {
    "type": "record",
    "name": "HelloKPN",
    "fields": [{"name": "message", "type": "string", "default": ""}],
}

AVRO_SCHEMA_V2 = {
    "type": "record",
    "name": "HelloKPN",
    "fields": [
        {"name": "message", "type": "string", "default": ""},
        {"name": "additional_message", "type": "string", "default": "default"},
    ],
}


@pytest.fixture(scope="function")
def schema_server_url(httpserver: HTTPServer):
    schema_path_v1 = "/schemas/example/hello_kpn/v0.0.1/schema.avsc"
    schema_path_v2 = "/schemas/example/hello_kpn/v0.0.2/schema.avsc"
    httpserver.expect_request("/schemas").respond_with_data("OK")
    httpserver.expect_request(schema_path_v1).respond_with_json(AVRO_SCHEMA_V1)
    httpserver.expect_request(schema_path_v2).respond_with_json(AVRO_SCHEMA_V2)
    return httpserver.url_for("/schemas")


@pytest.fixture
def avro_schema_v1():
    return AVRO_SCHEMA_V1


@pytest.fixture()
def consumer_record_factory():
    """
    RecordMetadata from AIOKafka
    """

    def consumer_record(
        topic: str = "my-topic",
        partition: int = 1,
        offset: int = 1,
        key: Optional[Any] = None,
        value: Optional[Any] = None,
        headers: Optional[Sequence[Tuple[str, bytes]]] = None,
    ):
        class ConsumerRecord(NamedTuple):
            offset: int
            partition: int
            topic: str
            key: Optional[Any]
            value: Optional[Any]
            headers: Optional[Sequence[Tuple[str, bytes]]]

        return ConsumerRecord(
            topic=topic,
            partition=partition,
            offset=offset,
            key=key,
            value=value,
            headers=headers,
        )

    return consumer_record


@pytest.fixture
def fake():
    return Faker()


@pytest.fixture()
def rand_consumer_record(fake: Faker):
    """A random consumer record"""

    def generate(
        topic: Optional[str] = None,
        headers: Optional[Sequence[Tuple[str, bytes]]] = None,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        timestamp: Optional[int] = None,
        timestamp_type: Optional[int] = None,
        key: Optional[Any] = None,
        value: Optional[Any] = None,
        checksum: Optional[int] = None,
        serialized_key_size: Optional[int] = None,
        serialized_value_size: Optional[int] = None,
    ) -> ConsumerRecord:

        return ConsumerRecord(
            topic=topic or fake.slug(),
            headers=headers or tuple(),
            partition=partition or fake.pyint(max_value=10),
            offset=offset or fake.pyint(max_value=99999999),
            timestamp=timestamp or fake.unix_time(),
            timestamp_type=timestamp_type or 1,
            key=key or fake.pystr(),
            value=value or fake.pystr().encode(),
            checksum=checksum or fake.pystr(),
            serialized_key_size=serialized_key_size or fake.pyint(max_value=10),
            serialized_value_size=serialized_value_size or fake.pyint(max_value=10),
        )

    return generate
