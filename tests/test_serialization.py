import json
from typing import Any, Dict, Optional
from unittest import mock

import pytest

from kstreams import ConsumerRecord, StreamEngine, consts
from kstreams.batch import BatchEvent
from kstreams.clients import Producer
from kstreams.streams import Stream
from kstreams.test_utils.test_utils import TestStreamClient
from kstreams.types import Headers
from kstreams.utils import encode_headers


class MyJsonSerializer:
    async def serialize(
        self,
        payload: Any,
        headers: Optional[Headers] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> bytes:
        """
        Serialize paylod to json
        """
        value = json.dumps(payload)
        return value.encode()


class MyJsonDeserializer:
    async def deserialize(self, consumer_record: ConsumerRecord, **kwargs) -> Any:
        if consumer_record.value is not None:
            data = consumer_record.value.decode()
            return json.loads(data)


@pytest.mark.asyncio
async def test_send_global_serializer(stream_engine: StreamEngine, record_metadata):
    serializer = MyJsonSerializer()
    stream_engine.serializer = serializer

    async def async_func():
        return record_metadata

    send = mock.AsyncMock(return_value=async_func())
    topic = "my-topic"
    value = {"message": "test"}
    headers = {
        "content-type": consts.APPLICATION_JSON,
    }

    with mock.patch.multiple(Producer, start=mock.DEFAULT, send=send):
        await stream_engine.start()
        metadata = await stream_engine.send(
            topic,
            value=value,
            headers=headers,
        )

        assert metadata
        send.assert_awaited_once_with(
            topic,
            value='{"message": "test"}'.encode(),
            key=None,
            partition=None,
            timestamp_ms=None,
            headers=encode_headers(headers),
        )


@pytest.mark.asyncio
async def test_send_custom_serialization(stream_engine: StreamEngine, record_metadata):
    assert stream_engine.serializer is None

    async def async_func():
        return record_metadata

    send = mock.AsyncMock(return_value=async_func())
    topic = "my-topic"
    value = {"message": "test"}
    headers = {
        "content-type": consts.APPLICATION_JSON,
    }

    with mock.patch.multiple(Producer, start=mock.DEFAULT, send=send):
        await stream_engine.start()
        metadata = await stream_engine.send(
            topic,
            value=value,
            headers=headers,
            serializer=MyJsonSerializer(),
        )

        assert metadata
        send.assert_awaited_once_with(
            topic,
            value='{"message": "test"}'.encode(),
            key=None,
            partition=None,
            timestamp_ms=None,
            headers=encode_headers(headers),
        )


@pytest.mark.asyncio
async def test_not_serialize_value(stream_engine: StreamEngine, record_metadata):
    # even if a serializer is set, we can send the value as is
    stream_engine.serializer = MyJsonSerializer()

    async def async_func():
        return record_metadata

    send = mock.AsyncMock(return_value=async_func())
    topic = "my-topic"
    value = {"message": "test"}

    with mock.patch.multiple(Producer, start=mock.DEFAULT, send=send):
        await stream_engine.start()
        metadata = await stream_engine.send(
            topic,
            value=value,
            serializer=None,
        )

        assert metadata

        # The value is not serialized, it is send as is
        # which is will aiokafka to crash because it expects bytes not dict
        send.assert_awaited_once_with(
            topic,
            value={"message": "test"},
            key=None,
            partition=None,
            timestamp_ms=None,
            headers=None,
        )


@pytest.mark.asyncio
async def test_send_many_global_serializer(
    stream_engine: StreamEngine, record_metadata
):
    serializer = MyJsonSerializer()
    stream_engine.serializer = serializer
    total_events = 2

    async def async_func():
        return record_metadata

    send_batch = mock.AsyncMock(return_value=async_func())
    topic = "my-topic"
    value = {"message": "test"}
    headers = {
        "content-type": consts.APPLICATION_JSON,
    }

    # use python dict as value
    batch_events = [
        BatchEvent(value=value, key="1", headers=headers) for _ in range(total_events)
    ]

    batch_mock = mock.Mock()

    def create_batch(_):
        return batch_mock

    with mock.patch.multiple(
        Producer, start=mock.DEFAULT, send_batch=send_batch, create_batch=create_batch
    ):
        await stream_engine.start()
        metadata = await stream_engine.send_many(
            topic,
            partition=0,
            batch_events=batch_events,
        )

        assert metadata

    send_batch.assert_awaited_with(
        mock.ANY,
        topic,
        partition=0,
    )

    batch_mock.append.assert_has_calls(
        [
            mock.call(
                key="1",
                # use python dict was converted to json bytes
                value='{"message": "test"}'.encode(),
                headers=encode_headers(headers),
                timestamp=None,
            )
            for _ in range(total_events)
        ]
    )


@pytest.mark.asyncio
async def test_send_many_custom_serialization(
    stream_engine: StreamEngine, record_metadata
):
    assert stream_engine.serializer is None

    async def async_func():
        return record_metadata

    total_events = 2
    send_batch = mock.AsyncMock(return_value=async_func())
    topic = "my-topic"
    value = {"message": "test"}
    headers = {
        "content-type": consts.APPLICATION_JSON,
    }

    # use python dict as value
    batch_events = [
        BatchEvent(value=value, key="1", headers=headers) for _ in range(total_events)
    ]

    batch_mock = mock.Mock()

    def create_batch(_):
        return batch_mock

    with mock.patch.multiple(
        Producer, start=mock.DEFAULT, send_batch=send_batch, create_batch=create_batch
    ):
        await stream_engine.start()
        metadata = await stream_engine.send_many(
            topic,
            partition=0,
            batch_events=batch_events,
            serializer=MyJsonSerializer(),  # use custom serializer
        )

        assert metadata

    send_batch.assert_awaited_with(
        mock.ANY,
        topic,
        partition=0,
    )

    batch_mock.append.assert_has_calls(
        [
            mock.call(
                key="1",
                # use python dict was converted to json bytes
                value='{"message": "test"}'.encode(),
                headers=encode_headers(headers),
                timestamp=None,
            )
            for _ in range(total_events)
        ]
    )


@pytest.mark.asyncio
async def test_not_serialize_value_send_many(
    stream_engine: StreamEngine, record_metadata
):
    # even if a serializer is set, we can send the value as is
    stream_engine.serializer = MyJsonSerializer()

    async def async_func():
        return record_metadata

    total_events = 2
    send_batch = mock.AsyncMock(return_value=async_func())
    topic = "my-topic"
    value = b'{"message": "test"}'
    # use python dict as value
    batch_events = [BatchEvent(value=value) for _ in range(total_events)]

    batch_mock = mock.Mock()

    def create_batch(_):
        return batch_mock

    with mock.patch.multiple(
        Producer, start=mock.DEFAULT, send_batch=send_batch, create_batch=create_batch
    ):
        await stream_engine.start()
        metadata = await stream_engine.send_many(
            topic,
            partition=0,
            batch_events=batch_events,
            serializer=None,  # send value as it is
        )

        assert metadata

    send_batch.assert_awaited_with(
        mock.ANY,
        topic,
        partition=0,
    )

    batch_mock.append.assert_has_calls(
        [
            mock.call(
                key=None,
                value=value,
                headers=None,
                timestamp=None,
            )
            for _ in range(total_events)
        ]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value, headers",
    (
        (
            {"message": "test"},
            {"content-type": consts.APPLICATION_JSON, "event-type": "hello-world"},
        ),
        (None, {"event-type": "delete-hello-world"}),
    ),
)
async def test_consume_global_deserialization(
    stream_engine: StreamEngine, value: Optional[Dict], headers: Dict
):
    """
    Even though deserialzers are deprecated, we still support them.
    """
    topic = "local--hello-kpn"
    stream_engine.deserializer = MyJsonDeserializer()
    client = TestStreamClient(stream_engine)
    save_to_db = mock.Mock()

    @stream_engine.stream(topic)
    async def hello_stream(stream: Stream):
        async for event in stream:
            save_to_db(event)

    async with client:
        # encode payload with serializer
        await client.send(
            topic,
            value=value,
            headers=headers,
            key="1",
            serializer=MyJsonSerializer(),
        )

    # The payload as been encoded with json,
    # we expect that the mock has been called with the original value (decoded)
    save_to_db.assert_called_once_with(value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value, headers",
    (
        (
            {"message": "test"},
            {"content-type": consts.APPLICATION_JSON, "event-type": "hello-world"},
        ),
        (None, {"event-type": "delete-hello-world"}),
    ),
)
async def test_consume_custom_deserialization(
    stream_engine: StreamEngine, value: Optional[Dict], headers: Dict
):
    assert stream_engine.deserializer is None
    topic = "local--hello-kpn"
    client = TestStreamClient(stream_engine)

    save_to_db = mock.Mock()

    @stream_engine.stream(topic, deserializer=MyJsonDeserializer())
    async def hello_stream(stream: Stream):
        async for event in stream:
            save_to_db(event)

    async with client:
        # encode payload with serializer
        await client.send(
            topic,
            value=value,
            headers=headers,
            key="1",
            serializer=MyJsonSerializer(),
        )

    # The payload as been encoded with json,
    # we expect that the mock has been called with the original value (decoded)
    save_to_db.assert_called_once_with(value)
