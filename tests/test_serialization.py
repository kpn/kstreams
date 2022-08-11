import json
from typing import Any, Dict, Optional
from unittest import mock

import pytest

from kstreams import StreamEngine, consts
from kstreams.clients import Producer, aiokafka
from kstreams.streams import Stream
from kstreams.test_utils.test_utils import TestStreamClient
from kstreams.types import Headers
from kstreams.utils import encode_headers


class MySerializer:
    async def serialize(
        self,
        payload: Any,
        headers: Optional[Headers] = None,
        value_serializer_kwargs: Optional[Dict] = None,
    ) -> bytes:
        """
        Serialize paylod to json
        """
        value = json.dumps(payload)
        return value.encode()


class MyDeserializer:
    async def deserialize(
        self, consumer_record: aiokafka.structs.ConsumerRecord, **kwargs
    ) -> Any:
        data = consumer_record.value.decode()
        return json.loads(data)


@pytest.mark.asyncio
async def test_custom_serialization(stream_engine: StreamEngine, record_metadata):
    async def async_func():
        return record_metadata

    send = mock.AsyncMock(return_value=async_func())
    topic = "my-topic"
    value = {"message": "test"}
    headers = {
        "content-type": consts.APPLICATION_JSON,
    }

    # with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
    with mock.patch.multiple(Producer, start=mock.DEFAULT, send=send):
        await stream_engine.start()

        value_serializer = MySerializer()
        serialized_data = await value_serializer.serialize(value)

        metadata = await stream_engine.send(
            topic,
            value=value,
            headers=headers,
            value_serializer=value_serializer,
        )

        assert metadata
        send.assert_awaited_once_with(
            topic,
            value=serialized_data,
            key=None,
            partition=None,
            timestamp_ms=None,
            headers=encode_headers(headers),
        )


@pytest.mark.asyncio
async def test_custom_deserialization(
    stream_engine: StreamEngine, consumer_record_factory
):
    topic = "local--hello-kpn"
    payload = {"message": "test"}
    headers = {
        "content-type": consts.APPLICATION_JSON,
    }

    save_to_db = mock.Mock()

    @stream_engine.stream(topic, value_deserializer=MyDeserializer())
    async def hello_stream(stream: Stream):
        async for event in stream:
            save_to_db(event)

    async with TestStreamClient() as client:
        # encode payload with serializer
        await client.send(
            topic,
            value=payload,
            headers=headers,
            key="1",
            value_serializer=MySerializer(),
        )

    # The payload as been encoded with json,
    # we expect that the mock has been called with the original value (decoded)
    save_to_db.assert_called_once_with(payload)
