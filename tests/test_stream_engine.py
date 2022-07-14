from kstreams.clients import Consumer, Producer
from kstreams.conf import settings
from kstreams.engine import StreamEngine
from kstreams.exceptions import DuplicateStreamException
from unittest import mock

import pytest

settings.configure(KAFKA_CONFIG_BOOTSTRAP_SERVERS=["localhost:9092"])


@pytest.mark.asyncio
async def test_add_streams(stream_engine: StreamEngine):
    topic = "dev-kpn-des--hello-kpn"

    @stream_engine.stream(topic, name="my-stream")
    async def stream(_):
        pass

    stream_instance = stream_engine.get_stream("my-stream")
    assert stream_instance.func == stream
    assert stream_instance.topics == [topic]


@pytest.mark.asyncio
async def test_add_stream_multiple_topics(stream_engine: StreamEngine):
    topics = ["dev-kpn-des--hello-kpn", "dev-kpn-des--hello-kpn-2"]

    @stream_engine.stream(topics, name="my-stream")
    async def stream(_):
        pass

    stream_instance = stream_engine.get_stream("my-stream")
    assert stream_instance.func == stream
    assert stream_instance.topics == topics


@pytest.mark.asyncio
async def test_add_existing_streams(stream_engine: StreamEngine):
    topic = "dev-kpn-des--hello-kpn"

    @stream_engine.stream(topic, name="my-stream")
    async def stream(_):
        pass

    with pytest.raises(DuplicateStreamException):

        @stream_engine.stream(topic, name="my-stream")
        async def my_stream(_):
            pass


@pytest.mark.asyncio
async def test_init_stop_streaming(stream_engine: StreamEngine):
    topic = "dev-kpn-des--hello-kpn"

    @stream_engine.stream(topic)
    async def stream(_):
        pass

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
        with mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
            await stream_engine.init_streaming()
            Consumer.start.assert_awaited()
            stream_engine._producer.start.assert_awaited()

            await stream_engine.stop_streaming()
            stream_engine._producer.stop.assert_awaited()
            Consumer.stop.assert_awaited()


@pytest.mark.asyncio
async def test_add_stream_custom_conf(stream_engine: StreamEngine):
    @stream_engine.stream(
        "dev-kpn-des--hello-kpn",
        name="stream-hello-kpn",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    async def stream(_):
        pass

    stream_instance = stream_engine.get_stream("stream-hello-kpn")

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
        with mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
            await stream_engine.start_streams()

            assert stream_instance.consumer._auto_offset_reset == "earliest"
            assert not stream_instance.consumer._enable_auto_commit
