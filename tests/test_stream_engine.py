from unittest import mock

import pytest

from kstreams.clients import Consumer, Producer
from kstreams.conf import settings
from kstreams.engine import Stream, StreamEngine
from kstreams.exceptions import DuplicateStreamException

settings.configure(KAFKA_CONFIG_BOOTSTRAP_SERVERS=["localhost:9092"])


@pytest.mark.asyncio
async def test_add_streams(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream_engine.stream(topic, name="my-stream")
    async def stream(_):
        pass

    stream_instance = stream_engine.get_stream("my-stream")
    assert stream_instance == stream
    assert stream_instance.topics == [topic]


@pytest.mark.asyncio
async def test_add_stream_multiple_topics(stream_engine: StreamEngine):
    topics = ["local--hello-kpn", "local--hello-kpn-2"]

    @stream_engine.stream(topics, name="my-stream")
    async def stream(_):
        pass

    stream_instance = stream_engine.get_stream("my-stream")
    assert stream_instance == stream
    assert stream_instance.topics == topics


@pytest.mark.asyncio
async def test_add_stream_as_instance(stream_engine: StreamEngine):
    topics = ["local--hello-kpn", "local--hello-kpn-2"]

    class MyValueDeserializer:
        ...

    value_deserializer = MyValueDeserializer()

    async def processor(stream: Stream):
        pass

    my_stream = Stream(
        topics,
        name="my-stream",
        func=processor,
        value_deserializer=value_deserializer,
    )

    assert not stream_engine.get_stream("my-stream")

    stream_engine.add_stream(my_stream)
    stream_instance = stream_engine.get_stream("my-stream")
    assert stream_instance == my_stream
    assert stream_instance.topics == topics
    assert stream_instance.value_deserializer == value_deserializer

    # can not add a stream with the same name
    with pytest.raises(DuplicateStreamException):
        stream_engine.add_stream(
            Stream(
                "a-topic",
                name="my-stream",
                func=processor,
            )
        )


@pytest.mark.asyncio
async def test_add_existing_streams(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream_engine.stream(topic, name="my-stream")
    async def stream(_):
        pass

    with pytest.raises(DuplicateStreamException):

        @stream_engine.stream(topic, name="my-stream")
        async def my_stream(_):
            pass


@pytest.mark.asyncio
async def test_init_stop_streaming(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream_engine.stream(topic)
    async def stream(_):
        pass

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
        with mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
            await stream_engine.start()
            Consumer.start.assert_awaited()
            stream_engine._producer.start.assert_awaited()

            await stream_engine.stop()
            stream_engine._producer.stop.assert_awaited()
            Consumer.stop.assert_awaited()


@pytest.mark.asyncio
async def test_add_stream_custom_conf(stream_engine: StreamEngine):
    @stream_engine.stream(
        "local--hello-kpn",
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


@pytest.mark.asyncio
async def test_add_stream_as_generator(
    stream_engine: StreamEngine, consumer_record_factory
):
    @stream_engine.stream("local--hello-kpn")
    async def stream(consumer):
        async for cr in consumer:
            yield cr

    assert stream == stream_engine._streams[0]
    assert not stream.running

    cr = consumer_record_factory()

    async def getone(_):
        return cr

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, getone=getone):
        async with stream as stream_flow:
            # Now the stream should be running as we are in the context
            assert stream.running
            async for value in stream_flow:
                assert value == cr
                break

    # Now the stream is stopped because we left the context
    assert not stream.running
