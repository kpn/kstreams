import asyncio
from unittest import mock

import pytest

from kstreams.clients import Consumer, Producer
from kstreams.engine import Stream, StreamEngine
from kstreams.exceptions import DuplicateStreamException, EngineNotStartedException
from kstreams.streams import stream


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

    class MyDeserializer:
        ...

    deserializer = MyDeserializer()

    async def processor(stream: Stream):
        pass

    my_stream = Stream(
        topics,
        name="my-stream",
        func=processor,
        deserializer=deserializer,
    )

    assert not stream_engine.get_stream("my-stream")

    stream_engine.add_stream(my_stream)
    stream_instance = stream_engine.get_stream("my-stream")
    assert stream_instance == my_stream
    assert stream_instance.topics == topics
    assert stream_instance.deserializer == deserializer

    # can not add a stream with the same name
    with pytest.raises(DuplicateStreamException):
        stream_engine.add_stream(Stream("a-topic", name="my-stream", func=processor))


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
async def test_start_stop_streaming(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream_engine.stream(topic)
    async def stream(_):
        pass

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
        with mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
            await stream_engine.start()
            Consumer.start.assert_awaited()
            stream_engine._producer.start.assert_awaited()

            await asyncio.sleep(0)  # Allow stream coroutine to run once

            await stream_engine.stop()
            stream_engine._producer.stop.assert_awaited()
            Consumer.stop.assert_awaited()


@pytest.mark.asyncio
async def test_recreate_consumer_on_re_tart_stream(stream_engine: StreamEngine):
    with mock.patch("kstreams.clients.aiokafka.AIOKafkaConsumer.start"):
        topic_name = "local--kstreams"
        stream_name = "my-stream"

        @stream_engine.stream(topic_name, name=stream_name)
        async def consume(stream):
            async for _ in stream:
                ...

        stream = stream_engine.get_stream(stream_name)
        await stream.start()
        consumer = stream.consumer
        await stream.stop()
        await stream.start()
        assert consumer is not stream.consumer


@pytest.mark.asyncio
async def test_engine_not_started(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    # TODO: create a new Engine instance after remove the singlenton and
    # not do `stream_engine._producer = None`
    stream_engine._producer = None

    with pytest.raises(EngineNotStartedException):
        await stream_engine.send(topic, value=b"1")


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


@pytest.mark.asyncio
async def test_sream_decorator(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream(topic)
    async def streaming_fn(_):
        pass

    stream_engine.add_stream(streaming_fn)

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
        with mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
            await stream_engine.start()
            Consumer.start.assert_awaited()
            stream_engine._producer.start.assert_awaited()

            await stream_engine.stop()
            stream_engine._producer.stop.assert_awaited()
            Consumer.stop.assert_awaited()


@pytest.mark.asyncio
async def test_sream_decorates_properly(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream(topic)
    async def streaming_fn(_):
        """text from func"""

    assert streaming_fn.__name__ == "streaming_fn"
    assert streaming_fn.__doc__ == "text from func"
