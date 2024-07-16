import asyncio
from typing import Callable
from unittest import mock

import pytest

from kstreams import ConsumerRecord
from kstreams.clients import Consumer, Producer
from kstreams.engine import Stream, StreamEngine
from kstreams.exceptions import DuplicateStreamException, EngineNotStartedException


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
async def test_remove_existing_stream(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    class MyDeserializer:
        ...

    deserializer = MyDeserializer()

    async def processor(stream: Stream):
        pass

    my_stream = Stream(
        topic,
        name="my-stream",
        func=processor,
        deserializer=deserializer,
    )

    stream_engine.add_stream(my_stream)
    assert len(stream_engine._streams) == 1
    await stream_engine.remove_stream(my_stream)
    assert len(stream_engine._streams) == 0


@pytest.mark.asyncio
async def test_remove_missing_stream(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    class MyDeserializer:
        ...

    deserializer = MyDeserializer()

    async def processor(stream: Stream):
        pass

    my_stream = Stream(
        topic,
        name="my-stream",
        func=processor,
        deserializer=deserializer,
    )

    with pytest.raises(ValueError):
        await stream_engine.remove_stream(my_stream)


@pytest.mark.asyncio
async def test_remove_existing_stream_stops_stream(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    class MyDeserializer:
        ...

    deserializer = MyDeserializer()

    async def processor(stream: Stream):
        pass

    my_stream = Stream(
        topic,
        name="my-stream",
        func=processor,
        deserializer=deserializer,
    )
    stream_engine.add_stream(my_stream)

    with mock.patch.multiple(Stream, start=mock.DEFAULT, stop=mock.DEFAULT):
        await stream_engine.remove_stream(my_stream)
        Stream.stop.assert_awaited()


@pytest.mark.asyncio
async def test_start_stop_stream_engine(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream_engine.stream(topic)
    async def stream(_):
        ...

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
        with mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
            await stream_engine.start()
            stream_engine._producer.start.assert_awaited()

            await asyncio.sleep(0)  # Allow stream coroutine to run once
            Consumer.start.assert_awaited()

            await stream_engine.stop()
            stream_engine._producer.stop.assert_awaited()
            Consumer.stop.assert_awaited()


@pytest.mark.asyncio
async def test_wait_for_streams_before_stop(
    stream_engine: StreamEngine, consumer_record_factory: Callable[..., ConsumerRecord]
):
    topic = "local--hello-kpn"
    value = b"Hello world"
    save_to_db = mock.AsyncMock()

    async def getone(_):
        return consumer_record_factory(value=value)

    @stream_engine.stream(topic)
    async def stream(cr: ConsumerRecord):
        # Use 5 seconds sleep to simulate a super slow event processing
        await asyncio.sleep(5)
        await save_to_db(cr.value)

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        stop=mock.DEFAULT,
        getone=getone,
    ), mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
        await stream_engine.start()
        await asyncio.sleep(0)  # Allow stream coroutine to run once

        # stop engine immediately, this should not break the streams
        # and it should wait until the event is processed.
        await stream_engine.stop()
        Consumer.stop.assert_awaited()
        save_to_db.assert_awaited_once_with(value)


@pytest.mark.asyncio
async def test_engine_not_started(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    # TODO: create a new Engine instance after remove the singlenton and
    # not do `stream_engine._producer = None`
    stream_engine._producer = None

    with pytest.raises(EngineNotStartedException):
        await stream_engine.send(topic, value=b"1")
