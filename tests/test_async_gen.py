from typing import Callable
from unittest import mock

import pytest

from kstreams import ConsumerRecord, Stream, StreamEngine, TestStreamClient
from kstreams.clients import Consumer


@pytest.mark.asyncio
async def test_add_stream_as_generator(
    stream_engine: StreamEngine, consumer_record_factory: Callable[..., ConsumerRecord]
):
    @stream_engine.stream("local--hello-kpn")
    async def stream(cr: ConsumerRecord):
        yield cr

    assert stream == stream_engine._streams[0]
    assert not stream.running

    cr = consumer_record_factory()

    async def getone(_):
        return cr

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, getone=getone):
        # simulate an engine start
        await stream.start()

        # Now the stream should be running as we are in the context
        assert stream.running
        async for value in stream:
            assert value == cr
            break


@pytest.mark.asyncio
async def test_stream_consume_events_as_generator_cr_typing(
    stream_engine: StreamEngine,
):
    topic = "local--hello-kpn"
    event = b'{"message": "Hello world!"}'
    client = TestStreamClient(stream_engine)
    save_to_db = mock.Mock()

    @stream_engine.stream(topic)
    async def stream(cr: ConsumerRecord):
        save_to_db(cr.value)
        yield cr

    async with client:
        await client.send(topic, value=event, key="1")

        async with stream as stream_flow:
            async for cr in stream_flow:
                assert cr.value == event
                break

        # we left the stream context, so it has stopped
        assert not stream.running

    # check that the event was consumed
    save_to_db.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_stream_consume_events_as_generator_all_typing(
    stream_engine: StreamEngine,
):
    topic = "local--hello-kpn"
    event = b'{"message": "Hello world!"}'
    client = TestStreamClient(stream_engine)
    save_to_db = mock.Mock()

    @stream_engine.stream(topic)
    async def my_stream(cr: ConsumerRecord, stream: Stream):
        save_to_db(cr.value)
        # commit the event. Not ideal but we want to prove that works
        await stream.commit()
        yield cr

    async with client:
        await client.send(topic, value=event, key="1")

        async with my_stream as stream_flow:
            async for cr in stream_flow:
                assert cr.value == event
                break

        # we left the stream context, so it has stopped
        assert not my_stream.running

    # check that the event was consumed
    save_to_db.assert_called_once_with(event)
