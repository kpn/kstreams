import asyncio
from unittest import mock

import pytest

from kstreams import ConsumerRecord, StreamEngine, TestStreamClient
from kstreams.consts import StreamErrorPolicy


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stream_options", ({}, {"error_policy": StreamErrorPolicy.STOP})
)
async def test_stop_stream_error_policy(stream_engine: StreamEngine, stream_options):
    event = b'{"message": "Hello world!"}'
    topic = "kstrems--local"
    topic_two = "kstrems--local-two"
    save_to_db = mock.Mock()
    client = TestStreamClient(stream_engine)

    @stream_engine.stream(topic, **stream_options)
    async def my_stream(cr: ConsumerRecord):
        save_to_db(value=cr.value, key=cr.key)
        raise ValueError("Crashing Stream...")

    @stream_engine.stream(topic_two)
    async def my_stream_two(cr: ConsumerRecord):
        save_to_db(cr.value)

    async with client:
        await client.send(topic, value=event, key="1")

        # sleep so the event loop can switch context
        await asyncio.sleep(1e-10)

        await client.send(topic_two, value=event, key="2")

        # Streams was stopped before leaving the context
        assert not my_stream.running

        # Streams still running before leaving the context
        assert my_stream_two.running

    # check that mock was called by both Streams
    save_to_db.assert_has_calls(
        [
            mock.call(value=b'{"message": "Hello world!"}', key="1"),
            mock.call(b'{"message": "Hello world!"}'),
        ]
    )


@pytest.mark.asyncio
async def test_stop_engine_error_policy(stream_engine: StreamEngine):
    event = b'{"message": "Hello world!"}'
    topic = "kstrems--local"
    topic_two = "kstrems--local-two"
    save_to_db = mock.Mock()
    client = TestStreamClient(stream_engine)

    @stream_engine.stream(topic, error_policy=StreamErrorPolicy.STOP_ENGINE)
    async def my_stream(cr: ConsumerRecord):
        raise ValueError("Crashing Stream...")

    @stream_engine.stream(topic_two)
    async def my_stream_two(cr: ConsumerRecord):
        save_to_db(cr.value)

    async with client:
        # send event and crash the first Stream, then the second one
        # should be stopped because of StreamErrorPolicy.STOP_ENGINE
        await client.send(topic, value=event, key="1")

        # Send an event to the second Stream, it should be consumed
        # as the Stream has been stopped
        await client.send(topic_two, value=event, key="1")

        # Both streams are stopped before leaving the context
        assert not my_stream.running
        assert not my_stream_two.running

    # check that the event was consumed only once.
    # The StreamEngine must wait for graceful shutdown
    save_to_db.assert_called_once_with(b'{"message": "Hello world!"}')


@pytest.mark.asyncio
async def test_stop_application_error_policy(stream_engine: StreamEngine):
    event = b'{"message": "Hello world!"}'
    topic = "kstrems--local"
    topic_two = "kstrems--local-two"
    save_to_db = mock.Mock()
    client = TestStreamClient(stream_engine)

    with mock.patch("signal.raise_signal"):

        @stream_engine.stream(topic, error_policy=StreamErrorPolicy.STOP_APPLICATION)
        async def my_stream(cr: ConsumerRecord):
            raise ValueError("Crashing Stream...")

        @stream_engine.stream(topic_two)
        async def my_stream_two(cr: ConsumerRecord):
            save_to_db(cr.value)

        async with client:
            # send event and crash the first Stream, then the second one
            # should be stopped because of StreamErrorPolicy.STOP_ENGINE
            await client.send(topic, value=event, key="1")

            # Send an event to the second Stream, it should be consumed
            # as the Stream has been stopped
            await client.send(topic_two, value=event, key="1")

            # Both streams are stopped before leaving the context
            assert not my_stream.running
            assert not my_stream_two.running

        # check that the event was consumed only once.
        # The StreamEngine must wait for graceful shutdown
        save_to_db.assert_called_once_with(b'{"message": "Hello world!"}')


@pytest.mark.asyncio
async def test_restart_stream_error_policy(stream_engine: StreamEngine):
    event = b'{"message": "Hello world!"}'
    topic = "kstrems--local-kskss"
    save_to_db = mock.Mock()
    client = TestStreamClient(stream_engine)

    @stream_engine.stream(topic, error_policy=StreamErrorPolicy.RESTART)
    async def my_stream(cr: ConsumerRecord):
        if cr.key == "1":
            raise ValueError("Crashing Stream...")
        save_to_db(value=cr.value, key=cr.key)

    async with client:
        await client.send(topic, value=event, key="2")

        # send event to crash the Stream but it should be restarted
        # because of StreamErrorPolicy.RESTART
        await client.send(topic, value=event, key="1")

        # Send another event to make sure that the Stream is not dead
        await client.send(topic, value=event, key="3")

        # sleep so the event loop can switch context
        await asyncio.sleep(1e-10)

        # Both streams are stopped before leaving the context
        assert my_stream.running

    # check that the Stream has consumed two events
    save_to_db.assert_has_calls(
        [
            mock.call(value=b'{"message": "Hello world!"}', key="2"),
            mock.call(value=b'{"message": "Hello world!"}', key="3"),
        ]
    )
