from kstreams.test_utils import (
    TestConsumer,
    TestProducer,
    TestStreamClient,
    TopicManager,
)
from unittest.mock import Mock, patch

import pytest


@pytest.mark.asyncio
async def test_engine_clients():
    client = TestStreamClient()

    async with client as test_client:
        assert test_client.stream_engine.consumer_class is TestConsumer
        assert test_client.stream_engine.producer_class is TestProducer

    # after leaving the context, everything should go to normal
    assert client.stream_engine.consumer_class is client.consumer_class
    assert client.stream_engine.producer_class is client.producer_class


@pytest.mark.asyncio
async def test_send_event_with_test_client():
    topic = "local--kstreams"

    async with TestStreamClient() as test_client:
        metadata = await test_client.send(
            topic, value=b'{"message": "Hello world!"}', key="1"
        )
        current_offset = metadata.offset
        assert metadata.topic == topic

        # send another event and check that the offset was incremented
        metadata = await test_client.send(
            topic, value=b'{"message": "Hello world!"}', key="1"
        )
        assert metadata.offset == current_offset + 1


@pytest.mark.asyncio
async def test_streams_consume_events():
    from examples.simple import stream_engine

    topic = "local--kstreams-2"
    event = b'{"message": "Hello world!"}'
    save_to_db = Mock()

    @stream_engine.stream(topic)
    async def consume(stream):
        async for cr in stream:
            save_to_db(cr.value)

    async with TestStreamClient() as test_client:
        await test_client.send(topic, value=event, key="1")

    # check that the event was consumed
    save_to_db.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_topic_created():
    topic = "local--kstreams"
    async with TestStreamClient() as test_client:
        await test_client.send(topic, value=b'{"message": "Hello world!"}', key="1")

    assert TopicManager.get_topic(topic)


@pytest.mark.asyncio
async def test_e2e_example():
    """
    Test that events are produce by the engine and consumed by the streams
    """
    from examples.simple import produce

    with patch("examples.simple.on_consume") as on_consume, patch(
        "examples.simple.on_produce"
    ) as on_produce:
        async with TestStreamClient():
            await produce()

    on_produce.call_count == 5
    on_consume.call_count == 5

    # check that all events has been consumed
    assert TopicManager.all_messages_consumed()


@pytest.mark.asyncio
async def test_e2e_consume_multiple_topics():
    from examples.consume_multiple_topics import produce, topics

    events_per_topic = 2

    async with TestStreamClient():
        await produce(events_per_topic=events_per_topic)

    topic_1 = TopicManager.get_topic(topics[0])
    topic_2 = TopicManager.get_topic(topics[1])

    assert topic_1.total_messages == events_per_topic
    assert topic_2.total_messages == events_per_topic

    assert TopicManager.all_messages_consumed()
