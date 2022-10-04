from unittest.mock import Mock

import pytest

from kstreams import StreamEngine
from kstreams.streams import Stream
from kstreams.test_utils import (
    TestConsumer,
    TestProducer,
    TestStreamClient,
    TopicManager,
    structs,
)


@pytest.mark.asyncio
async def test_engine_clients(stream_engine: StreamEngine):
    client = TestStreamClient(stream_engine)

    async with client:
        assert stream_engine.consumer_class is TestConsumer
        assert stream_engine.producer_class is TestProducer

    # after leaving the context, everything should go to normal
    assert client.stream_engine.consumer_class is client.consumer_class
    assert client.stream_engine.producer_class is client.producer_class


@pytest.mark.asyncio
async def test_send_event_with_test_client(stream_engine: StreamEngine):
    topic = "local--kstreams"
    client = TestStreamClient(stream_engine)

    async with client:
        metadata = await client.send(
            topic, value=b'{"message": "Hello world!"}', key="1"
        )
        current_offset = metadata.offset
        assert metadata.topic == topic

        # send another event and check that the offset was incremented
        metadata = await client.send(
            topic, value=b'{"message": "Hello world!"}', key="1"
        )
        assert metadata.offset == current_offset + 1


@pytest.mark.asyncio
async def test_streams_consume_events(stream_engine: StreamEngine):
    from examples.simple import stream_engine

    client = TestStreamClient(stream_engine)
    topic = "local--kstreams-2"
    event = b'{"message": "Hello world!"}'
    tp = structs.TopicPartition(topic=topic, partition=1)
    save_to_db = Mock()

    @stream_engine.stream(topic, name="my-stream")
    async def consume(stream):
        async for cr in stream:
            save_to_db(cr.value)

    async with client:
        await client.send(topic, value=event, key="1")
        stream = stream_engine.get_stream("my-stream")
        assert stream.consumer.assignment() == [tp]
        assert stream.consumer.last_stable_offset(tp) == 1
        assert stream.consumer.highwater(tp) == 1
        assert await stream.consumer.position(tp) == 1

    # check that the event was consumed
    save_to_db.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_topic_created(stream_engine: StreamEngine):
    topic_name = "local--kstreams"
    value = b'{"message": "Hello world!"}'
    key = "1"
    client = TestStreamClient(stream_engine)
    async with client:
        await client.send(topic_name, value=value, key=key)

    # check that the event was sent to a Topic
    topic = client.get_topic(topic_name)
    consumer_record = await topic.get()
    assert consumer_record.value == value
    assert consumer_record.key == key


@pytest.mark.asyncio
async def test_consumer_commit(stream_engine: StreamEngine):
    topic_name = "local--kstreams-marcos"
    value = b'{"message": "Hello world!"}'
    name = "my-stream"
    key = "1"
    partition = 2
    tp = structs.TopicPartition(
        topic=topic_name,
        partition=partition,
    )
    total_events = 10

    @stream_engine.stream(topic_name, name=name)
    async def my_stream(stream: Stream):
        async for cr in stream:
            await stream.consumer.commit({tp: cr.offset})

    client = TestStreamClient(stream_engine)
    async with client:
        for _ in range(0, total_events):
            await client.send(topic_name, partition=partition, value=value, key=key)

    # check that everything was commited
    stream = stream_engine.get_stream(name)
    assert (await stream.consumer.committed(tp)) == total_events


@pytest.mark.asyncio
async def test_e2e_example():
    """
    Test that events are produce by the engine and consumed by the streams
    """
    from examples.simple import event_store, produce, stream_engine

    client = TestStreamClient(stream_engine)

    async with client:
        await produce()

    assert event_store.total == 5

    # check that all events has been consumed
    assert TopicManager.all_messages_consumed()


@pytest.mark.asyncio
async def test_e2e_consume_multiple_topics():
    from examples.consume_multiple_topics import produce, stream_engine, topics

    events_per_topic = 2
    client = TestStreamClient(stream_engine)

    async with client:
        await produce(events_per_topic=events_per_topic)

    topic_1 = TopicManager.get(topics[0])
    topic_2 = TopicManager.get(topics[1])

    assert topic_1.total_messages == events_per_topic
    assert topic_2.total_messages == events_per_topic

    assert TopicManager.all_messages_consumed()
