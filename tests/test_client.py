from unittest.mock import Mock

import pytest

from kstreams import StreamEngine, TopicPartition
from kstreams.streams import Stream
from kstreams.test_utils import (
    TestConsumer,
    TestProducer,
    TestStreamClient,
    TopicManager,
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

        assert metadata.topic == topic
        assert metadata.partition == 0
        assert metadata.offset == 1

        # send another event and check that the offset was incremented
        metadata = await client.send(
            topic, value=b'{"message": "Hello world!"}', key="1"
        )
        assert metadata.offset == 2

        # send en event to a different partition
        metadata = await client.send(
            topic, value=b'{"message": "Hello world!"}', key="1", partition=2
        )

        # because it is a different partition the offset should be 1
        assert metadata.offset == 1


@pytest.mark.asyncio
async def test_streams_consume_events(stream_engine: StreamEngine):
    client = TestStreamClient(stream_engine)
    topic = "local--kstreams-consumer"
    event = b'{"message": "Hello world!"}'
    tp = TopicPartition(topic=topic, partition=0)
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
async def test_only_consume_topics_with_streams(stream_engine: StreamEngine):
    """
    The test creates a stream but no events are send to it,
    it means that the `TestStreamClient` should not wait for the topic to be consumed
    even thought the topic is exist.
    """
    client = TestStreamClient(stream_engine)
    topic = "local--kstreams"

    @stream_engine.stream("a-different-topic", name="my-stream")
    async def consume(stream):
        async for cr in stream:
            ...

    async with client:
        metadata = await client.send(
            topic, value=b'{"message": "Hello world!"}', key="1"
        )

        assert metadata.topic == topic
        assert metadata.partition == 0
        assert metadata.offset == 1


@pytest.mark.asyncio
async def test_topic_created(stream_engine: StreamEngine):
    topic_name = "local--kstreams"
    value = b'{"message": "Hello world!"}'
    key = "1"
    client = TestStreamClient(stream_engine)
    async with client:
        await client.send(topic_name, value=value, key=key)

        # check that the event was sent to a Topic
        consumer_record = await client.get_event(topic_name=topic_name)

        assert consumer_record.value == value
        assert consumer_record.key == key


@pytest.mark.asyncio
async def test_get_event_outside_context(stream_engine: StreamEngine):
    topic_name = "local--kstreams"
    value = b'{"message": "Hello world!"}'
    key = "1"
    client = TestStreamClient(stream_engine)
    async with client:
        # produce to events and consume only one in the client context
        await client.send(topic_name, value=value, key=key)
        await client.send(topic_name, value=value, key=key)

        # check that the event was sent to a Topic
        consumer_record = await client.get_event(topic_name=topic_name)
        assert consumer_record.value == value
        assert consumer_record.key == key

    with pytest.raises(ValueError) as exc:
        await client.get_event(topic_name=topic_name)

    assert (
        f"You might be trying to get the topic {topic_name} outside the "
        "`client async context` or trying to get an event from an empty "
        f"topic {topic_name}. Make sure that the code is inside the async context"
        "and the topic has events."
    ) == str(exc.value)


@pytest.mark.asyncio
async def test_clean_up_events(stream_engine: StreamEngine):
    topic_name = "local--kstreams-clean-up"
    value = b'{"message": "Hello world!"}'
    key = "1"
    client = TestStreamClient(stream_engine)

    async with client:
        # produce to events and consume only one in the client context
        await client.send(topic_name, value=value, key=key)
        await client.send(topic_name, value=value, key=key)

        # check that the event was sent to a Topic
        consumer_record = await client.get_event(topic_name=topic_name)
        assert consumer_record.value == value
        assert consumer_record.key == key

    # even though there is still one event in the topic
    # after leaving the context the topic should be empty
    assert not TopicManager.topics


@pytest.mark.asyncio
async def test_partitions_for_topic(stream_engine: StreamEngine):
    topic_name = "local--kstreams"
    value = b'{"message": "Hello world!"}'
    key = "1"
    client = TestStreamClient(stream_engine)

    @stream_engine.stream(topic_name, name="my-stream")
    async def consume(stream):
        async for cr in stream:
            ...

    async with client:
        # produce to events and consume only one in the client context
        await client.send(topic_name, value=value, key=key, partition=0)
        await client.send(topic_name, value=value, key=key, partition=2)
        await client.send(topic_name, value=value, key=key, partition=10)

    stream = stream_engine.get_stream("my-stream")
    assert stream.consumer.partitions_for_topic(topic_name) == set([0, 2, 10])


@pytest.mark.asyncio
async def test_end_offsets(stream_engine: StreamEngine):
    topic_name = "local--kstreams"
    value = b'{"message": "Hello world!"}'
    key = "1"
    client = TestStreamClient(stream_engine)

    @stream_engine.stream(topic_name, name="my-stream")
    async def consume(stream):
        async for cr in stream:
            ...

    async with client:
        # produce to events and consume only one in the client context
        await client.send(topic_name, value=value, key=key, partition=0)
        await client.send(topic_name, value=value, key=key, partition=0)
        await client.send(topic_name, value=value, key=key, partition=2)
        await client.send(topic_name, value=value, key=key, partition=10)

        topic_partitions = [
            TopicPartition(topic_name, 0),
            TopicPartition(topic_name, 2),
            TopicPartition(topic_name, 10),
        ]

        stream = stream_engine.get_stream("my-stream")
        assert (await stream.consumer.end_offsets(topic_partitions)) == {
            TopicPartition(topic="local--kstreams", partition=0): 2,
            TopicPartition(topic="local--kstreams", partition=2): 1,
            TopicPartition(topic="local--kstreams", partition=10): 1,
        }


@pytest.mark.asyncio
async def test_consumer_commit(stream_engine: StreamEngine):
    topic_name = "local--kstreams-consumer-commit"
    value = b'{"message": "Hello world!"}'
    name = "my-stream"
    key = "1"
    partition = 2
    tp = TopicPartition(
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
            record_metadata = await client.send(
                topic_name, partition=partition, value=value, key=key
            )
            assert record_metadata.partition == partition

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

    total_events = 2
    client = TestStreamClient(stream_engine)

    async with client:
        await produce(total_events)

        topic_1 = TopicManager.get(topics[0])
        topic_2 = TopicManager.get(topics[1])

        assert topic_1.total_events == total_events
        assert topic_2.total_events == total_events

        assert TopicManager.all_messages_consumed()
