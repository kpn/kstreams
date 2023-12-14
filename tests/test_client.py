import asyncio
from typing import Set
from unittest.mock import Mock, call

import pytest

from kstreams import ConsumerRecord, StreamEngine, TopicPartition, TopicPartitionOffset
from kstreams.streams import Stream
from kstreams.test_utils import (
    TestConsumer,
    TestProducer,
    TestStreamClient,
    TopicManager,
)

topic = "local--kstreams-consumer"
tp0 = TopicPartition(topic=topic, partition=0)
tp1 = TopicPartition(topic=topic, partition=1)
tp2 = TopicPartition(topic=topic, partition=2)


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
@pytest.mark.parametrize(
    "monitoring_enabled",
    (
        True,
        False,
    ),
)
async def test_send_event_with_test_client(
    stream_engine: StreamEngine, monitoring_enabled: bool
):
    topic = "local--kstreams"
    client = TestStreamClient(stream_engine, monitoring_enabled=monitoring_enabled)

    async with client:
        metadata = await client.send(
            topic, value=b'{"message": "Hello world!"}', key="1"
        )

        assert metadata.topic == topic
        assert metadata.partition == 0
        assert metadata.offset == 0

        # send another event and check that the offset was incremented
        metadata = await client.send(
            topic, value=b'{"message": "Hello world!"}', key="1"
        )
        assert metadata.offset == 1

        # send en event to a different partition
        metadata = await client.send(
            topic, value=b'{"message": "Hello world!"}', key="1", partition=2
        )

        # because it is a different partition the offset should be 1
        assert metadata.offset == 0


@pytest.mark.asyncio
async def test_streams_consume_events(stream_engine: StreamEngine):
    client = TestStreamClient(stream_engine)
    event = b'{"message": "Hello world!"}'
    save_to_db = Mock()

    @stream_engine.stream(topic, name="my-stream")
    async def consume(stream):
        async for cr in stream:
            save_to_db(cr.value)

    async with client:
        await client.send(topic, value=event, key="1")
        stream = stream_engine.get_stream("my-stream")
        assert stream.consumer.assignment() == [tp0, tp1, tp2]
        assert stream.consumer.last_stable_offset(tp0) == 0
        assert stream.consumer.highwater(tp0) == 1
        assert await stream.consumer.position(tp0) == 1

    # check that the event was consumed
    save_to_db.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_stream_consume_many(stream_engine: StreamEngine):
    event = b'{"message": "Hello world!"}'
    max_records = 2
    save_to_db = Mock()

    @stream_engine.stream(topic)
    async def stream(stream: Stream):
        while True:
            data = await stream.getmany(max_records=max_records)
            save_to_db(
                [
                    cr.value
                    for consumer_records_list in data.values()
                    for cr in consumer_records_list
                ]
            )

    client = TestStreamClient(stream_engine)
    async with client:
        await client.send(topic, value=event, key="1")
        await client.send(topic, value=event, key="1")

    save_to_db.assert_called_once_with([event for _ in range(0, max_records)])


@pytest.mark.asyncio
async def test_stream_consume_events_as_generator(stream_engine: StreamEngine):
    topic = "local--hello-kpn"
    event = b'{"message": "Hello world!"}'
    client = TestStreamClient(stream_engine)
    save_to_db = Mock()

    @stream_engine.stream(topic)
    async def my_stream(stream: Stream):
        async for cr in stream:
            save_to_db(cr.value)
            yield cr

    async with client:
        await client.send(topic, value=event, key="1")

        async with my_stream as processor:
            async for cr in processor:
                assert cr.value == event
                break

    # check that the event was consumed
    save_to_db.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_stream_func_with_cr(stream_engine: StreamEngine):
    client = TestStreamClient(stream_engine)
    event = b'{"message": "Hello world!"}'
    save_to_db = Mock()

    @stream_engine.stream(topic, name="my-stream")
    async def my_stream(cr: ConsumerRecord):
        save_to_db(cr.value)

    async with client:
        await client.send(topic, value=event, key="1")
        await client.send(topic, value=event, key="1")

    # check that the event was consumed
    save_to_db.assert_has_calls(
        [
            call(b'{"message": "Hello world!"}'),
            call(b'{"message": "Hello world!"}'),
        ]
    )


@pytest.mark.asyncio
async def test_stream_func_with_cr_and_stream(stream_engine: StreamEngine):
    tp = TopicPartition(topic=topic, partition=0)
    client = TestStreamClient(stream_engine)
    event = b'{"message": "Hello world!"}'
    save_to_db = Mock()

    @stream_engine.stream(topic, name="my-stream", enable_auto_commit=False)
    async def my_stream(cr: ConsumerRecord, stream: Stream):
        save_to_db(cr.value)
        # commit 100, just because we can
        await stream.commit({tp: 100})

    async with client:
        await client.send(topic, value=event, key="1")
        await client.send(topic, value=event, key="1")

        # give some time so the `commit` can finished
        await asyncio.sleep(1)
        assert await my_stream.consumer.committed(tp) == 100

    # check that the event was consumed
    save_to_db.assert_has_calls(
        [
            call(b'{"message": "Hello world!"}'),
            call(b'{"message": "Hello world!"}'),
        ]
    )


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
        assert metadata.offset == 0


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
    assert stream.consumer.partitions_for_topic(topic_name) == set([0, 1, 2, 10])


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
            await stream.commit({tp: cr.offset})

    client = TestStreamClient(stream_engine)
    async with client:
        for _ in range(0, total_events):
            record_metadata = await client.send(
                topic_name, partition=partition, value=value, key=key
            )
            assert record_metadata.partition == partition

    # check that everything was commited
    stream = stream_engine.get_stream(name)
    assert (await stream.consumer.committed(tp)) == total_events - 1


@pytest.mark.asyncio
async def test_e2e_example():
    """
    Test that events are produce by the engine and consumed by the streams
    """
    from examples.simple import event_store, produce, stream_engine

    client = TestStreamClient(stream_engine)

    async with client:
        metadata = await produce()

    assert event_store.total == 5
    assert metadata.partition == 0

    # check that all events has been consumed
    assert TopicManager.all_messages_consumed()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "monitoring_enabled",
    (
        True,
        False,
    ),
)
async def test_e2e_consume_multiple_topics(monitoring_enabled):
    from examples.consume_multiple_topics import produce, stream_engine, topics

    total_events = 2
    client = TestStreamClient(stream_engine, monitoring_enabled=monitoring_enabled)

    async with client:
        await produce(total_events)

        topic_1 = TopicManager.get(topics[0])
        topic_2 = TopicManager.get(topics[1])

        assert topic_1.total_events == total_events
        assert topic_2.total_events == total_events

        assert TopicManager.all_messages_consumed()


@pytest.mark.asyncio
async def test_streams_consume_events_with_initial_offsets(stream_engine: StreamEngine):
    client = TestStreamClient(stream_engine)
    event1 = b'{"message": "Hello world1!"}'
    event2 = b'{"message": "Hello world2!"}'
    process = Mock()

    tp0 = TopicPartition(topic=topic, partition=0)
    tp1 = TopicPartition(topic=topic, partition=1)
    tp2 = TopicPartition(topic=topic, partition=2)

    assignments: Set[TopicPartition] = set()
    assignments.update(
        tp0,
        tp1,
        tp2,
    )

    async with client:
        await client.send(topic, value=event1, partition=0)
        await client.send(topic, value=event1, partition=0)
        await client.send(topic, value=event1, partition=0)
        await client.send(topic, value=event2, partition=1)

        assert TopicManager.get(name=topic).size() == 4

        async def func_stream(consumer: Stream):
            async for cr in consumer:
                process(cr.value)

        stream: Stream = Stream(
            topics=topic,
            consumer_class=TestConsumer,
            name="my-stream",
            func=func_stream,
            initial_offsets=[
                # initial topic offset is -1
                TopicPartitionOffset(topic=topic, partition=0, offset=1),
                TopicPartitionOffset(topic=topic, partition=1, offset=0),
                TopicPartitionOffset(topic=topic, partition=2, offset=10),
            ],
        )
        stream_engine.add_stream(stream)
        await stream.start()

        # simulate partitions assigned on rebalance
        await stream.rebalance_listener.on_partitions_assigned(assigned=assignments)

        assert stream.consumer.assignment() == [tp0, tp1, tp2]

        assert stream.consumer.last_stable_offset(tp0) == 2
        assert stream.consumer.highwater(tp0) == 3
        assert await stream.consumer.position(tp0) == 3

        assert stream.consumer.last_stable_offset(tp1) == 0
        assert stream.consumer.highwater(tp1) == 1
        assert await stream.consumer.position(tp1) == 1

        # the position will be 0 as the offset 10 does not exist
        assert stream.consumer.last_stable_offset(tp2) == -1
        assert stream.consumer.highwater(tp2) == 0
        assert await stream.consumer.position(tp2) == 0

        # We moved to offset 1 on partition 0, then there are
        # 3 events in the Queue rather than 4
        assert TopicManager.get(name=topic).size() == 3

    process.assert_has_calls([call(event1), call(event1), call(event2)], any_order=True)
