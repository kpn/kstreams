import asyncio
import contextlib
import importlib
from typing import Set
from unittest.mock import Mock, call

import pytest

from kstreams import ConsumerRecord, StreamEngine, TopicPartition, TopicPartitionOffset
from kstreams.streams import Stream
from kstreams.structs import BatchEvent
from kstreams.test_utils import (
    TestConsumer,
    TestProducer,
    TestStreamClient,
    TopicManager,
)
from tests import TimeoutErrorException

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
    assert client.stream_engine.consumer_class is client.engine_consumer_class
    assert client.stream_engine.producer_class is client.engine_producer_class


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
async def test_send_many_events_with_test_client(stream_engine: StreamEngine):
    topic = "local--kstreams"
    client = TestStreamClient(stream_engine)

    batch_events = [
        BatchEvent(
            value=f"Hello world {str(id)}!".encode(),
            key=str(id),
        )
        for id in range(5)
    ]

    async with client:
        metadata = await client.send_many(topic, partition=0, batch_events=batch_events)

        assert metadata.topic == topic
        assert metadata.partition == 0
        assert metadata.offset == 4


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
        assert stream is not None
        assert stream.consumer is not None
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
        assert my_stream.consumer is not None
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
async def test_consume_events_topics_by_pattern(stream_engine: StreamEngine):
    """
    This test shows the possibility to subscribe to multiple topics using a pattern
    """
    pattern = "^dev--customer-.*$"
    customer_invoice_topic = "dev--customer-invoice"
    customer_profile_topic = "dev--customer-profile"
    invoice_event = b"invoice-1"
    profile_event = b"profile-1"
    customer_id = "1"

    client = TestStreamClient(
        stream_engine, topics=[customer_invoice_topic, customer_profile_topic]
    )

    @stream_engine.stream(topics=pattern, subscribe_by_pattern=True)
    async def stream(cr: ConsumerRecord):
        if cr.topic == customer_invoice_topic:
            assert cr.value == invoice_event
        elif cr.topic == customer_profile_topic:
            assert cr.value == profile_event
        else:
            raise ValueError(f"Invalid topic {cr.topic}")

    async with client:
        await client.send(customer_invoice_topic, value=invoice_event, key=customer_id)
        await client.send(customer_profile_topic, value=profile_event, key=customer_id)

        # give some time to consume all the events
        await asyncio.sleep(0.1)
        assert TopicManager.all_messages_consumed()


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
async def test_client_create_extra_user_topics(stream_engine: StreamEngine):
    """
    Test that the topics defined by the end user are created before
    the test cycle has started.

    Note: the topics to be created should not have a `stream` associated,
    otherwise it would not make any sense.
    """
    value = b'{"message": "Hello world!"}'
    key = "my-key"
    extra_topic = "local--kstreams-statistics-consumer"

    @stream_engine.stream(topic, name="my-stream")
    async def consume(cr: ConsumerRecord):
        # produce event to a topic that has not a stream associated
        await client.send(extra_topic, value=cr.value, key=cr.key)

    client = TestStreamClient(stream_engine, topics=[extra_topic])
    async with client:
        # produce to the topic that has a stream. Then, inside the stream
        await client.send(topic, value=value, key=key)

        # get the event from a topic that has not a `stream`
        event = await client.get_event(topic_name=extra_topic)
        assert event.value == value
        assert event.key == key


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
async def test_exit_context_after_stream_crashing(stream_engine: StreamEngine):
    """
    Check that when a Stream crashes after consuming 1 event it stops,
    then the TestStreamClient should be able to leave the context even though
    more events were send to the topic.
    """
    topic = "kstreams--local"
    value = b'{"message": "Hello world!"}'
    client = TestStreamClient(stream_engine=stream_engine)

    @stream_engine.stream(topic)
    async def stream(cr: ConsumerRecord):
        raise ValueError(f"Invalid topic {cr.topic}")

    async with client:
        topic_instance = TopicManager.get(topic)
        await client.send(topic, value=value)

        # Allow the event loop to switch context
        await asyncio.sleep(1e-10)
        assert topic_instance.size() == 0

        # send another event to the topic, but the
        # stream is already daed
        await client.send(topic, value=value)
        assert topic_instance.size() == 1

    # From TopicManager point of view all events were consumed
    # because there are not active Streams for the current topic,
    # even though the topic `kstreams--local` is not empty
    assert TopicManager.all_messages_consumed()


@pytest.mark.asyncio
async def test_partitions_for_topic(stream_engine: StreamEngine):
    topic_name = "local--kstreams"
    value = b'{"message": "Hello world!"}'
    key = "1"
    client = TestStreamClient(stream_engine)

    @stream_engine.stream(topic_name, name="my-stream")
    async def stream(stream):
        async for cr in stream:
            ...

    async with client:
        # produce to events and consume only one in the client context
        await client.send(topic_name, value=value, key=key, partition=0)
        await client.send(topic_name, value=value, key=key, partition=2)
        await client.send(topic_name, value=value, key=key, partition=10)

        await asyncio.sleep(1e-10)
        assert stream.consumer is not None
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
        assert stream is not None
        assert stream.consumer is not None
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

        await asyncio.sleep(1e-10)
        # check that everything was commited
        assert my_stream.consumer is not None
        assert (await my_stream.consumer.committed(tp)) == total_events - 1


@pytest.mark.asyncio
async def test_e2e_example():
    """
    Test that events are produce by the engine and consumed by the streams
    """
    simple_example = importlib.import_module(
        "examples.simple-example.simple_example.app"
    )

    client = TestStreamClient(simple_example.stream_engine)

    async with client:
        metadata = await simple_example.produce()

    assert simple_example.event_store.total == 5
    assert metadata.partition == 0

    # check that all events has been consumed
    assert TopicManager.all_messages_consumed()


@pytest.mark.asyncio
async def test_repeat_e2e_example():
    """
    This test is to show that the same Stream can be tested multiple
    times and that all the resources must start from scratch on every unittest:
        1. There must not be events in topics from previous tests
        2. All extra partitions should be removed from Topics
        3. Streams on new test must have only the default partitions (0, 1, 2)
        4. Total events per Topic (asyncio.Queue) must be 0
    """
    simple_example = importlib.import_module(
        "examples.simple-example.simple_example.app"
    )

    topic_name = simple_example.topic
    client = TestStreamClient(simple_example.stream_engine)

    # From the previous test, we have produced 5 events
    # which still they are on memory. This is the application
    # logic
    assert simple_example.event_store.total == 5

    # clean the application store
    simple_example.event_store.clean()
    assert simple_example.event_store.total == 0

    async with client:
        topic = TopicManager.get(topic_name)
        assert topic.is_empty()

        # Even Though the topic is empty, the counter might be not
        assert topic.total_events == 0

        # check that all default partitions are empty
        assert topic.total_partition_events[0] == -1
        assert topic.total_partition_events[1] == -1
        assert topic.total_partition_events[2] == -1

        # add 1 event to the store
        metadata = await client.send(topic=topic_name, value=b"add-event")

        # give some time to process the event
        await asyncio.sleep(0.1)
        assert simple_example.event_store.total == 1
        assert metadata.partition == 0

        # remove 1 event from the store
        metadata = await client.send(
            topic=topic_name, value=b"remove-event", partition=1
        )

        # give some time to process the event
        await asyncio.sleep(0.1)
        assert simple_example.event_store.total == 0
        assert metadata.partition == 1

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
    consume_multiple_topics_example = importlib.import_module(
        "examples.consume-multiple-topics-example.consume_multiple_topics_example.app"
    )

    total_events = 2
    client = TestStreamClient(
        consume_multiple_topics_example.stream_engine,
        monitoring_enabled=monitoring_enabled,
    )

    async with client:
        await consume_multiple_topics_example.produce(total_events)

        topic_1 = TopicManager.get(consume_multiple_topics_example.topics[0])
        topic_2 = TopicManager.get(consume_multiple_topics_example.topics[1])

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

        with contextlib.suppress(TimeoutErrorException):
            # now it is possible to run a stream directly, so we need
            # to stop the `forever` consumption
            await asyncio.wait_for(stream.start(), timeout=1.0)

        assert stream.consumer is not None
        assert stream.rebalance_listener is not None

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

        # Checl all the events were consumed
        assert not TopicManager.get(name=topic).size()

    process.assert_has_calls([call(event1), call(event1), call(event2)], any_order=True)


@pytest.mark.asyncio
async def test_send_many_example():
    app = importlib.import_module("examples.send-many.src.send_many.app")

    client = TestStreamClient(app.stream_engine)

    async with client:
        metadata = await app.send_many()

        topic = TopicManager.get("local--kstreams-send-many")
        assert topic.total_events == 5
        assert topic.consumed

    assert metadata.partition == 0
    assert metadata.topic == "local--kstreams-send-many"
    assert metadata.offset == 4
