import asyncio
import contextlib
from typing import Callable, Set
from unittest import mock

import pytest

from kstreams import ConsumerRecord, TopicPartition
from kstreams.clients import Consumer, Producer
from kstreams.engine import Stream, StreamEngine
from kstreams.streams import stream
from kstreams.structs import TopicPartitionOffset
from tests import TimeoutErrorException


@pytest.mark.asyncio
async def test_stream(stream_engine: StreamEngine, consumer_record_factory):
    topic_name = "local--kstreams"
    stream_name = "my-stream"
    value = b"test"

    async def getone(_):
        return consumer_record_factory(value=value)

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        subscribe=mock.DEFAULT,
        getone=getone,
    ):

        @stream_engine.stream(topic_name, name=stream_name)
        async def stream(cr: ConsumerRecord):
            assert cr.value == value
            await asyncio.sleep(0.2)

        assert stream.consumer is None
        assert stream.topics == [topic_name]

        with contextlib.suppress(TimeoutErrorException):
            # now it is possible to run a stream directly, so we need
            # to stop the `forever` consumption
            await asyncio.wait_for(stream.start(), timeout=0.1)

        assert stream.consumer
        Consumer.subscribe.assert_called_once_with(
            topics=[topic_name], listener=stream.rebalance_listener, pattern=None
        )
        await stream.stop()


@pytest.mark.asyncio
async def test_stream_multiple_topics(stream_engine: StreamEngine):
    topics = ["local--hello-kpn", "local--hello-kpn-2"]

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        subscribe=mock.DEFAULT,
    ):

        @stream_engine.stream(topics, name="my-stream")
        async def stream(_):
            ...

        assert stream.topics == topics

        await stream.start()
        Consumer.subscribe.assert_called_once_with(
            topics=topics, listener=stream.rebalance_listener, pattern=None
        )


@pytest.mark.asyncio
async def test_stream_subscribe_topics_pattern(stream_engine: StreamEngine):
    pattern = "^dev--customer-.*$"

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        subscribe=mock.DEFAULT,
    ):

        @stream_engine.stream(topics=pattern, subscribe_by_pattern=True)
        async def stream(_):
            ...

        assert stream.topics == [pattern]
        assert stream.subscribe_by_pattern

        await stream.start()
        Consumer.subscribe.assert_called_once_with(
            topics=None, listener=stream.rebalance_listener, pattern=pattern
        )


@pytest.mark.asyncio
async def test_stream_subscribe_topics_only_one_pattern(stream_engine: StreamEngine):
    """
    We can use only one pattern, so we use the first one
    """
    patterns = ["^dev--customer-.*$", "^acc--customer-.*$"]

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        subscribe=mock.DEFAULT,
        getone=mock.DEFAULT,
    ):

        @stream_engine.stream(topics=patterns, subscribe_by_pattern=True)
        async def stream(_):
            ...

        assert stream.topics == patterns
        assert stream.subscribe_by_pattern

        await stream.start()
        Consumer.subscribe.assert_called_once_with(
            topics=None, listener=stream.rebalance_listener, pattern=patterns[0]
        )


@pytest.mark.asyncio
async def test_stream_custom_conf(stream_engine: StreamEngine):
    @stream_engine.stream(
        "local--hello-kpn",
        name="stream-hello-kpn",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    async def stream(_):
        ...

    with mock.patch.multiple(
        Consumer, start=mock.DEFAULT, stop=mock.DEFAULT
    ), mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
        await stream_engine.start_streams()

        # switch the current Task to the one running in background
        await asyncio.sleep(0.1)

        assert stream.consumer._auto_offset_reset == "earliest"
        assert not stream.consumer._enable_auto_commit


@pytest.mark.asyncio
async def test_stream_getmany(
    stream_engine: StreamEngine, consumer_record_factory: Callable[..., ConsumerRecord]
):
    topic_partition_crs = {
        TopicPartition(topic="local--hello-kpn", partition=0): [
            consumer_record_factory(offset=1),
            consumer_record_factory(offset=2),
            consumer_record_factory(offset=3),
        ]
    }

    save_to_db = mock.Mock()

    @stream_engine.stream("local--hello-kpn")
    async def stream(stream: Stream):
        data = await stream.getmany(max_records=3)
        save_to_db(data)

    async def getmany(*args, **kwargs):
        return topic_partition_crs

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, getmany=getmany):
        await stream_engine.start_streams()
        await asyncio.sleep(0.1)
        save_to_db.assert_called_once_with(topic_partition_crs)


@pytest.mark.asyncio
async def test_stream_decorator(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream(topic)
    async def streaming_fn(_):
        pass

    stream_engine.add_stream(streaming_fn)

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
        with mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
            await stream_engine.start()

            # switch the current Task to the one running in background
            await asyncio.sleep(0.1)

            Consumer.start.assert_awaited()
            stream_engine._producer.start.assert_awaited()

            await stream_engine.stop()
            stream_engine._producer.stop.assert_awaited()
            Consumer.stop.assert_awaited()


@pytest.mark.asyncio
async def test_stream_decorates_properly(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream(topic)
    async def streaming_fn(_):
        """text from func"""

    assert streaming_fn.__name__ == "streaming_fn"
    assert streaming_fn.__doc__ == "text from func"


@pytest.mark.asyncio
async def test_no_recreate_consumer_on_re_start_stream(
    stream_engine: StreamEngine, consumer_record_factory
):
    topic_name = "local--kstreams"
    stream_name = "my-stream"

    async def getone(_):
        return consumer_record_factory()

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        getone=getone,
    ):

        @stream_engine.stream(topic_name, name=stream_name)
        async def stream(my_stream):
            async for cr in my_stream:
                assert cr
                break

        await stream.start()
        consumer = stream.consumer
        await stream.stop()
        await stream.start()
        assert consumer is stream.consumer


@pytest.mark.asyncio
async def test_seek_to_initial_offsets_normal(
    stream_engine: StreamEngine, consumer_record_factory
):
    assignments: Set[TopicPartition] = set()
    partition = 100
    offset = 10
    topic_name = "example_topic"
    value = b"Hello world"
    assignments.add(TopicPartition(topic=topic_name, partition=partition))
    seek_mock = mock.Mock()

    async def getone(_):
        return consumer_record_factory(value=value)

    with mock.patch.multiple(
        Consumer,
        assignment=lambda _: assignments,
        seek=seek_mock,
        start=mock.DEFAULT,
        getone=getone,
    ):

        @stream_engine.stream(
            topic_name,
            initial_offsets=[
                TopicPartitionOffset(
                    topic=topic_name, partition=partition, offset=offset
                )
            ],
        )
        async def stream(my_stream):
            async for cr in my_stream:
                assert cr.value == value
                break

        await stream.start()
        # simulate a partitions assigned rebalance
        await stream.rebalance_listener.on_partitions_assigned(assigned=assignments)

        seek_mock.assert_called_once_with(
            partition=TopicPartition(topic=topic_name, partition=partition),
            offset=offset,
        )


@pytest.mark.asyncio
async def test_seek_to_initial_offsets_ignores_wrong_input(
    stream_engine: StreamEngine, consumer_record_factory
):
    offset = 100
    partition = 100
    topic_name = "example_topic"
    wrong_topic = "different_topic"
    value = b"Hello world"
    wrong_partition = 1
    assignments: Set[TopicPartition] = set()
    assignments.add(TopicPartition(topic=topic_name, partition=partition))
    seek_mock = mock.Mock()

    async def getone(_):
        return consumer_record_factory(value=value)

    with mock.patch.multiple(
        Consumer,
        assignment=lambda _: assignments,
        seek=seek_mock,
        start=mock.DEFAULT,
        getone=getone,
    ):

        @stream_engine.stream(
            topic_name,
            initial_offsets=[
                TopicPartitionOffset(
                    topic=wrong_topic, partition=partition, offset=offset
                ),
                TopicPartitionOffset(
                    topic=topic_name, partition=wrong_partition, offset=offset
                ),
            ],
        )
        async def stream(my_stream):
            async for cr in my_stream:
                assert cr.value == value
                break

        await stream.start()
        # simulate a partitions assigned rebalance
        await stream.rebalance_listener.on_partitions_assigned(assigned=assignments)
        seek_mock.assert_not_called()
