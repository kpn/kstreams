import asyncio
from typing import Callable
from unittest import mock

import pytest

from kstreams import BatchEvent, ConsumerRecord, RecordMetadata
from kstreams.clients import Consumer, Producer
from kstreams.engine import Stream, StreamEngine
from kstreams.exceptions import DuplicateStreamException, EngineNotStartedException


class DummyDeserializer:
    async def deserialize(
        self, consumer_record: ConsumerRecord, **kwargs
    ) -> ConsumerRecord:
        return consumer_record


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

    deserializer = DummyDeserializer()

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

    deserializer = DummyDeserializer()

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

    deserializer = DummyDeserializer()

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

    deserializer = DummyDeserializer()

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
        Stream.stop.assert_awaited()  # type: ignore


@pytest.mark.asyncio
async def test_start_stop_stream_engine(stream_engine: StreamEngine):
    topic = "local--hello-kpn"

    @stream_engine.stream(topic)
    async def stream(_): ...

    with mock.patch.multiple(Consumer, start=mock.DEFAULT, stop=mock.DEFAULT):
        with mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
            await stream_engine.start()
            stream_engine._producer.start.assert_awaited()  # type: ignore

            await asyncio.sleep(0)  # Allow stream coroutine to run once
            Consumer.start.assert_awaited()

            await stream_engine.stop()
            stream_engine._producer.stop.assert_awaited()  # type: ignore
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

    with (
        mock.patch.multiple(
            Consumer,
            start=mock.DEFAULT,
            stop=mock.DEFAULT,
            getone=getone,
        ),
        mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT),
    ):
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

    with pytest.raises(EngineNotStartedException):
        await stream_engine.send_many(topic, partition=0, batch_events=[])


@pytest.mark.asyncio
async def test_send(stream_engine: StreamEngine, record_metadata: RecordMetadata):
    topic = "local--hello-kpn"
    value = b"Hello world"

    async def async_func():
        return record_metadata

    send = mock.AsyncMock(return_value=async_func())

    with (
        mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT, send=send),
    ):
        await stream_engine.start()
        metadata = await stream_engine.send(topic, value=value, partition=1)

        # stop engine immediately, this should not break the streams
        # and it should wait until the event is processed.
        await stream_engine.stop()

        met_position = (
            list(
                stream_engine.monitor.MET_OFFSETS.labels(
                    topic=topic,
                    partition=1,
                ).collect()
            )[0]
            .samples[0]
            .value
        )

        assert met_position == metadata.offset


@pytest.mark.asyncio
async def test_send_many(stream_engine: StreamEngine, record_metadata: RecordMetadata):
    topic = "local--hello-kpn"
    value = b"Hello world"
    total_events = 5

    async def async_func():
        return record_metadata

    send_batch = mock.AsyncMock(return_value=async_func())
    batch_events = [BatchEvent(value=value, key="1") for _ in range(total_events)]

    with (
        mock.patch.multiple(
            Producer, start=mock.DEFAULT, stop=mock.DEFAULT, send_batch=send_batch
        ),
    ):
        await stream_engine.start()
        await stream_engine.send_many(topic, partition=1, batch_events=batch_events)
        await stream_engine.stop()

    send_batch.assert_awaited_once()
    send_batch.assert_awaited_with(
        mock.ANY,
        topic,
        partition=1,
    )
