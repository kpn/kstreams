import asyncio
import contextlib
import typing
from unittest import mock

import pytest

from kstreams import (
    Consumer,
    ConsumerRecord,
    Producer,
    RecordMetadata,
    StreamEngine,
    TopicPartition,
    types,
)
from tests import TimeoutErrorException

from .conftest import JsonSerializer


@pytest.mark.parametrize("transaction_id", [None, "my-transaction-id"])
@pytest.mark.asyncio
async def test_create_transaction_with_stream_engine(
    stream_engine: StreamEngine,
    transaction_id: typing.Optional[str],
):
    with mock.patch.multiple(
        Producer,
        start=mock.DEFAULT,
        begin_transaction=mock.DEFAULT,
        commit_transaction=mock.DEFAULT,
    ):
        async with stream_engine.transaction(transaction_id=transaction_id) as t:
            ...

        if transaction_id is None:
            assert t.transaction_id is not None
            assert t.producer._txn_manager.transactional_id is not None
        else:
            assert t.transaction_id == "my-transaction-id"
            assert t.producer._txn_manager.transactional_id == "my-transaction-id"

        assert t.send is not None

        t.producer.start.assert_awaited()
        t.producer.begin_transaction.assert_awaited()
        t.producer.commit_transaction.assert_awaited()

        assert t.producer._closed


@pytest.mark.asyncio
async def test_create_transaction_with_stream(
    stream_engine: StreamEngine, consumer_record_factory
):
    value = b"test"
    transaction_id = "my-transaction-id"

    async def getone(_):
        return consumer_record_factory(value=value)

    with (
        mock.patch.multiple(
            Consumer,
            start=mock.DEFAULT,
            subscribe=mock.DEFAULT,
            getone=getone,
        ),
        mock.patch.multiple(
            Producer,
            start=mock.DEFAULT,
            stop=mock.DEFAULT,
            begin_transaction=mock.DEFAULT,
            commit_transaction=mock.DEFAULT,
        ),
    ):

        @stream_engine.stream("local--kstreams")
        async def stream(
            cr: ConsumerRecord[str, bytes], transaction: types.Transaction
        ):
            async with transaction(transaction_id=transaction_id) as t:
                assert cr.value == value

            assert t.transaction_id == transaction_id
            assert t.producer._txn_manager.transactional_id == transaction_id

            t.producer.start.assert_awaited()
            t.producer.begin_transaction.assert_awaited()
            t.producer.commit_transaction.assert_awaited()
            t.producer.stop.assert_awaited()

            await asyncio.sleep(0.1)

        with contextlib.suppress(TimeoutErrorException):
            await asyncio.wait_for(stream.start(), timeout=0.1)

        await stream.stop()


@pytest.mark.asyncio
async def test_send_event_in_transaction(
    stream_engine: StreamEngine, record_metadata: RecordMetadata
):
    async def async_func():
        return record_metadata

    with mock.patch.multiple(
        Producer,
        start=mock.DEFAULT,
        begin_transaction=mock.DEFAULT,
        commit_transaction=mock.DEFAULT,
        send=mock.AsyncMock(return_value=async_func()),
    ):
        async with stream_engine.transaction() as t:
            await t.send("sink-topic", value=b"1", key="1")

        t.producer.begin_transaction.assert_awaited()
        t.producer.commit_transaction.assert_awaited()
        t.producer.send.assert_awaited_once_with(
            "sink-topic",
            value=b"1",
            key="1",
            partition=None,
            timestamp_ms=None,
            headers=None,
        )


@pytest.mark.asyncio
async def test_send_event_with_global_serializer_in_transaction(
    stream_engine: StreamEngine, record_metadata: RecordMetadata
):
    stream_engine.serializer = JsonSerializer()
    value = {"value": "Hello world!!"}

    async def async_func():
        return record_metadata

    with mock.patch.multiple(
        Producer,
        start=mock.DEFAULT,
        begin_transaction=mock.DEFAULT,
        commit_transaction=mock.DEFAULT,
        send=mock.AsyncMock(return_value=async_func()),
    ):
        # Check that we send json data
        async with stream_engine.transaction() as t:
            await t.send("sink-topic", value=value, key="1")

        t.producer.begin_transaction.assert_awaited()
        t.producer.commit_transaction.assert_awaited()
        t.producer.send.assert_awaited_once_with(
            "sink-topic",
            value=b'{"value": "Hello world!!"}',
            key="1",
            partition=None,
            timestamp_ms=None,
            headers=None,
        )


@pytest.mark.asyncio
async def test_override_global_serializer_in_transaction(
    stream_engine: StreamEngine, record_metadata: RecordMetadata
):
    stream_engine.serializer = JsonSerializer()

    async def async_func():
        return record_metadata

    with mock.patch.multiple(
        Producer,
        start=mock.DEFAULT,
        begin_transaction=mock.DEFAULT,
        commit_transaction=mock.DEFAULT,
        send=mock.AsyncMock(return_value=async_func()),
    ):
        # We have to set serializer=None otherwise we will get
        # TypeError: Object of type bytes is not JSON serializable
        async with stream_engine.transaction() as t:
            await t.send(
                "sink-topic", value=b"Helloooo", key="2", partition=10, serializer=None
            )

        t.producer.begin_transaction.assert_awaited()
        t.producer.commit_transaction.assert_awaited()
        t.producer.send.assert_awaited_once_with(
            "sink-topic",
            value=b"Helloooo",
            key="2",
            partition=10,
            timestamp_ms=None,
            headers=None,
        )


@pytest.mark.asyncio
async def test_commit_event_in_transaction(
    stream_engine: StreamEngine,
    record_metadata: RecordMetadata,
    consumer_record_factory: typing.Callable[..., ConsumerRecord],
):
    async def async_func():
        return record_metadata

    async def getone(_):
        return consumer_record_factory(value=b"1")

    with (
        mock.patch.multiple(
            Producer,
            start=mock.DEFAULT,
            begin_transaction=mock.DEFAULT,
            commit_transaction=mock.DEFAULT,
            send=mock.AsyncMock(return_value=async_func()),
            send_offsets_to_transaction=mock.DEFAULT,
        ),
        mock.patch.multiple(
            Consumer,
            start=mock.DEFAULT,
            subscribe=mock.DEFAULT,
            unsubscribe=mock.DEFAULT,
            getone=getone,
        ),
    ):

        @stream_engine.stream("local--kstreams", group_id="test-group")
        async def stream(
            cr: ConsumerRecord[str, bytes], transaction: types.Transaction
        ):
            async with transaction() as t:
                assert cr.value == b"1"
                await t.send("sink-topic", value=b"1", key="1")

                tp = TopicPartition(topic=cr.topic, partition=cr.partition)
                await t.commit_offsets(
                    offsets={tp: cr.offset + 1}, group_id="test-group"
                )

            t.producer.begin_transaction.assert_awaited()
            t.producer.commit_transaction.assert_awaited()
            t.producer.send.assert_awaited_once_with(
                "sink-topic",
                value=b"1",
                key="1",
                partition=None,
                timestamp_ms=None,
                headers=None,
            )
            t.producer.send_offsets_to_transaction.assert_awaited_once_with(
                {tp: cr.offset + 1}, "test-group"
            )

            assert t.producer._closed
            await asyncio.sleep(0.1)

        with contextlib.suppress(TimeoutErrorException):
            await asyncio.wait_for(stream.start(), timeout=0.1)

        await stream.stop()


@pytest.mark.asyncio
async def test_abort_transaction(
    stream_engine: StreamEngine,
    record_metadata: RecordMetadata,
    consumer_record_factory: typing.Callable[..., ConsumerRecord],
):
    async def async_func():
        return record_metadata

    async def getone(_):
        return consumer_record_factory(value=b"1")

    with (
        mock.patch.multiple(
            Producer,
            start=mock.DEFAULT,
            begin_transaction=mock.DEFAULT,
            commit_transaction=mock.DEFAULT,
            abort_transaction=mock.DEFAULT,
            send=mock.AsyncMock(return_value=async_func()),
            send_offsets_to_transaction=mock.DEFAULT,
        ),
        mock.patch.multiple(
            Consumer,
            start=mock.DEFAULT,
            subscribe=mock.DEFAULT,
            unsubscribe=mock.DEFAULT,
            getone=getone,
        ),
        pytest.raises(ValueError) as exc,
    ):

        @stream_engine.stream("local--kstreams", group_id="test-group")
        async def stream(
            cr: ConsumerRecord[str, bytes], transaction: types.Transaction
        ) -> typing.NoReturn:
            async with transaction() as t:
                assert cr.value == b"1"
                await t.send("sink-topic", value=b"1", key="1")

                tp = TopicPartition(topic=cr.topic, partition=cr.partition)
                await t.commit_offsets(
                    offsets={tp: cr.offset + 1}, group_id="test-group"
                )

                # raise exception to abort the transaction
                raise ValueError("This is a test error")

            t.producer.begin_transaction.assert_awaited()

            # commit_transaction should not be called
            t.producer.commit_transaction.assert_not_awaited()

            # abort_transaction should be called
            t.producer.abort_transaction.assert_awaited()

            # The event is always produced even if the transaction is aborted
            # the consumer should filter the event using
            # isolation_level="read_committed"
            t.producer.send.assert_awaited_once_with(
                "sink-topic",
                value=b"1",
                key="1",
                partition=None,
                timestamp_ms=None,
                headers=None,
            )

            # The commit is always called even if the transaction is aborted
            t.producer.send_offsets_to_transaction.assert_awaited_once_with(
                {tp: cr.offset + 1}, "test-group"
            )

            assert t.producer._closed
            await asyncio.sleep(0.1)

        with contextlib.suppress(TimeoutErrorException):
            await asyncio.wait_for(stream.start(), timeout=0.1)

    await stream.stop()

    assert exc.type is ValueError
