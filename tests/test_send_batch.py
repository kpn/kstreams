from unittest import mock

from kstreams import BatchEvent, RecordMetadata
from kstreams.batch import BatchAggregator
from kstreams.clients import Producer
from kstreams.engine import StreamEngine


async def test_batch_aggregator_default_partition(
    stream_engine: StreamEngine, record_metadata: RecordMetadata
):
    topic = "test-send-many"
    partition = 0

    batch_events = [
        BatchEvent(key="key1", value=b"value1"),
        BatchEvent(key="key2", value=b"value2"),
        BatchEvent(key="key3", value=b"value3"),
    ]

    async def async_func():
        return record_metadata

    send_batch = mock.AsyncMock(return_value=async_func())

    with (
        mock.patch.multiple(
            Producer, start=mock.DEFAULT, stop=mock.DEFAULT, send_batch=send_batch
        ),
    ):
        await stream_engine.start()
        assert stream_engine._producer is not None

        aggregator = BatchAggregator(
            topic=topic,
            partition=partition,
            producer=stream_engine._producer,
        )

        for event in batch_events:
            aggregator.append(
                key=event.key,
                value=event.value,
            )

        assert list(aggregator.batches.keys()) == [partition]

        result = await aggregator.flush()
        assert len(result) == 1


async def test_partitions_from_events_key_with_default_partitioner(
    stream_engine: StreamEngine, record_metadata: RecordMetadata
):
    topic = "test-send-many"

    batch_events = [
        BatchEvent(key=key, value=b"value") for key in ["key1", "key2", "key3"]
    ]

    async def async_func():
        return record_metadata

    send_batch = mock.AsyncMock(side_effect=[async_func(), async_func()])

    with (
        mock.patch.multiple(
            Producer, start=mock.DEFAULT, stop=mock.DEFAULT, send_batch=send_batch
        ),
        mock.patch.multiple(
            BatchAggregator,
            _all_partitions=lambda _: [1, 2, 3],
            _available_partitions=lambda _: [1, 2, 3],
        ),
    ):
        await stream_engine.start()
        assert stream_engine._producer is not None

        aggregator = BatchAggregator(
            topic=topic,
            producer=stream_engine._producer,
        )

        for event in batch_events:
            aggregator.append(
                key=event.key,
                value=event.value,
            )

        assert list(aggregator.batches.keys()) == [3, 2]

        result = await aggregator.flush()
        # we have 2 record metadatas because 2 partitions were used
        assert len(result) == 2


async def test_partitions_from_keyless_events_with_default_partitioner(
    stream_engine: StreamEngine, record_metadata: RecordMetadata
):
    topic = "test-send-many"

    batch_events = [BatchEvent(key=key, value=b"value") for key in [None, None, None]]

    async def async_func():
        return record_metadata

    send_batch = mock.AsyncMock(side_effect=[async_func(), async_func(), async_func()])

    with (
        mock.patch.multiple(
            Producer, start=mock.DEFAULT, stop=mock.DEFAULT, send_batch=send_batch
        ),
        mock.patch.multiple(
            BatchAggregator,
            _all_partitions=lambda _: [1, 2, 3],
            _available_partitions=lambda _: [1, 2, 3],
        ),
    ):
        await stream_engine.start()
        assert stream_engine._producer is not None

        aggregator = BatchAggregator(
            topic=topic,
            producer=stream_engine._producer,
        )

        for event in batch_events:
            aggregator.append(
                key=event.key,
                value=event.value,
            )

        total_partitions = len(aggregator.batches.keys())

        result = await aggregator.flush()
        # we have 2 record metadatas because 2 partitions were used
        assert len(result) == total_partitions


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
