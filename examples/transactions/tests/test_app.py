import asyncio
from unittest import mock

import pytest
from transactions.app import stream_engine

from kstreams import TopicPartition, consts
from kstreams.test_utils import TestStreamClient


@pytest.mark.asyncio
async def test_consume_from_transactional_topic():
    client = TestStreamClient(stream_engine)

    tp = TopicPartition(
        topic="local--kstreams-transactional",
        partition=1,
    )

    async with client:
        async with client.transaction() as t:
            await t.send(
                "local--kstreams-transactional",
                value=b"Some event in transaction",
                serializer=None,
                partition=1,
            )

        stream = stream_engine.get_stream("transactional-stream")

        # give some time to the streams to consume all the events
        await asyncio.sleep(0.1)
        assert (await stream.consumer.committed(tp)) == 1


@pytest.mark.asyncio
async def test_e2et():
    client = TestStreamClient(stream_engine)
    tp = TopicPartition(
        topic="local--kstreams-transactional",
        partition=0,
    )
    total_events = 3

    with mock.patch("transactions.streams.save_to_db") as save_to_db:
        async with client:
            for _ in range(total_events):
                await client.send(
                    topic="local--kstreams-json",
                    value={"message": "Hello world!"},
                    partition=0,
                    headers={
                        "content-type": consts.APPLICATION_JSON,
                    },
                )

            # check that everything was commited
            stream = stream_engine.get_stream("transactional-stream")
            assert stream.consumer is not None

            # give some time to the streams to consume all the events
            await asyncio.sleep(0.1)
            assert (await stream.consumer.committed(tp)) == total_events

    save_to_db.assert_awaited()
    assert save_to_db.call_count == total_events
