from unittest.mock import AsyncMock, call

import pytest
from aiokafka.errors import ConsumerStoppedError

from kstreams import ConsumerRecord, Stream, StreamEngine, TestStreamClient, middleware


@pytest.mark.asyncio
async def test_middleware_stack_from_stream(
    stream_engine: StreamEngine, dlq_middleware
):
    stream_name = "my-stream"
    stream_name_local = "my-stream-local"

    @stream_engine.stream(
        "kstreams-topic",
        name=stream_name,
        middlewares=[middleware.Middleware(dlq_middleware.middleware)],
    )
    async def consume(cr: ConsumerRecord): ...

    @stream_engine.stream(
        "kstreams-topic-local",
        name=stream_name_local,
        middlewares=[middleware.Middleware(dlq_middleware.middleware)],
    )
    async def process(cr: ConsumerRecord, stream: Stream): ...

    my_stream = stream_engine.get_stream(stream_name)
    if my_stream is None:
        raise ValueError("Stream not found")
    my_stream_local = stream_engine.get_stream(stream_name_local)
    if my_stream_local is None:
        raise ValueError("Stream not found")

    middlewares = [
        middleware_factory.middleware
        for middleware_factory in my_stream.get_middlewares(stream_engine)
    ]
    middlewares_stream_local = [
        middleware_factory.middleware
        for middleware_factory in my_stream_local.get_middlewares(stream_engine)
    ]
    assert (
        middlewares
        == middlewares_stream_local
        == [middleware.ExceptionMiddleware, dlq_middleware.middleware]
    )


@pytest.mark.asyncio
async def test_middleware_stack_order(
    stream_engine: StreamEngine, dlq_middleware, elastic_middleware
):
    stream_name = "my-stream"

    @stream_engine.stream(
        "kstreams-topic",
        name=stream_name,
        middlewares=[
            middleware.Middleware(dlq_middleware.middleware),
            middleware.Middleware(elastic_middleware.middleware),
        ],
    )
    async def consume(cr: ConsumerRecord): ...

    my_stream = stream_engine.get_stream(stream_name)
    if my_stream is None:
        raise ValueError("Stream not found")
    middlewares = [
        middleware_factory.middleware
        for middleware_factory in my_stream.get_middlewares(stream_engine)
    ]
    assert middlewares == [
        middleware.ExceptionMiddleware,
        dlq_middleware.middleware,
        elastic_middleware.middleware,
    ]


@pytest.mark.asyncio
async def test_middleware_call_from_stream(stream_engine: StreamEngine, dlq_middleware):
    topic = "local--hello-kpn"
    value = b"joker"
    save_to_db = AsyncMock()
    client = TestStreamClient(stream_engine)

    middlewares = [middleware.Middleware(dlq_middleware.middleware)]

    @stream_engine.stream(topic, middlewares=middlewares)
    async def my_stream(cr: ConsumerRecord):
        if cr.value == value:
            raise ValueError("Error from stream...")
        await save_to_db(cr.value)

    async with client:
        await client.send(topic, value=value)

    dlq_middleware.call.assert_awaited_once_with(value)

    # check that the call was not done
    save_to_db.assert_not_awaited()


@pytest.mark.asyncio
async def test_middleware_call_from_async_generator(
    stream_engine: StreamEngine, elastic_middleware
):
    topic = "local--hello-kpn"
    event = b"batman"
    save_to_db = AsyncMock()
    client = TestStreamClient(stream_engine)
    middlewares = [middleware.Middleware(elastic_middleware.middleware)]

    @stream_engine.stream(topic, middlewares=middlewares)
    async def my_stream(cr: ConsumerRecord):
        await save_to_db(cr.value)
        yield cr

    async with client:
        await client.send(topic, value=event)

        async with my_stream as stream_flow:
            async for cr in stream_flow:
                assert cr.value == event
                break

    elastic_middleware.call.assert_awaited_once_with(event)
    save_to_db.assert_awaited_once_with(event)


@pytest.mark.asyncio
async def test_middleware_call_chain_from_stream(
    stream_engine: StreamEngine, dlq_middleware, elastic_middleware, s3_middleware
):
    topic = "local--hello-kpn"
    event_1 = b"batman"
    event_2 = b"joker"
    save_to_db = AsyncMock()

    client = TestStreamClient(stream_engine)

    middlewares = [
        middleware.Middleware(dlq_middleware.middleware),
        middleware.Middleware(elastic_middleware.middleware),
        middleware.Middleware(s3_middleware.middleware),
    ]

    @stream_engine.stream(topic, middlewares=middlewares)
    async def consume(cr: ConsumerRecord):
        if cr.value == event_2:
            raise ValueError("Error from stream...")
        await save_to_db(cr.value)

    async with client:
        await client.send(topic, value=event_1)
        await client.send(topic, value=event_2)

    # called only when the whole chain has worked
    save_to_db.assert_awaited_once_with(event_1)

    # called only when the whole chain is broken
    dlq_middleware.call.assert_awaited_once_with(event_2)

    # called always
    elastic_middleware.call.assert_has_awaits(calls=[call(event_1), call(event_2)])
    s3_middleware.call.assert_has_awaits(calls=[call(event_1), call(event_2)])

    await stream_engine.stop()


@pytest.mark.asyncio
async def test_base_middleware_exception(stream_engine: StreamEngine):
    topic = "local--hello-kpn"
    stream_name = "my_stream"
    client = TestStreamClient(stream_engine)

    @stream_engine.stream(
        topic,
        name=stream_name,
        middlewares=[middleware.Middleware(middleware.BaseMiddleware)],
    )
    async def stream(cr: ConsumerRecord): ...

    async with client:
        await client.send(topic, value=b"test")

    assert not stream.running
    assert stream.consumer is None


@pytest.mark.asyncio
async def test_exception_middleware_consumer_stops(stream_engine: StreamEngine):
    topic = "local--hello-kpn"
    stream_name = "my_stream"
    client = TestStreamClient(stream_engine)

    @stream_engine.stream(topic, name=stream_name)
    async def stream(cr: ConsumerRecord):
        raise ConsumerStoppedError

    async with client:
        await client.send(topic, value=b"test")

    assert not stream.running
    assert stream.consumer is None
