import asyncio
from unittest import mock

import pytest

from kstreams import ConsumerRecord, StreamEngine
from kstreams.clients import Consumer, Producer


@pytest.mark.asyncio
async def test_hook_on_startup(stream_engine: StreamEngine, consumer_record_factory):
    on_startup_sync_mock = mock.Mock()
    on_startup_async_mock = mock.AsyncMock()

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        stop=mock.DEFAULT,
    ), mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
        assert stream_engine._on_startup == []

        @stream_engine.stream("local--kstreams")
        async def stream(cr: ConsumerRecord): ...

        @stream_engine.on_startup
        async def init_db():
            on_startup_sync_mock()

        @stream_engine.on_startup
        async def backgound_task():
            await on_startup_async_mock()

            # check monitoring is not running
            assert not stream_engine.monitor.running

            # check stream is not running
            assert not stream.running

        assert stream_engine._on_startup == [init_db, backgound_task]

        # check that `on_startup` hooks were not called before
        # `stream_engine.start()` was called
        on_startup_sync_mock.assert_not_called()
        on_startup_async_mock.assert_not_awaited()

        await stream_engine.start()

        # check that `on_startup` hooks were called
        on_startup_sync_mock.assert_called_once()
        on_startup_async_mock.assert_awaited_once()

        await stream_engine.stop()


@pytest.mark.asyncio
async def test_hook_after_startup(stream_engine: StreamEngine, consumer_record_factory):
    after_startup_async_mock = mock.AsyncMock()
    set_healthy_pod = mock.AsyncMock()

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        stop=mock.DEFAULT,
    ), mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
        assert stream_engine._after_startup == []

        @stream_engine.stream("local--kstreams")
        async def stream(cr: ConsumerRecord): ...

        @stream_engine.after_startup
        async def healthy():
            await set_healthy_pod()

        @stream_engine.after_startup
        async def backgound_task():
            # give some time to start the tasks
            await asyncio.sleep(0.1)

            await after_startup_async_mock()

            # check monitoring is running
            assert stream_engine.monitor.running

            # check stream is running
            assert stream.running

        assert stream_engine._after_startup == [healthy, backgound_task]

        # check that `after_startup` hooks were not called before
        # `stream_engine.start()` was called
        set_healthy_pod.assert_not_awaited()
        after_startup_async_mock.assert_not_awaited()

        await stream_engine.start()

        # check that `after_startup` hooks were called
        set_healthy_pod.assert_awaited_once()
        after_startup_async_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_hook_on_stop(stream_engine: StreamEngine, consumer_record_factory):
    close_db_mock = mock.Mock()
    backgound_task_mock = mock.AsyncMock()

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        stop=mock.DEFAULT,
    ), mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
        assert stream_engine._on_stop == []

        @stream_engine.stream("local--kstreams")
        async def stream(cr: ConsumerRecord): ...

        @stream_engine.on_stop
        async def close_db():
            close_db_mock()

        @stream_engine.on_stop
        async def stop_backgound_task():
            backgound_task_mock.cancel()

            # check that monitoring is running
            assert stream_engine.monitor.running

            # check streams are running
            assert stream.running

        assert stream_engine._on_stop == [close_db, stop_backgound_task]
        await stream_engine.start()

        # give some time to start the tasks
        await asyncio.sleep(0.1)

        # check that `on_stop` hooks were not called before
        # `stream_engine.stop()` was called
        close_db_mock.assert_not_called()
        backgound_task_mock.cancel.assert_not_awaited()

        await stream_engine.stop()

        # check that `on_stop` hooks were called
        close_db_mock.assert_called_once()
        backgound_task_mock.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_hook_after_stop(stream_engine: StreamEngine, consumer_record_factory):
    delete_files_mock = mock.AsyncMock()

    with mock.patch.multiple(
        Consumer,
        start=mock.DEFAULT,
        stop=mock.DEFAULT,
    ), mock.patch.multiple(Producer, start=mock.DEFAULT, stop=mock.DEFAULT):
        assert stream_engine._after_stop == []

        @stream_engine.stream("local--kstreams")
        async def stream(cr: ConsumerRecord): ...

        @stream_engine.after_stop
        async def delete_files():
            await delete_files_mock()

            # check that monitoring is not already running
            assert not stream_engine.monitor.running

            # check streams are not running
            assert not stream.running

        assert stream_engine._after_stop == [delete_files]
        await stream_engine.start()

        # check that `after_stop` hooks were not called before
        # `stream_engine.stop()` was called
        delete_files_mock.assert_not_awaited()

        await stream_engine.stop()

        # give some time to start the tasks
        await asyncio.sleep(0.1)

        # check that `after_stop` hooks were called
        delete_files_mock.assert_awaited_once()
