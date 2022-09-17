from typing import Any, AsyncGenerator, Generator

import pytest

from kstreams._di.dependencies.core import StreamDependencyManager
from kstreams.streams import Stream
from kstreams.types import ConsumerRecord


class AppWrapper:
    """This is a fake class used to check if the ConsumerRecord is injected"""

    def __init__(self) -> None:
        self.foo = "bar"

    async def consume(self, cr: ConsumerRecord) -> str:
        return self.foo


@pytest.fixture
def di_cr(rand_consumer_record) -> Generator[ConsumerRecord, Any, None]:
    """Dependency injected ConsumerRecord"""
    yield rand_consumer_record()


async def test_cr_is_injected(di_cr: ConsumerRecord):
    async def user_fn(cr: ConsumerRecord) -> str:
        cr.value = "hello"
        return cr.value

    stream_manager = StreamDependencyManager()
    stream_manager._register_consumer_record()
    stream_manager.solve_user_fn(user_fn)
    content = await stream_manager.execute(di_cr)
    assert content == "hello"


async def test_cr_is_injected_in_class(di_cr: ConsumerRecord):
    app = AppWrapper()
    stream_manager = StreamDependencyManager()
    stream_manager._register_consumer_record()
    stream_manager.solve_user_fn(app.consume)
    content = await stream_manager.execute(di_cr)
    assert content == app.foo


async def test_cr_generics_is_injected(di_cr: ConsumerRecord):
    async def user_fn(cr: ConsumerRecord[Any, Any]) -> str:
        cr.value = "hello"
        return cr.value

    stream_manager = StreamDependencyManager()
    stream_manager._register_consumer_record()
    stream_manager.solve_user_fn(user_fn)
    content = await stream_manager.execute(di_cr)
    assert content == "hello"


async def test_cr_generics_str_is_injected(di_cr: ConsumerRecord):
    async def user_fn(cr: ConsumerRecord[str, str]) -> str:
        cr.value = "hello"
        return cr.value

    stream_manager = StreamDependencyManager()
    stream_manager._register_consumer_record()
    stream_manager.solve_user_fn(user_fn)
    content = await stream_manager.execute(di_cr)
    assert content == "hello"


async def test_cr_with_generator(di_cr: ConsumerRecord):
    async def user_fn(cr: ConsumerRecord) -> AsyncGenerator[str, None]:
        cr.value = "hello"
        yield cr.value

    stream_manager = StreamDependencyManager()
    stream_manager._register_consumer_record()
    stream_manager.solve_user_fn(user_fn)
    content = await stream_manager.execute(di_cr)

    assert content == "hello"


async def test_stream(di_cr: ConsumerRecord):
    async def user_fn(stream: Stream) -> str:
        return stream.name

    stream = Stream("my-topic", func=user_fn, name="stream_name")
    stream_manager = StreamDependencyManager()
    stream_manager._register_stream(stream)
    stream_manager._register_consumer_record()
    stream_manager.solve_user_fn(user_fn)
    content = await stream_manager.execute(di_cr)
    assert content == "stream_name"


async def test_stream_and_consumer_record(di_cr: ConsumerRecord):
    async def user_fn(stream: Stream, record: ConsumerRecord) -> tuple[str, str]:
        return (stream.name, record.topic)

    stream = Stream("my-topic", func=user_fn, name="stream_name")
    stream_manager = StreamDependencyManager()
    stream_manager._register_stream(stream)
    stream_manager._register_consumer_record()
    stream_manager.solve_user_fn(user_fn)
    (stream_name, topic_name) = await stream_manager.execute(di_cr)

    assert stream_name == "stream_name"
    assert topic_name == di_cr.topic
