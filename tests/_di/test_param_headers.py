from typing import Callable

import pytest

from kstreams import FromHeader, Header
from kstreams._di.dependencies.core import StreamDependencyManager
from kstreams.exceptions import HeaderNotFound
from kstreams.types import ConsumerRecord
from kstreams.typing import Annotated

RandConsumerRecordFixture = Callable[..., ConsumerRecord]


async def test_from_headers_ok(rand_consumer_record: RandConsumerRecordFixture):
    cr = rand_consumer_record(headers=(("event-type", "hello"),))

    async def user_fn(event_type: FromHeader[str]) -> str:
        return event_type

    stream_manager = StreamDependencyManager()
    stream_manager.solve_user_fn(user_fn)
    header_content = await stream_manager.execute(cr)
    assert header_content == "hello"


async def test_from_header_not_found(rand_consumer_record: RandConsumerRecordFixture):
    cr = rand_consumer_record(headers=(("event-type", "hello"),))

    def user_fn(a_header: FromHeader[str]) -> str:
        return a_header

    stream_manager = StreamDependencyManager()
    stream_manager.solve_user_fn(user_fn)
    with pytest.raises(HeaderNotFound):
        await stream_manager.execute(cr)


@pytest.mark.xfail(reason="not implemenetd yet")
async def test_from_headers_numbers(rand_consumer_record: RandConsumerRecordFixture):
    cr = rand_consumer_record(headers=(("event-type", "1"),))

    async def user_fn(event_type: FromHeader[int]) -> int:
        return event_type

    stream_manager = StreamDependencyManager()
    stream_manager.solve_user_fn(user_fn)
    header_content = await stream_manager.execute(cr)
    assert header_content == 1


async def test_headers_alias(rand_consumer_record: RandConsumerRecordFixture):
    cr = rand_consumer_record(headers=(("EventType", "hello"),))

    async def user_fn(event_type: Annotated[int, Header(alias="EventType")]) -> int:
        return event_type

    stream_manager = StreamDependencyManager()
    stream_manager.solve_user_fn(user_fn)
    header_content = await stream_manager.execute(cr)
    assert header_content == "hello"


async def test_headers_convert_underscores(
    rand_consumer_record: RandConsumerRecordFixture,
):
    cr = rand_consumer_record(headers=(("event_type", "hello"),))

    async def user_fn(
        event_type: Annotated[int, Header(convert_underscores=False)],
    ) -> int:
        return event_type

    stream_manager = StreamDependencyManager()
    stream_manager.solve_user_fn(user_fn)
    header_content = await stream_manager.execute(cr)
    assert header_content == "hello"
