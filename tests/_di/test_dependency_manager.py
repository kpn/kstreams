from kstreams import StreamDependencyManager
from kstreams.types import ConsumerRecord


async def test_cr_is_injected(rand_consumer_record):
    rand_consumer_record

    async def user_fn(event_type: ConsumerRecord) -> str:
        event_type.value = "hello"
        return event_type.value

    stream_manager = StreamDependencyManager()
    stream_manager.build(user_fn=user_fn)
    content = await stream_manager.execute(rand_consumer_record)
    assert content == "hello"
