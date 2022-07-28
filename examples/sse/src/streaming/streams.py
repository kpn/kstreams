from kstreams import Stream

from .engine import stream_engine


def stream_factory(
    *, topic: str, group_id: str = None, auto_offset_reset: str = "earliest"
):
    @stream_engine.stream(topic, group_id=group_id, auto_offset_reset=auto_offset_reset)
    async def stream(stream: Stream):
        async for cr in stream:
            print(f"yield {cr.value}")
            yield cr.value

    return stream
