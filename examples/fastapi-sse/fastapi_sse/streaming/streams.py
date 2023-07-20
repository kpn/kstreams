from kstreams import Stream

from .engine import stream_engine


def stream_factory(
    *, topic: str, group_id: str = None, auto_offset_reset: str = "latest"
):
    async def stream_func(stream: Stream):
        async for cr in stream:
            yield cr.value

    s = Stream(
        topic,
        name=group_id,
        func=stream_func,
        config=dict(
            auto_offset_reset=auto_offset_reset,
            group_id=group_id,
        ),
    )
    stream_engine.add_stream(s)
    return s
