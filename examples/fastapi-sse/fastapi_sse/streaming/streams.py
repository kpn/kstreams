from typing import Optional

from kstreams import ConsumerRecord, Stream

from .engine import stream_engine


def stream_factory(
    *, topic: str, group_id: Optional[str] = None, auto_offset_reset: str = "latest"
):
    async def stream_func(cr: ConsumerRecord):
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
