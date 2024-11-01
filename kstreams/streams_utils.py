import enum
import inspect
from typing import List

# NOTE: remove this module when Stream with `no typing` support is deprecated


class UDFType(str, enum.Enum):
    NO_TYPING = "NO_TYPING"
    WITH_TYPING = "WITH_TYPING"


class StreamErrorPolicy(str, enum.Enum):
    RESTART = "RESTART"
    STOP = "STOP"
    STOP_ENGINE = "STOP_ENGINE"
    STOP_APPLICATION = "STOP_APPLICATION"


def setup_type(params: List[inspect.Parameter]) -> UDFType:
    """
    Inspect the user defined function (coroutine) to get the  proper way to call it

    The cases are:

        1. Using only the Stream with ot without typing. This is the classical way
            to use kstreams. This might be deprecated.

            Note: that the `argument` streams, can be anything like consumer,
            stream, processor, etc.

            @stream_engine.stream(topic, name="my-stream")
                async def consume(stream: Stream):
                    for cr in stream:
                        ...

            or

            @stream_engine.stream(topic, name="my-stream")
                async def consume(stream):
                    for cr in stream:
                        ...

        2. Using `typing hints`. This can include: ConsumerRecord, Stream and Send.
            Any combination of the `typing hints` is also possible

            @stream_engine.stream(topic, name="my-stream")
            async def consume(cr: ConsumerRecord):
                ...

            @stream_engine.stream(topic, name="my-stream")
            async def consume(cr: ConsumerRecord, stream: Stream):
                ...

            @stream_engine.stream(topic, name="my-stream")
            async def consume(cr: ConsumerRecord, stream: Stream, send: Send):
                ...
    """
    from .streams import Stream

    first_annotation = params[0].annotation

    if first_annotation in (inspect._empty, Stream) and len(params) < 2:
        # use case 1 NO_TYPING
        return UDFType.NO_TYPING
    # typing cases
    return UDFType.WITH_TYPING
