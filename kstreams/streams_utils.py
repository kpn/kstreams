import enum
import inspect
from typing import Any, Callable


class UDFType(str, enum.Enum):
    NO_TYPING = "NO_TYPING"
    CR_ONLY_TYPING = "CR_ONLY_TYPING"
    ALL_TYPING = "ALL_TYPING"


def inspect_udf(func: Callable, a_type: Any) -> UDFType:
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

        2. Using only the ConsumerRecord (it must be used with typing)

            @stream_engine.stream(topic, name="my-stream")
                async def consume(cr: ConsumerRecord):
                    ...

        3. Using ConsumerRecord and Stream, both with typing.
            The order is important as they are arguments and not kwargs

            @stream_engine.stream(topic, name="my-stream")
                async def consume(cr: ConsumerRecord, stream: Stream):
                    ...
    """
    signature = inspect.signature(func)

    # order is important as there are not kwargs
    parameters = list(signature.parameters.values())

    if len(parameters) == 1:
        # use case 1 or 2
        first_parameter = parameters[0]
        if first_parameter.annotation in (inspect._empty, a_type):
            # use case 1
            return UDFType.NO_TYPING
        else:
            # use case 2
            return UDFType.CR_ONLY_TYPING
    else:
        # use case 3
        return UDFType.ALL_TYPING
