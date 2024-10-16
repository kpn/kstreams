import inspect
import sys
import typing

from kstreams import types
from kstreams.streams import Stream
from kstreams.streams_utils import UDFType, setup_type

from .middleware import BaseMiddleware

if sys.version_info < (3, 10):

    async def anext(async_gen: typing.AsyncGenerator):
        return await async_gen.__anext__()


class UdfHandler(BaseMiddleware):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        signature = inspect.signature(self.next_call)
        self.params = list(signature.parameters.values())
        self.type: UDFType = setup_type(self.params)

    def bind_udf_params(self, cr: types.ConsumerRecord) -> typing.List:
        # NOTE: When `no typing` support is deprecated then this can
        # be more eficient as the CR will be always there.
        ANNOTATIONS_TO_PARAMS = {
            types.ConsumerRecord: cr,
            Stream: self.stream,
            types.Send: self.send,
        }

        return [ANNOTATIONS_TO_PARAMS[param.annotation] for param in self.params]

    async def __call__(self, cr: types.ConsumerRecord) -> typing.Any:
        """
        Call the coroutine `async def my_function(...)` defined by the end user
        in a proper way according to its parameters. The `handler` is the
        coroutine defined by the user.

        Use cases:
            1. UDFType.CR_ONLY_TYPING: Only ConsumerRecord with typing

            @stream_engine.stream(topic, name="my-stream")
                async def consume(cr: ConsumerRecord):
                    ...

            2. UDFType.ALL_TYPING: ConsumerRecord and Stream with typing.
                The order is important as they are arguments and not kwargs

            @stream_engine.stream(topic, name="my-stream")
                async def consume(cr: ConsumerRecord, stream: Stream):
                    ...
        """
        params = self.bind_udf_params(cr)

        if inspect.isasyncgenfunction(self.next_call):
            return await anext(self.next_call(*params))
        return await self.next_call(*params)
