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
    """User Defined Function Handler Middleware

    Manages dependency injection for user defined functions (UDFs) that are
    defined as coroutines. The UDFs are defined by the user and are passed
    to the stream engine to be executed when a consumer record is received.

    The UDFs can have different signatures and the middleware is responsible
    for managing the dependency injection for the UDFs.

    UdfHandler tries to stay small and performant.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        signature = inspect.signature(self.next_call)
        self.params: typing.List[typing.Any] = [
            typing.get_origin(param.annotation) or param.annotation
            for param in signature.parameters.values()
        ]
        self.type: UDFType = setup_type(self.params)
        self.annotations_to_params: dict[type, typing.Any] = {
            types.ConsumerRecord: None,
            Stream: self.stream,
            types.Send: self.send,
        }

    def get_type(self) -> UDFType:
        return self.type

    def bind_cr(self, cr: types.ConsumerRecord) -> None:
        self.annotations_to_params[types.ConsumerRecord] = cr

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
        self.bind_cr(cr)
        params = [self.annotations_to_params[param_type] for param_type in self.params]

        if inspect.isasyncgenfunction(self.next_call):
            return await anext(self.next_call(*params))
        return await self.next_call(*params)
