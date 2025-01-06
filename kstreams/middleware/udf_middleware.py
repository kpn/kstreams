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


class UdfParam(typing.NamedTuple):
    annotation: type
    args: typing.Tuple[typing.Any]
    is_generic: bool = False


def build_params(signature: inspect.Signature) -> typing.List[UdfParam]:
    return [
        UdfParam(
            typing.get_origin(param.annotation) or param.annotation,
            typing.get_args(param.annotation),
            typing.get_origin(param.annotation) is not None,
        )
        for param in signature.parameters.values()
    ]


class UdfHandler(BaseMiddleware):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        signature = inspect.signature(self.next_call)
        self.params: typing.List[UdfParam] = [
            UdfParam(
                typing.get_origin(param.annotation) or param.annotation,
                typing.get_args(param.annotation),
                typing.get_origin(param.annotation) is not None,
            )
            for param in signature.parameters.values()
        ]

        self.type: UDFType = setup_type([p.annotation for p in self.params])

        self.ANNOTATIONS_TO_PARAMS: dict[type, typing.Any] = {
            types.ConsumerRecord: None,
            Stream: self.stream,
            types.Send: self.send,
        }

    def get_type(self) -> UDFType:
        """Used by the stream_engine to know whether to call this middleware or not."""
        return self.type

    def build_param(self, param: UdfParam, cr: types.ConsumerRecord) -> type:
        if (
            param.annotation is types.ConsumerRecord
            and param.is_generic
            and len(param.args) == 2  # guarantees ConsumerRecord has two args
        ):
            cr_type = param.args[1]

            # Check if it's compatible with a pydantic model
            if hasattr(cr_type, "model_validate"):
                pydantic_value = cr_type.model_validate(cr.value)
                self.ANNOTATIONS_TO_PARAMS[types.ConsumerRecord] = cr._replace(
                    value=pydantic_value
                )
        return param.annotation

    def bind_udf_params(self, cr: types.ConsumerRecord) -> typing.List:
        self.ANNOTATIONS_TO_PARAMS[types.ConsumerRecord] = cr
        return [
            self.ANNOTATIONS_TO_PARAMS[self.build_param(param, cr)]
            for param in self.params
        ]

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
