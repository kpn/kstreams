import inspect
import typing

from kstreams import types
from kstreams._di.dependencies.core import StreamDependencyManager
from kstreams.streams_utils import UDFType, setup_type

from .middleware import BaseMiddleware


class DependencyInjectionHandler(BaseMiddleware):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.dependecy_manager = StreamDependencyManager(
            stream=self.stream, send=self.send
        )

        # To be deprecated once streams with type hints are deprecated
        signature = inspect.signature(self.next_call)
        self.params = list(signature.parameters.values())
        self.type: UDFType = setup_type(self.params)
        if self.type == UDFType.WITH_TYPING:
            self.dependecy_manager.solve_user_fn(fn=self.next_call)

    def get_type(self) -> UDFType:
        return self.type

    async def __call__(self, cr: types.ConsumerRecord) -> typing.Any:
        return await self.dependecy_manager.execute(cr)
