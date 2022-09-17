from typing import Any, Callable, Optional

from di import Container, bind_by_type
from di.dependent import Dependent
from di.executors import AsyncExecutor

from kstreams._di.dependencies.hooks import bind_by_generic
from kstreams.streams import Stream
from kstreams.types import ConsumerRecord, Send

LayerFn = Callable[..., Any]


class StreamDependencyManager:
    """Core of dependency injection on kstreams.

    This is an internal class of kstreams that manages the dependency injection,
    as a user you should not use this class directly.

    Attributes:
        container: dependency store.
        stream: the stream wrapping the user function. Optional to improve testability.
            When instanciating this class you must provide a stream, otherwise users
            won't be able to use the `stream` parameter in their functions.
        send: send object. Optional to improve testability, same as stream.

    Usage:

    stream and send are ommited for simplicity

    ```python
    def user_func(cr: ConsumerRecord):
        ...

    sdm = StreamDependencyManager()
    sdm.solve(user_func)
    sdm.execute(consumer_record)
    ```
    """

    container: Container

    def __init__(
        self,
        container: Optional[Container] = None,
        stream: Optional[Stream] = None,
        send: Optional[Send] = None,
    ):
        self.container = container or Container()
        self.async_executor = AsyncExecutor()
        self.stream = stream
        self.send = send

    def solve_user_fn(self, fn: LayerFn) -> None:
        """Build the dependency graph for the given function.

        Objects must be injected before this function is called.

        Attributes:
            fn: user defined function, using allowed kstreams params
        """
        self._register_consumer_record()

        if isinstance(self.stream, Stream):
            self._register_stream(self.stream)

        if self.send is not None:
            self._register_send(self.send)

        self.solved_user_fn = self.container.solve(
            Dependent(fn, scope="consumer_record"),
            scopes=["consumer_record", "stream", "application"],
        )

    async def execute(self, consumer_record: ConsumerRecord) -> Any:
        """Execute the dependencies graph with external values.

        Attributes:
            consumer_record: A kafka record containing `values`, `headers`, etc.
        """
        async with self.container.enter_scope("consumer_record") as state:
            return await self.solved_user_fn.execute_async(
                executor=self.async_executor,
                state=state,
                values={ConsumerRecord: consumer_record},
            )

    def _register_stream(self, stream: Stream):
        """Register the stream with the container."""
        hook = bind_by_type(
            Dependent(lambda: stream, scope="consumer_record", wire=False), Stream
        )
        self.container.bind(hook)

    def _register_consumer_record(self):
        """Register consumer record with the container.

        We bind_by_generic because we want to bind the `ConsumerRecord` type which
        is generic.

        The value must be injected at runtime.
        """
        hook = bind_by_generic(
            Dependent(ConsumerRecord, scope="consumer_record", wire=False),
            ConsumerRecord,
        )
        self.container.bind(hook)

    def _register_send(self, send: Send):
        hook = bind_by_type(
            Dependent(lambda: send, scope="consumer_record", wire=False), Send
        )
        self.container.bind(hook)
