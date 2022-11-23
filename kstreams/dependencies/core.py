from typing import Any, Callable, Optional

from di.container import Container, bind_by_type
from di.dependant import Dependant
from di.executors import AsyncExecutor

from kstreams.types import ConsumerRecord


class StreamDependencyManager:
    """Core of dependency injection on kstreams.

    Attributes:
        container: dependency store.

    Usage

    ```python
    consumer_record = ConsumerRecord(...)
    def user_func(event_type: FromHeader[str]):
        ...

    sdm = StreamDependencyManager()
    sdm.solve(user_func)
    sdm.execute(consumer_record)
    ```
    """

    container: Container

    def __init__(self, container: Optional[Container] = None):
        self.container = container or Container()

    def _register_framework_deps(self):
        """Register with the container types that belong to kstream.

        These types can be injectable things available on startup.
        But also they can be things created on a per connection basis.
        They must be available at the time of executing the users' function.

        For example:

        - App
        - ConsumerRecord
        - Consumer
        - KafkaBackend

        Here we are just "registering", that's why we use `bind_by_type`.
        And eventually, when "executing", we should inject all the values we
        want to be "available".
        """
        self.container.bind(
            bind_by_type(
                Dependant(ConsumerRecord, scope="consumer_record", wire=False),
                ConsumerRecord,
            )
        )
        # NEXT: Add Consumer as a dependency, so it can be injected.

    def build(self, user_fn: Callable[..., Any]) -> None:
        """Build the dependency graph for the given function.

        Attributes:
            user_fn: user defined function, using allowed kstreams params
        """
        self._register_framework_deps()

        self.solved_user_fn = self.container.solve(
            Dependant(user_fn, scope="consumer_record"),
            scopes=["consumer_record"],
        )

    async def execute(self, consumer_record: ConsumerRecord) -> Any:
        """Execute the dependencies graph with external values.

        Attributes:
            consumer_record: A kafka record containing `values`, `headers`, etc.
        """
        with self.container.enter_scope("consumer_record") as state:
            return await self.container.execute_async(
                self.solved_user_fn,
                values={ConsumerRecord: consumer_record},
                executor=AsyncExecutor(),
                state=state,
            )
