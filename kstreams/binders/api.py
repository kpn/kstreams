import inspect
from typing import Any, AsyncIterator, Awaitable, Protocol, TypeVar, Union

from di.api.dependencies import CacheKey
from di.dependant import Dependant, Marker

from kstreams.types import ConsumerRecord


class ExtractorTrait(Protocol):
    """Implement to extract data from incoming `ConsumerRecord`.

    Consumers will always work with a consumer Record.
    Implementing this would let you extract information from the `ConsumerRecord`.
    """

    def __hash__(self) -> int:
        """Required by di in order to cache the deps"""
        ...

    def __eq__(self, __o: object) -> bool:
        """Required by di in order to cache the deps"""
        ...

    async def extract(
        self, consumer_record: ConsumerRecord
    ) -> Union[Awaitable[Any], AsyncIterator[Any]]:
        """This is where the magic should happen.

        For example, you could "extract" here a json from the `ConsumerRecord.value`
        """
        ...


T = TypeVar("T", covariant=True)


class MarkerTrait(Protocol[T]):
    def register_parameter(self, param: inspect.Parameter) -> T:
        ...


class Binder(Dependant[Any]):
    def __init__(
        self,
        *,
        extractor: ExtractorTrait,
    ) -> None:
        super().__init__(call=extractor.extract, scope="consumer_record")
        self.extractor = extractor

    @property
    def cache_key(self) -> CacheKey:
        return self.extractor


class BinderMarker(Marker):
    """Bind together the different dependencies.

    NETX: Add asyncapi marker here, like `MarkerTrait[AsyncApiTrait]`.
        Recommendation to wait until 3.0:
            - [#618](https://github.com/asyncapi/spec/issues/618)
    """

    def __init__(self, *, extractor_marker: MarkerTrait[ExtractorTrait]) -> None:
        self.extractor_marker = extractor_marker

    def register_parameter(self, param: inspect.Parameter) -> Binder:
        return Binder(extractor=self.extractor_marker.register_parameter(param))
