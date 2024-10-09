import typing

from kstreams import ConsumerRecord, RecordMetadata

if typing.TYPE_CHECKING:
    from .serializers import Serializer  #  pragma: no cover

Headers = typing.Dict[str, str]
EncodedHeaders = typing.Sequence[typing.Tuple[str, bytes]]
StreamFunc = typing.Callable
NextMiddlewareCall = typing.Callable[[ConsumerRecord], typing.Awaitable[None]]
EngineHooks = typing.Sequence[typing.Callable[[], typing.Any]]


class Send(typing.Protocol):
    def __call__(
        self,
        topic: str,
        value: typing.Any = None,
        key: typing.Any = None,
        partition: typing.Optional[int] = None,
        timestamp_ms: typing.Optional[int] = None,
        headers: typing.Optional[Headers] = None,
        serializer: typing.Optional["Serializer"] = None,
        serializer_kwargs: typing.Optional[typing.Dict] = None,
    ) -> typing.Awaitable[RecordMetadata]: ...
