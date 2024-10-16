import typing
from dataclasses import dataclass

from aiokafka.structs import RecordMetadata

if typing.TYPE_CHECKING:
    from .serializers import Serializer  #  pragma: no cover

Headers = typing.Dict[str, str]
EncodedHeaders = typing.Sequence[typing.Tuple[str, bytes]]
StreamFunc = typing.Callable

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


D = typing.TypeVar("D")
Deprecated = typing.Annotated[D, "deprecated"]

KT = typing.TypeVar("KT")
VT = typing.TypeVar("VT")


@dataclass
class ConsumerRecord(typing.Generic[KT, VT]):
    topic: str
    "The topic this record is received from"

    partition: int
    "The partition from which this record is received"

    offset: int
    "The position of this record in the corresponding Kafka partition."

    timestamp: int
    "The timestamp of this record"

    timestamp_type: int
    "The timestamp type of this record"

    key: typing.Optional[KT]
    "The key (or `None` if no key is specified)"

    value: typing.Optional[VT]
    "The value"

    checksum: typing.Optional[int]
    "Deprecated"

    serialized_key_size: int
    "The size of the serialized, uncompressed key in bytes."

    serialized_value_size: int
    "The size of the serialized, uncompressed value in bytes."

    headers: EncodedHeaders
    "The headers"


NextMiddlewareCall = typing.Callable[[ConsumerRecord], typing.Awaitable[None]]

# 0 for CreateTime; 1 for LogAppendTime;
# aiokafka also supports None, which means it's unsupported, but
# we only support messages > 1
TimestampType = typing.Literal[0, 1]
