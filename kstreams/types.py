import typing
from dataclasses import dataclass

from aiokafka.structs import RecordMetadata

if typing.TYPE_CHECKING:
    from .batch import BatchEvent  #  pragma: no cover
    from .serializers import Serializer  #  pragma: no cover

Headers = typing.Dict[str, str]
EncodedHeaders = typing.Sequence[typing.Tuple[str, bytes]]
StreamFunc = typing.Callable[..., typing.Any]
EngineHooks = typing.Sequence[typing.Callable[[], typing.Any]]


class Send(typing.Protocol):
    """
    Sends an event to the specified topic.

    Attributes:
        topic (str): Topic name to send the event to
        value (Any): Event value
        key (str | None): Event key
        partition (int | None): Topic partition
        timestamp_ms (int | None): Event timestamp in miliseconds
        headers (Dict[str, str] | None): Event headers
        serializer (kstreams.serializers.Serializer | None): Serializer to
            encode the event
        serializer_kwargs (Dict[str, Any] | None): Serializer kwargs

    Returns:
        RecordMetadata: Metadata of the sent record
    """

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


class SendMany(typing.Protocol):
    """
    Sends many events in a batch to the specified topic and partition.

    Attributes:
        topic (str): Topic name to send the event to
        partition (int): Partition to send the events to
        batch_events (List[kstreams.structs.BatchEvent]): List of events to send
        key (str | None): Events key. If provided, it is used when an
            event has not its own key
        timestamp_ms (int | None): Event timestamp in miliseconds
        headers (Dict[str, str] | None): Event headers
        serializer (kstreams.serializers.Serializer | None): Serializer to
            encode the event
        serializer_kwargs (Dict[str, Any] | None): Serializer kwargs

    Returns:
        RecordMetadata: Metadata of the sent records
    """

    def __call__(
        self,
        topic: str,
        partition: int,
        batch_events: typing.List["BatchEvent"],
        key: typing.Any = None,
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
    """
    A record received from a Kafka topic.

    Attributes:
        topic (str): The topic this record is received from
        partition (int): The partition from which this record is received
        offset (int): The position of this record in the corresponding Kafka partition
        timestamp (int): The timestamp of this record
        timestamp_type (int): The timestamp type of this record
        key (Optional[KT]): The key (or `None` if no key is specified)
        value (Optional[VT]): The value
        checksum (Optional[int]): Deprecated
        serialized_key_size (int): The size of the serialized, uncompressed key in bytes
        serialized_value_size (int): The size of the serialized, uncompressed value
            in bytes
        headers (EncodedHeaders): The headers
    """

    topic: str
    partition: int
    offset: int
    timestamp: int
    timestamp_type: int
    key: typing.Optional[KT]
    value: typing.Optional[VT]
    checksum: typing.Optional[int]
    serialized_key_size: int
    serialized_value_size: int
    headers: EncodedHeaders


NextMiddlewareCall = typing.Callable[[ConsumerRecord], typing.Awaitable[None]]

# 0 for CreateTime; 1 for LogAppendTime;
# aiokafka also supports None, which means it's unsupported, but
# we only support messages > 1
TimestampType = typing.Literal[0, 1]
