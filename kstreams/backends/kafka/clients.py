import logging
import typing

import aiokafka
from pydantic import BaseModel, ConfigDict, model_validator

logger = logging.getLogger(__name__)


class ProducerSettings(BaseModel):
    """Kafka producer configuration.

    Attributes:
        client_id: Name passed to Kafka brokers for request logging. Default to None.
        metadata_max_age_ms: Interval in milliseconds to force metadata refresh.
            Default to 300000 (`5 minutes`)
        request_timeout_ms: Produce request timeout in milliseconds.
            Default to 40000 (`40 seconds`)
        acks: Required broker acknowledgments for produce requests. Default to 1.
        compression_type: Compression algorithm used for produced batches.
            Default to None
        max_batch_size: Maximum buffered bytes per partition before send blocks.
            Default to 16384
        partitioner: Callable used to choose target partition. Default to None.
        max_request_size: Maximum size of a produce request in bytes.
            Default to 1048576 (`~1 MB`)
        linger_ms: Time in milliseconds to wait for additional records before send.
            Default to 0
        retry_backoff_ms: Backoff in milliseconds between retry attempts.
            Default to 100
        connections_max_idle_ms: Close idle connections after this timeout.
            Default to 540000 (`9 minutes`)
        enable_idempotence: Enables idempotent production semantics.
            The value of `acks` must be `all` when `enable_idempotence` is True.
            Default to False.
        transactional_id: Transactional producer id for transactions.
            Default to None
        transaction_timeout_ms: Transaction timeout in milliseconds.
            Default to 60000 (`1 minute`)

    Raises:
        ValueError: If ``enable_idempotence`` is True and ``acks`` is 0 or 1.

    !!! Example
        ```python
        from kstreams import create_engine
        from kstreams.backends.kafka import Kafka
        from kstreams.clients import ProducerSettings


        backend = Kafka(bootstrap_servers=["localhost:9092"])
        producer_settings = ProducerSettings(
            client_id="my-producer",
            linger_ms=10,
            request_timeout_ms=80000,
        )

        stream_engine = create_engine(
            backend=backend,
            producer_settings=producer_settings
        )
        ```
    """

    client_id: typing.Optional[str] = None
    metadata_max_age_ms: int = 300000
    request_timeout_ms: int = 40000
    acks: typing.Union[typing.Literal[0, 1], typing.Literal["all"]] = 1
    compression_type: typing.Optional[
        typing.Literal["gzip", "snappy", "lz4", "zstd"]
    ] = None
    max_batch_size: int = 16384
    partitioner: typing.Optional[typing.Callable[..., int]] = None
    max_request_size: int = 1048576
    linger_ms: int = 0
    retry_backoff_ms: int = 100
    connections_max_idle_ms: typing.Optional[int] = 540000
    enable_idempotence: bool = False
    transactional_id: typing.Optional[str] = None
    transaction_timeout_ms: int = 60000

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @model_validator(mode="after")
    @classmethod
    def validate_idempotence_and_acks(cls, values):
        if values.enable_idempotence and values.acks in (0, 1):
            raise ValueError("`acks` must be `all` when `enable_idempotence` is True")
        return values


class Consumer(aiokafka.AIOKafkaConsumer):
    def __init__(
        self,
        *args,
        key_deserializer: typing.Optional[typing.Callable] = None,
        **kwargs,
    ):
        self.config = kwargs

        if key_deserializer is None:

            def key_deserializer(key):
                if key is not None:
                    return key.decode()

        super().__init__(*args, key_deserializer=key_deserializer, **self.config)


class Producer(aiokafka.AIOKafkaProducer):
    def __init__(
        self,
        *args,
        key_serializer=None,
        **kwargs,
    ):
        self.config = kwargs

        if key_serializer is None:

            def key_serializer(key):
                if key is not None:
                    return key.encode("utf-8")

        super().__init__(*args, key_serializer=key_serializer, **self.config)

    def all_partitions_for_topic(self, topic: str) -> typing.List[int]:
        return list(self._metadata.partitions_for_topic(topic))

    def available_partitions_for_topic(self, topic: str) -> typing.List[int]:
        return list(self._metadata.available_partitions_for_topic(topic))
