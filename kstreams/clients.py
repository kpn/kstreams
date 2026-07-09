import logging
import typing

import aiokafka
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
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


class ConsumerSettings(BaseModel):
    """Kafka consumer configuration.

    Attributes:
        group_id: Name of the consumer group to join.
        client_id: Name passed to Kafka brokers for request logging. Default to `None`.
        group_instance_id: Unique identifier for static consumer group membership.
            Default to `None`
        fetch_max_wait_ms: Maximum time in milliseconds the broker waits to fill fetch
            requests. Default to 500 (`0.5 seconds`)
        fetch_max_bytes: Maximum bytes to fetch in a single request. Default to 52428800
        fetch_min_bytes: Minimum bytes to fetch in a single request. Default to 1
        max_partition_fetch_bytes: Maximum bytes to fetch per partition in a
            single request. Default to 1048576 (`~1 MB`)
        request_timeout_ms: Fetch request timeout in milliseconds. Default to 40000
        retry_backoff_ms: Backoff in milliseconds between retry attempts. Default to 100
        auto_offset_reset: What to do when there is no initial offset or
            the current offset is invalid. Default to `latest`
        enable_auto_commit: Whether to automatically commit offsets. Default to True
        auto_commit_interval_ms: Interval in milliseconds to automatically
            commit offsets. Default to 5000 (`5 seconds`)
        check_crcs: Whether to check CRCs on messages consumed from Kafka.
            Default to `True`
        metadata_max_age_ms: Interval in milliseconds to force metadata refresh.
            Default to 300000 (`5 minutes`)
        partition_assignment_strategy: List of partition assignor classes to
            use for consumer group coordination.
            Default to `RoundRobinPartitionAssignor`
        max_poll_interval_ms: Maximum time in milliseconds between calls to poll()
            before the consumer is considered failed. Default to 300000 (`5 minutes`)
        rebalance_timeout_ms: Time in milliseconds to wait for a rebalance to
            complete before raising an error. We decouple this setting to allow finer
            tuning by users that use
            [RebalanceListener](../stream/#kstreams.RebalanceListener)
            to delay rebalacing If not set, defaults to `session_timeout_ms`.
        session_timeout_ms: Timeout in milliseconds for consumer group session.
            Default to 10000 (`10 seconds`)
        heartbeat_interval_ms: Interval in milliseconds to send heartbeats to
            the consumer group coordinator. Default to 3000 (`3 seconds`)
        consumer_timeout_ms: Time in milliseconds to wait for messages in poll()
            before returning an empty result. Default to 200 (`0.2 seconds`)
        max_poll_records: Maximum number of records returned in a
            single call to poll(). Default to `None` (no limit)
        exclude_internal_topics: Whether to exclude internal topics when
            auto-subscribing to topics by regex. Default to `True`
        connections_max_idle_ms: Close idle connections after this timeout.
            Default to `None` (no timeout)
        isolation_level: Controls how to read messages written by
            transactional producers. Default to `read_uncommitted`

    !!! Example
        ```python
        from kstreams import create_engine, ConsumerSettings, ConsumerRecord

        consumer_settings = ConsumerSettings(
            group_id="example-group",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

        stream_engine = create_engine(title="my-stream-engine")


        @stream_engine.stream("my-topic", settings=consumer_settings)
        async def stream(cr: ConsumerRecord) -> None:
            print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")

        ```
    """

    # kafka client settings
    group_id: str
    client_id: typing.Optional[str] = None
    group_instance_id: typing.Optional[str] = None
    fetch_max_wait_ms: int = 500
    fetch_max_bytes: int = 52428800
    fetch_min_bytes: int = 1
    max_partition_fetch_bytes: int = 1048576
    request_timeout_ms: int = 40000
    retry_backoff_ms: int = 100
    auto_offset_reset: typing.Literal["latest", "earliest", "none"] = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    check_crcs: bool = True
    metadata_max_age_ms: int = 300000
    partition_assignment_strategy: typing.Tuple[typing.Type, ...] = (
        RoundRobinPartitionAssignor,
    )
    max_poll_interval_ms: int = 300000
    rebalance_timeout_ms: typing.Optional[int] = None
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000
    consumer_timeout_ms: int = 200
    max_poll_records: typing.Optional[int] = None
    exclude_internal_topics: bool = True
    connections_max_idle_ms: typing.Optional[int] = 540000
    isolation_level: typing.Literal["read_uncommitted", "read_committed"] = (
        "read_uncommitted"
    )

    # TODO: check this later! Shuld it be part of ConsumerSettings?
    #  Should we keep separeted Kstreams settings to the kafka driver?

    # name: Optional name for the consumer. If not set, defaults to generated uuid4.
    #     initial_offsets: Optional list of `TopicPartitionOffset` to set
    # initial offsets to start consuming from. Default to `None`, which means
    #     the consumer will start based on `auto_offset_reset` setting.
    # rebalance_listener (kstreams.rebalance_listener.RebalanceListener): Listener
    #     callbacks when partition are assigned or revoked
    # middlewares: Optional list of `middleware.Middleware` to apply to the consumer.
    #     error_policy: Policy to handle errors during stream processing.
    #     Default to `StreamErrorPolicy.STOP`
    # get_many: Optional `GetMany` instance to customize the behavior of
    #     the `get_many` method. Default to `None`, which means the default behavior
    #     will be used.

    # # Kstreams specific settings
    # name: typing.Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    # initial_offsets: typing.Optional[typing.List[TopicPartitionOffset]] = None
    # rebalance_listener: typing.Optional[RebalanceListener] = None
    # middlewares: typing.Optional[typing.List[Middleware]] = None
    # subscribe_by_pattern: bool = False
    # error_policy: StreamErrorPolicy = StreamErrorPolicy.STOP
    # get_many: typing.Optional[GetMany] = None

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")


class Consumer(aiokafka.AIOKafkaConsumer):
    def __init__(
        self,
        *args,
        key_deserializer: typing.Optional[typing.Callable] = None,
        **kwargs,
    ):
        self.config = kwargs

        # Move this to Middleware in the future to allow
        # users to customize it more easily
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

        # Move this to Serializer in the future to allow
        # users to customize it more easily
        if key_serializer is None:

            def key_serializer(key):
                if key is not None:
                    return key.encode("utf-8")

        super().__init__(*args, key_serializer=key_serializer, **self.config)

    def all_partitions_for_topic(self, topic: str) -> typing.List[int]:
        return list(self._metadata.partitions_for_topic(topic))

    def available_partitions_for_topic(self, topic: str) -> typing.List[int]:
        return list(self._metadata.available_partitions_for_topic(topic))
