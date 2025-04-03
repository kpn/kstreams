# Streams

A `Stream` in `kstreams` is an extension of [AIOKafkaConsumer](https://aiokafka.readthedocs.io/en/stable/consumer.html)

Consuming can be done using `kstreams.Stream`. You only need to decorate a `coroutine` with `@stream_engine.streams`. The decorator has the same  [aiokafka consumer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafkaconsumer-class) API at initialization, in other words they accept the same `args` and `kwargs` that the `aiokafka consumer` accepts.

::: kstreams.streams.Stream
    options:
        show_root_heading: true
        docstring_section_style: table
        show_source: false
        members:
          - ""

## Dependency Injection

The old way to itereate over a stream is with the `async for _ in stream` loop. The iterable approach works but in most cases end users are interested only in the `ConsumerRecord`,
for this reason it is possible to remove the `async for loop` using proper `typing hints`. The available `typing hints` are:

- `ConsumerRecord`: The `aiokafka` ConsumerRecord that will be received every time that a new event is in the `Stream`
- `Stream`: The `Stream` object that is subscribed to the topic/s. Useful when `manual` commit is enabled or when other `Stream` operations are needed
- `Send`: Coroutine to produce events. The same as `stream_engine.send(...)`

if you use `type hints` then every time that a new event is in the stream the `coroutine` function defined by the end user will ba `awaited` with the specified types

=== "ConsumerRecord"
    ```python
    @stream_engine.stream(topic)
    async def my_stream(cr: ConsumerRecord):
        print(cr.value)
    ```

=== "ConsumerRecord and Stream"
    ```python
    @stream_engine.stream(topic, enable_auto_commit=False)
    async def my_stream(cr: ConsumerRecord, stream: Stream):
        print(cr.value)
        await stream.commit()
    ```

=== "ConsumerRecord, Stream and Send"
    ```python
    @stream_engine.stream(topic, enable_auto_commit=False)
    async def my_stream(cr: ConsumerRecord, stream: Stream, send: Send):
        print(cr.value)
        await stream.commit()
        await send("sink-to-elastic-topic", value=cr.value)
    ```

=== "Old fashion"
    ```python
    @stream_engine.stream(topic)
    async def consume(stream):  # you can specify the type but it will be the same result
        async for cr in stream:
            print(cr.value)
            # you can do something with the stream as well!!
    ```

!!! Note
    The type arguments can be in `any` order. This might change in the future.

!!! warning
    It is still possible to use the `async for in` loop, but it might be removed in the future. Migrate to the typing approach

## Creating a Stream instance

If for any reason you need to create `Streams` instances directly, you can do it without using the decorator `stream_engine.stream`.

```python title="Stream instance"
import aiorun
from kstreams import create_engine, Stream, ConsumerRecord

stream_engine = create_engine(title="my-stream-engine")


class MyDeserializer:

    async def deserialize(self, consumer_record: ConsumerRecord, **kwargs):
        return consumer_record.value.decode()


async def stream(cr: ConsumerRecord) -> None:
    print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")


stream = Stream(
    "local--kstreams",
    name="my-stream"
    func=stream,  # coroutine or async generator
    deserializer=MyDeserializer(),
)
# add the stream to the engine
stream_engine.add_stream(stream)


async def start():
    await stream_engine.start()
    await produce()


async def shutdown(loop):
    await stream_engine.stop()


if __name__ == "__main__":
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
```

### Removing a stream from the engine

```python title="Removing stream"
stream_engine.remove_stream(stream)
```

### Starting the stream with initial offsets

If you want to start your consumption from certain offsets, you can include that in your stream instantiation.

Use case:
This feature is useful if one wants to manage their own offsets, rather than committing consumed offsets to Kafka.
When an application manages its own offsets and tries to start a stream, we start the stream using the initial
offsets as defined in the database.

If you try to seek on a partition or topic that is not assigned to your stream, the code will ignore the seek
and print out a warning. For example, if you have two consumers that are consuming from different partitions,
and you try to seek for all of the partitions on each consumer, each consumer will seek for the partitions
it has been assigned, and it will print out a warning log for the ones it was not assigned.

If you try to seek on offsets that are not yet present on your partition, the consumer will revert to the auto_offset_reset
config. There will not be a warning, so be aware of this.

Also be aware that when your application restarts, it most likely will trigger the initial_offsets again.
This means that setting intial_offsets to be a hardcoded number might not get the results you expect.

```python title="Initial Offsets from Database"
from kstreams import Stream, structs


topic_name = "local--kstreams"
db_table = ExampleDatabase()
initial_offset = structs.TopicPartitionOffset(topic=topic_name, partition=0, offset=db_table.offset)


async def my_stream(stream: Stream):
    ...


stream = Stream(
    topic_name,
    name="my-stream",
    func=my_stream,  # coroutine or async generator
    deserializer=MyDeserializer(),
    initial_offsets=[initial_offset],
)
```

## Stream crashing

If your stream `crashes` for any reason the event consumption is stopped, meaning that non event will be consumed from the `topic`. However, it is possible to set three different `error policies` per stream:

- `StreamErrorPolicy.STOP` (**default**): Stop the `Stream` when an exception occurs. The exception is raised after the stream is properly stopped.
- `StreamErrorPolicy.RESTART`: Stop and restart the `Stream` when an exception occurs. The event that caused the exception is skipped. The exception is *NOT raised* because the application should contine working, however `logger.exception()` is used to alert the user.
- `StreamErrorPolicy.STOP_ENGINE`: Stop the `StreamEngine` when an exception occurs. The exception is raised after *ALL* the Streams were properly stopped.
- `StreamErrorPolicy.STOP_APPLICATION`: Stop the `StreamEngine` when an exception occurs and raises `signal.SIGTERM` to make sure that the `application` is finished.

In the following example, the `StreamErrorPolicy.RESTART` error policy is specifed. If the `Stream` crashed with the `ValueError` exception it is restarted:

```python
from kstreams import create_engine, ConsumerRecord
from kstreams.consts import StreamErrorPolicy

stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream(
    "local--hello-world",
    group_id="example-group",
    error_policy=StreamErrorPolicy.RESTART
)
async def stream(cr: ConsumerRecord) -> None:
    if cr.key == b"error":
        # Stream will be restarted after the ValueError is raised
        raise ValueError("error....")

    print(f"Event consumed. Payload {cr.value}")
```

We can see the logs:

```bash
ValueError: error....
INFO:aiokafka.consumer.group_coordinator:LeaveGroup request succeeded
INFO:aiokafka.consumer.consumer:Unsubscribed all topics or patterns and assigned partitions
INFO:kstreams.streams:Stream consuming from topics ['local--hello-world'] has stopped!!! 


INFO:kstreams.middleware.middleware:Restarting stream <kstreams.streams.Stream object at 0x102d44050>
INFO:aiokafka.consumer.subscription_state:Updating subscribed topics to: frozenset({'local--hello-world'})
...
INFO:aiokafka.consumer.group_coordinator:Setting newly assigned partitions {TopicPartition(topic='local--hello-world', partition=0)} for group example-group
```

!!! note
    If you are using `aiorun` with `stop_on_unhandled_errors=True` and the `error_policy` is `StreamErrorPolicy.RESTART` then the `application` will NOT stop as the exception that caused the `Stream` to `crash` is not `raised`

## Changing consumer behavior

Most of the time you will only set the `topic` and the `group_id` to the `consumer`, but sometimes you might want more control over it, for example changing the `policy for resetting offsets on OffsetOutOfRange errors` or `session timeout`. To do this, you have to use the same `kwargs` as the [aiokafka consumer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafkaconsumer-class) API

```python
# The consumer sends periodic heartbeats every 500 ms
# On OffsetOutOfRange errors, the offset will move to the oldest available message (‘earliest’)

@stream_engine.stream("local--kstream", group_id="de-my-partition", session_timeout_ms=500, auto_offset_reset"earliest")
async def stream(cr: ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")
```

## Manual commit

When processing more sensitive data and you want to be sure that the `kafka offeset` is commited once that you have done your tasks, you can use `enable_auto_commit=False` mode of Consumer.

```python title="Manual commit example"
@stream_engine.stream("local--kstream", group_id="de-my-partition", enable_auto_commit=False)
async def stream(cr: ConsumerRecord, stream: Stream):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")

    # We need to make sure that the pyalod was stored before commiting the kafka offset
    await store_in_database(payload)
    await stream.commit()  # You need to commit!!!
```

!!! note
    This is a tradeoff from at most once to at least once delivery, to achieve exactly once you will need to save offsets in the destination database and validate those yourself.

## Yield from stream

Sometimes is useful to `yield` values from a `stream` so you can consume events in your on phase or because you want to return results to the frontend (SSE example).
If you use the `yield` keyword inside a `coroutine` it will be "transform" to a  `asynchronous generator function`, meaning that inside there is an `async generator` and it can be consumed.

Consuming an `async generator` is simple, you just use the `async for in` clause. Because consuming events only happens with the `for loop`, you have to make sure that the `Stream` has been started properly and after leaving the `async for in` the `stream` has been properly stopped.

To facilitate the process, we have `context manager` that makes sure of the `starting/stopping` process.

```python title="Yield example"
# Create your stream
@stream_engine.stream("local--kstream")
async def stream(cr: ConsumerRecord, stream: Stream):
    yield cr.value


# Consume the stream:
async with stream as stream_flow:  # Use the context manager
    async for value in stream_flow:
        ...
        # do something with value (cr.value)
```

!!! note
    If for some reason you interrupt the "async for in" in the async generator, the Stream will stopped consuming events
    meaning that the lag will increase.

!!! note
    Yield from a stream only works with the [typing approach](https://kpn.github.io/kstreams/stream/#dependency-injection-and-typing)

## Get many

::: kstreams.streams.Stream.getmany
    options:
        docstring_section_style: table
        show_signature_annotations: false

!!! warning
    This approach does not works with `Dependency Injection`.

## Rebalance Listener

For some cases you will need a `RebalanceListener` so when partitions are `assigned` or `revoked` to the stream different accions can be performed.

### Use cases

- Cleanup or custom state save on the start of a rebalance operation
- Saving offsets in a custom store when a partition is `revoked`
- Load a state or cache warmup on completion of a successful partition re-assignment.

### Metrics Rebalance Listener

Kstreams use a default listener for all the streams to clean the metrics after a rebalance takes place

::: kstreams.MetricsRebalanceListener
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false
        show_bases: false

### Manual Commit

If `manual` commit is enabled, you migh want to use the `ManualCommitRebalanceListener`. This `rebalance listener` will call `commit`
before the `stream` partitions are revoked to avoid the error `CommitFailedError` and *duplicate* message delivery after a rebalance. See code [example](https://github.com/kpn/kstreams/tree/master/examples/stream-with-manual-commit) with
manual `commit`

::: kstreams.ManualCommitRebalanceListener
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false
        show_bases: false

!!! note
    `ManualCommitRebalanceListener` also includes the `MetricsRebalanceListener` funcionality.

### Custom Rebalance Listener

If you want to define a custom `RebalanceListener`, it has to inherits from `kstreams.RebalanceListener`.

::: kstreams.RebalanceListener
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false
        show_bases: false

!!! note
    It also possible to inherits from `ManualCommitRebalanceListener` and `MetricsRebalanceListener`
