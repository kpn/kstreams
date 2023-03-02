# Streams

A `Stream` in `kstreams` is an extension of [AIOKafkaConsumer](https://aiokafka.readthedocs.io/en/stable/consumer.html)

Consuming can be done using `kstreams.Stream`. You only need to decorate a `coroutine` with `@stream_engine.streams`. The decorator has the same  [aiokafka consumer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafkaconsumer-class) API at initialization, in other words they accept the same `args` and `kwargs` that the `aiokafka consumer` accepts.

::: kstreams.streams.Stream
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false

## Creating a Stream instance

If for any reason you need to create `Streams` instances directly, you can do it without using the decorator `stream_engine.stream`.

```python title="Stream instance"
import aiorun
from kstreams import create_engine, Stream, ConsumerRecord

stream_engine = create_engine(title="my-stream-engine")


class MyDeserializer:

    async def deserialize(self, consumer_record: ConsumerRecord, **kwargs):
        return consumer_record.value.decode()


async def stream(stream: Stream) -> None:
    async for cr in stream:
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

If your stream `crashes` for any reason, the event consumption will stop meaning that non event will be consumed from the `topic`.
As an end user you are responsable of deciding what to do. In future version approaches like `re-try`, `stream engine stops on stream crash` might be introduced.

```python title="Crashing example"
import aiorun
from kstreams import create_engine

stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream("local--kstreams", group_id="de-my-partition")
async def stream(stream: Stream) -> None:
    async for cr in stream:
        print(f"Event consumed. Payload {cr.payload}")


async def produce():
    await stream_engine.send(
        "local--kstreams",
        value=b"Hi"
    )


async def start():
    await stream_engine.start()
    await produce()


async def shutdown(loop):
    await stream_engine.stop()


if __name__ == "__main__":
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
```

```bash
CRASHED Stream!!! Task <Task pending name='Task-23' coro=<BaseStream.start.<locals>.func_wrapper() running at /Users/Projects/kstreams/kstreams/streams.py:55>>

 'ConsumerRecord' object has no attribute 'payload'
Traceback (most recent call last):
  File "/Users/Projects/kstreams/kstreams/streams.py", line 52, in func_wrapper
    await self.func(self)
  File "/Users/Projects/kstreams/examples/fastapi_example/streaming/streams.py", line 9, in stream
    print(f"Event consumed: headers: {cr.headers}, payload: {cr.payload}")
AttributeError: 'ConsumerRecord' object has no attribute 'payload'
```

## Consuming from multiple topics

Consuming from multiple topics using one `stream` is possible. A `List[str]` of topics must be provided.

```python title="Consume from multiple topics"
stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream(["local--kstreams", "local--hello-world"], group_id="example-group")
async def consume(stream: Stream) -> None:
    async for cr in stream:
        print(f"Event consumed from topic {cr.topic}: headers: {cr.headers}, payload: {cr.value}")
```

## Changing consumer behavior

Most of the time you will only set the `topic` and the `group_id` to the `consumer`, but sometimes you might want more control over it, for example changing the `policy for resetting offsets on OffsetOutOfRange errors` or `session timeout`. To do this, you have to use the same `kwargs` as the [aiokafka consumer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafkaconsumer-class) API

```python
# The consumer sends periodic heartbeats every 500 ms
# On OffsetOutOfRange errors, the offset will move to the oldest available message (‘earliest’)

@stream_engine.stream("local--kstream", group_id="de-my-partition", session_timeout_ms=500, auto_offset_reset"earliest")
async def stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")
```

## Manual commit

When processing more sensitive data and you want to be sure that the `kafka offeset` is commited once that you have done your tasks, you can use `enable_auto_commit=False` mode of Consumer.

```python title="Manual commit example"
@stream_engine.stream("local--kstream", group_id="de-my-partition", enable_auto_commit=False)
async def stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")

        # We need to make sure that the pyalod was stored before commiting the kafka offset
        await store_in_database(payload)
        await stream.consumer.commit()  # You need to commit!!!
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
async def stream(stream: Stream):
    async for cr in stream:
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
