# Testing

To test `streams` and `producers` or perform `e2e` tests you can make use of the `test_utils.TestStreamClient`.

The `TestStreamClient` aims to emulate as much as possible the `kafka` behaviour using `asyncio.Queue`. This is excellent because you can test quite easily your code without spinning up `kafka`, but this comes with some limitations. It is not possible to know beforehand how many topics exist, how many partitions per topic exist, the replication factor, current offsets, etc. So, the `test client` will create `topics`, `partitions`, `assigments`, etc on runtime. Each `Stream` in your application will have assigned 3 partitions per topic by default (0, 1 and 2) during *`test environment`*

With the `test client` you can:

- Send events so you won't need to mock the `producer`
- Call the consumer code, then the client will make sure that all the events are consumed before leaving the `async context`

## Using `TestStreamClient`

Import `TestStreamClient`.

Create a `TestStreamClient` by passing the **engine** instance to it.

Create functions with a name that starts with `test_` (this is standard `pytest` conventions).

Use the `TestStreamClient` object the same way as you do with `engine`.

Write simple `assert` statements with the standard Python expressions that you need to check (again, standard `pytest`).

## Example

Let's assume that you have the following code example. The goal is to store all the consumed events in an `EventStore` for future analysis.

```python
# example.py
import aiorun
import typing
from dataclasses import dataclass, field

from kstreams import ConsumerRecord, create_engine
from kstreams.streams import Stream

topic = "local--kstreams"

stream_engine = create_engine(title="my-stream-engine")


@dataclass
class EventStore:
    """
    Store events in memory
    """
    events: typing.List[ConsumerRecord] = field(default_factory=list)

    def add(self, event: ConsumerRecord) -> None:
        self.events.append(event)

    @property
    def total(self):
        return len(self.events)


event_store = EventStore()


@stream_engine.stream(topic, group_id="example-group")
async def consume(cr: ConsumerRecord):
    event_store.add(cr)


async def produce():
    payload = b'{"message": "Hello world!"}'

    for _ in range(5):
        await stream_engine.send(topic, value=payload, key="1")
        await asyncio.sleep(2)


async def start():
    await stream_engine.start()
    await produce()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
```

Then you could have a `test_stream.py` file to test the code, you need to instanciate the `TestStreamClient` with the `engine`:

```python
# test_stream.py
import pytest
from kstreams.test_utils import TestStreamClient

from example import stream_engine, event_store

client = TestStreamClient(stream_engine)


@pytest.mark.asyncio
async def test_add_event_on_consume():
    """
    Produce some events and check that the EventStore is updated.
    """
    topic = "local--kstreams"  # Use the same topic as the stream
    event = b'{"message": "Hello world!"}'

    async with client:
        metadata = await client.send(topic, value=event, key="1")  # send the event with the test client
        current_offset = metadata.offset
        assert metadata.topic == topic

        # send another event and check that the offset was incremented
        metadata = await client.send(topic, value=b'{"message": "Hello world!"}', key="1")
        assert metadata.offset == current_offset + 1

    # check that the event_store has 2 events stored
    assert event_store.total == 2
```

!!! Note
    Notice that the `produce` coroutine is not used to send events in the test case.
    The `TestStreamClient.send` coroutine is used instead.
    This allows to test `streams` without having producer code in your application

### Testing the Commit

In some cases your stream will commit, in this situation checking the commited partitions can be useful.

```python
import pytest
from kstreams.test_utils import TestStreamClient
from kstreams import ConsumerRecord, Stream, TopicPartition

from .example import produce, stream_engine

topic_name = "local--kstreams-marcos"
value = b'{"message": "Hello world!"}'
name = "my-stream"
key = "1"
partition = 2
tp = TopicPartition(
    topic=topic_name,
    partition=partition,
)
total_events = 10

@stream_engine.stream(topic_name, name=name)
async def my_stream(cr: ConsumerRecord, stream: Stream):
    # commit every time that an event arrives
    await stream.commit({tp: cr.offset})


# test the code
client = TestStreamClient(stream_engine)

@pytest.mark.asyncio
async def test_consumer_commit(stream_engine: StreamEngine):
    async with client:
        for _ in range(0, total_events):
            await client.send(topic_name, partition=partition, value=value, key=key)

        # check that everything was commited
        stream = stream_engine.get_stream(name)
        assert (await stream.committed(tp)) == total_events
```

### E2E test

In the previous code example the application produces to and consumes from the same topic, then `TestStreamClient.send` is not needed because the `engine.send` is producing. For those situation you can just use your `producer` code and check that certain code was called.

```python
# test_example.py
import pytest
from kstreams.test_utils import TestStreamClient

from .example import produce, stream_engine

client = TestStreamClient(stream_engine)


@pytest.mark.asyncio
async def test_e2e_example():
    """
    Test that events are produce by the engine and consumed by the streams
    """
    with patch("example.on_consume") as on_consume, patch("example.on_produce") as on_produce:
        async with client:
            await produce()

    on_produce.call_count == 5
    on_consume.call_count == 5
```

## Producer only

In some scenarios, your application will only produce events and other application/s will consume it, but you want to make sure that
the event was procuced in a proper way and the `topic` contains that `event`.

```python
# producer_example.py
from kstreams import create_engine
import aiorun
import asyncio

stream_engine = create_engine(title="my-stream-engine")


async def produce(topic: str, value: bytes, key: str):
    # This could be a complicated function or something like a FastAPI view
    await stream_engine.send(topic, value=value, key=key)


async def start():
    await stream_engine.start()
    await produce()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
```

Then you could have a `test_producer_example.py` file to test the code:

```python
# test_producer_example.py
import pytest
from kstreams.test_utils import TestStreamClient

from producer_example import stream_engine, produce

client = TestStreamClient(stream_engine)


@pytest.mark.asyncio
async def test_event_produced():
    topic_name = "local--kstreams"
    value = b'{"message": "Hello world!"}'
    key = "1"

    async with client:
        await produce(topic=topic_name ,value=value, key=key) # use the produce code to send events

        # check that the event was placed in a topic in a proper way
        consumer_record = await client.get_event(topic_name=topic_name)

        assert consumer_record.value == value
        assert consumer_record.key == key
```

!!! Note
    Even thought the previous example is using a simple `produce` function,
    it shows what to do when the `procuder code` is encapsulated in other functions,
    for example a `FastAPI` view.
    Then you don't want to use `client.send` directly, just called the function that contains `stream_engine.send(...)`

## Disabling monitoring during testing

Monitoring streams and producers is vital for streaming application but it requires extra effort. Sometimes during testing,
monitoring is not required as we only want to focus on testing the buisness logic. In order to disable monitoring
during testing use:

```python
client = TestStreamClient(stream_engine, monitoring_enabled=False)
```
