To test your `streams` or perform `e2e` tests you can make use of the `test_utils.TestStreamClient`.
The `TestStreamClient` you can send events so you won't need a `producer`

Let's assume that you have the following code example:

```python
# simple.py
from kstreams import create_engine
import asyncio

topic = "dev-kpn-des--kstreams"
stream_engine = create_engine(title="my-stream-engine")


def on_consume(value):
    print(f"Value {value} consumed")
    return value


def on_produce(metadata):
    print(f"Metadata {metadata} sent")
    return metadata


@stream_engine.stream(topic, group_id="example-group")
async def consume(stream: Stream):
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")
        on_consume(value)


async def produce():
    payload = b'{"message": "Hello world!"}'

    for _ in range(5):
        metadata = await stream_engine.send(topic, value=payload, key="1")
        print(f"Message sent: {metadata}")
        on_produce(metadata)


async def main():
    await stream_engine.init_streaming()
    await produce()
    await stream_engine.stop_streaming()


if __name__ == "__main__":
    asyncio.run(main())
```

### Test stream using the TestStreamClient

```python
# test_stream.py
import pytest
from kstreams.test_utils import TestStreamClient


@pytest.mark.asyncio
async def test_streams_consume_events():
    topic = "dev-kpn-des--kstreams"  # Use the same topic as the stream
    event = b'{"message": "Hello world!"}'

    with patch("example.on_consume") as on_consume:
        async with TestStreamClient() as test_client:
            metadata = await test_client.send(topic, value=event, key="1")  # send the event with the test client
            current_offset = metadata.offset
            assert metadata.topic == topic

            # send another event and check that the offset was incremented
            metadata = await test_client.send(topic, value=b'{"message": "Hello world!"}', key="1")
            assert metadata.offset == current_offset + 1

    # check that the event was consumed
    on_consume.assert_called()
```

### E2E test

```python
# test_example.py
import pytest
from kstreams.test_utils import TestStreamClient

from .example import produce


@pytest.mark.asyncio
async def test_e2e_example():
    """
    Test that events are produce by the engine and consumed by the streams
    """
    with patch("example.on_consume") as on_consume, patch("example.on_produce") as on_produce:
        async with TestStreamClient():
            await produce()

    on_produce.call_count == 5
    on_consume.call_count == 5
```