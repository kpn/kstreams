If you have a large project with maybe multiple `streams` we recommend the following project structure:

```
├── my-project
│   ├── my_project
│   │   ├── __init__.py
│   │   ├── app.py
│   │   ├── resources.py
│   │   ├── streams.py
│   │   └── streams_roster.py
│   │── tests
│   │   ├── __init__.py
│   │   ├── conftest.py
│   │── pyproject.toml
│   │── README.md
```

- The file `my_project/resouces.py` contains the creation of the `StreamEngine`
- The file `my_project/app.py` contains the entrypoint of your program
- The file `my_project/streams.py` contains all the `Streams`

A full project example ready to use can be found [here](https://github.com/kpn/kstreams/tree/master/examples/recommended-worker-app)

!!! note
    This is just a recommendation, there are many ways to structure your project

## Resources

This python module contains any global resource that will be used later in the application, for example `DB connections` or the `StreamEngine`. Typically we will have the following:

```python
from kstreams import backends, create_engine

backend = backends.Kafka(
    bootstrap_servers=["localhost:9092"],
    security_protocol=backends.kafka.SecurityProtocol.PLAINTEXT,
)

stream_engine = kstreams.create_engine(
    title="my-stream-engine",
    backend=backend,
)
```

Then later `stream_engine` can be reused to start the application.

## Streams

When starting your project you can have `N` number of `Streams` with its `handler`, let's say in `streams.py` module. All of the `Streams` will run next to each other and because they are in the same project it is really easy to share common code. However, this comes with a downside of `scalability` as it is not possible to take the advantages of `kafka` and scale up `Streams` individually. In next versions the `StreamEngine` will be able to select which `Stream/s` should run to mitigate this issue. Typically, your `streams.py` will look like:

```python
from kstreams import Stream

from .streams_roster import stream_roster, stream_two_roster


my_stream = Stream(
    "local--hello-world",
    func=stream_roster,
    config={
        "group_id": "example-group",
    },
    ...
)

my_second_stream = Stream(
    "local--hello-world-2",
    func=stream_two_roster,
    config={
        "group_id": "example-group-2",
    },
    ...
)

...
```

and `streams_roster.py` contains all the `coroutines` that will be executed when an event arrives

```python
import logging

from kstreams import ConsumerRecord, Send, Stream

logger = logging.getLogger(__name__)


async def stream_roster(cr: ConsumerRecord, send: Send) -> None:
    logger.info(f"showing bytes: {cr.value}")
    value = f"Event confirmed. {cr.value}"

    await send(
        "another-topic-to-wink",
        value=value.encode(),
        key="1",
    )


async def stream_two_roster(cr: ConsumerRecord, send: Send, stream: Stream) -> None:
    ...
```

It is worth to note three things:

- We separate the `Stream` with its `coroutine` to be able to test the `business logic` easily
- If you need to produce events inside a `Stream` add the `send coroutine` using [dependency-injection](https://kpn.github.io/kstreams/stream/#dependency-injection)
- We are not using `StreamEngine` at all to avoid `circular import` errors

## Application

The `entrypoint` is usually in `app.py`. The module contains the import of `stream_engine`, it's `hooks` and the `streams` to be added to the `engine`:

```python
import aiorun
import asyncio
import logging

from kstreams.stream_utils import StreamErrorPolicy

from .resources import stream_engine
from .streams import my_stream, my_second_stream

logger = logging.getLogger(__name__)


# hooks
@stream_engine.after_startup
async def init_events():
    await stream_engine.send("local--hello-world", value="Hi Kstreams!")


# add the stream to the stream_engine
stream_engine.add_stream(my_stream, error_policy=StreamErrorPolicy.RESTART)
stream_engine.add_stream(my_second_stream, error_policy=StreamErrorPolicy.STOP_ENGINE)


async def start():
    await stream_engine.start()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting application...")
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
```

To run it we recommend [aiorun](https://github.com/cjrh/aiorun). It can be also run with `asyncio` directly but `aiorun` does all the boilerplate for us.

## Tests

In this module you test your application using the `TestStreamClient`, usually provided as a `fixture` thanks to `pytest`. The package `pytest-asyncio` is also needed 
to test `async` code.

```python
# conftest.py
import pytest

from kstreams.test_utils import TestStreamClient

from my_project.resources import stream_engine


@pytest.fixture
def stream_client():
    return TestStreamClient(stream_engine=stream_engine)
```

then you can test your streams

```python
# test_app.py
import pytest


@pytest.mark.asyncio
async def test_my_stream(stream_client):
    topic = "local--hello-world"  # Use the same topic as the stream
    event = b'{"message": "Hello world!"}'

    async with stream_client:
        metadata = await stream_client.send(topic, value=event, key="1")
        assert metadata.topic == topic
```
