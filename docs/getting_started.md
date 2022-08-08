You can starting using `kstreams` with simple `producers` and `consumers` and/or integrated it with any `async` framework like `FastAPI`

## Simple consumer and producer

```python title="Simple use case"
import asyncio
from kstreams import create_engine, Stream

stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream("local--py-stream", group_id="de-my-partition")
async def consume(stream: Stream):
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {value}")


async def produce():
    payload = b'{"message": "Hello world!"}'

    for i in range(5):
        metadata = await stream_engine.send("local--py-streams", value=payload, key="1")
        print(f"Message sent: {metadata}")
        await asyncio.sleep(5)


async def main():
    await stream_engine.start()
    await produce()
    await stream_engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

*(This script is complete, it should run "as is")*

## FastAPI

The following code example shows how `kstreams` can be integrated with any `async` framework like `FastAPI`. The full example can be found [here](https://github.com/kpn/kstreams/tree/master/examples/fastapi-example)


First, we need to create an `engine`:

```python title="Create the StreamEngine"
# streaming.engine.py
from kstreams import create_engine

stream_engine = create_engine(
    title="my-stream-engine",
)
```

Define the `streams`:

```python title="Application stream"
# streaming.streams.py
from .engine import stream_engine
from kstreams import Stream


@stream_engine.stream("local--kstream")
async def stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.payload}")
```

Create the `FastAPI`:

```python title="FastAPI"
# app.py
from fastapi import FastAPI
from starlette.responses import Response
from starlette_prometheus import PrometheusMiddleware, metrics

from .streaming.streams import stream_engine

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    await stream_engine.start()

@app.on_event("shutdown")
async def shutdown_event():
    await stream_engine.stop()


@app.get("/events")
async def post_produce_event() -> Response:
    payload = '{"message": "hello world!"}'

    metadata = await stream_engine.send(
        "local--kstream",
        value=payload.encode(),
    )
    msg = (
        f"Produced event on topic: {metadata.topic}, "
        f"part: {metadata.partition}, offset: {metadata.offset}"
    )

    return Response(msg)


app.add_middleware(PrometheusMiddleware, filter_unhandled_paths=True)
app.add_api_route("/metrics", metrics)
```

## Changing Kafka settings

To modify the settings of a cluster, like the servers, refer to the [backends docs](./backends.md)
