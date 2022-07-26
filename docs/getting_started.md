You can starting using `kstreams` with simple `producers` and `consumers` and/or integrated it with any `async` framework  like `FastAPI`

## Simple consumer and producer

```python
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

1. Create the kafka cluster: `make kafka-cluster`
2. Creation your topics: `make create-topic topic-name=local--hello-world`

```python
# streaming.engine.py
from kstreams import create_engine

stream_engine = create_engine(
    title="my-stream-engine",
)
```

```python
# app.py
from .streaming.streams import stream_engine
from fastapi import FastAPI
from starlette.responses import Response
from starlette_prometheus import metrics, PrometheusMiddleware


def create_app():
    app = FastAPI()

    add_endpoints(app)
    _setup_prometheus(app)

    @app.on_event("startup")
    async def startup_event():
        await stream_engine.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        await stream_engine.stop()

    return app


def add_endpoints(app):
    @app.get("/events")
    async def post_produce_event() -> None:
        payload = {"message": "hello world!"}

        metadata = await stream_engine.send(
            "local--kstream",
            value=payload,
        )
        msg = (
            f"Produced event on topic: {metadata.topic}, "
            f"part: {metadata.partition}, offset: {metadata.offset}"
        )
        return Response(msg)


def _setup_prometheus(app: FastAPI) -> None:
    app.add_middleware(PrometheusMiddleware, filter_unhandled_paths=True)
    app.add_api_route("/metrics", metrics)


application = create_app()
```

```python
# streaming.streams.py
from .engine import stream_engine
from kstreams import Stream


@stream_engine.stream("local--kstream")
async def stream(stream: Stream):
    print("consuming.....")
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.payload}")
```
