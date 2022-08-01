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
app.add_api_route("/metrics", metrics)  # type: ignore
