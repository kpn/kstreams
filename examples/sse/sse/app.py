import asyncio

from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from starlette.datastructures import Address

from .streaming.streams import stream_engine, stream_factory


app = FastAPI(title="streaming-sse")

@app.on_event("startup")
async def startup_event():
    await stream_engine.start()

@app.on_event("shutdown")
async def shutdown_event():
    await stream_engine.stop()


async def event_publisher(*, client: Address, topic: str, group_id: str):
    print(f"Client connected {client}")
    stream = stream_factory(topic=topic, group_id=group_id)

    async with stream as stream_flow:
        try:
            async for event in stream_flow:
                yield dict(data=event)
        except asyncio.CancelledError:
            print(f"Disconnected from client (via refresh/close) {client}")


@app.get("/topics/{topic}/{group_id}/")
async def sse(request: Request, topic: str, group_id: str):
    """Simulates and endless stream
    In case of server shutdown the running task has to be stopped
    via signal handler in order
    to enable proper server shutdown. Otherwise, there will be dangling
    tasks preventing proper shutdown.
    """
    return EventSourceResponse(
        event_publisher(client=request.client, topic=topic, group_id=group_id)
    )
