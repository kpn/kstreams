from kstreams import Stream

from .engine import stream_engine


@stream_engine.stream("local--kstream")
async def stream(stream: Stream):
    print("consuming.....")
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")
