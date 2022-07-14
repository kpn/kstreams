from .engine import stream_engine
from kstreams import Stream


@stream_engine.stream("dev-kpn-des--kstream")
async def stream(stream: Stream):
    print("consuming.....")
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")
