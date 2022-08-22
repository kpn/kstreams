from kstreams import Stream, stream


@stream("local--kstream")
async def consume(stream: Stream):
    print("consuming.....")
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")
