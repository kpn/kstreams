from kstreams import Stream, stream


@stream("local--hello-world", group_id="example-group")
async def consume(stream: Stream) -> None:
    async for cr in stream:
        print(f"showing bytes: {cr.value}")
