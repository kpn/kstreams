from kstreams import stream, ConsumerRecord


@stream("local--hello-world", group_id="example-group")
async def consume(cr: ConsumerRecord) -> None:
    print(f"showing bytes: {cr.value}")
