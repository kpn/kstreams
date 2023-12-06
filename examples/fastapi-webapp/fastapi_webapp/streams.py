from kstreams import ConsumerRecord, stream


@stream("local--kstream", group_id="kstreams--group-id")
async def consume(cr: ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")
