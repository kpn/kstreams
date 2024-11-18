import logging

import asyncio
from kstreams import ConsumerRecord, stream

logger = logging.getLogger(__name__)


@stream("local--kstream", group_id="kstreams--group-id")
async def consume(cr: ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")

    if cr.value == b"error":
        raise ValueError("error....")


@stream("local--hello-world", group_id="example-group-2")
async def consume_2(cr: ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    await asyncio.sleep(10)
    raise ValueError
