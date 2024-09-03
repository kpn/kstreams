import logging

from kstreams import ConsumerRecord, stream

logger = logging.getLogger(__name__)


@stream("local--kstream", group_id="kstreams--group-id")
async def consume(cr: ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")

    if cr.value == b"error":
        raise ValueError("error....")
