import logging

from kstreams import ConsumerRecord, stream

logger = logging.getLogger(__name__)


@stream("local--kstreams", group_id="example-group")
async def my_stream(cr: ConsumerRecord) -> None:
    logger.info(f"showing bytes: {cr.value}")
