import logging

from kstreams import ConsumerRecord, stream

logger = logging.getLogger(__name__)


@stream("local--hello-world", group_id="example-group")
async def consume(cr: ConsumerRecord) -> None:
    logger.info(f"showing bytes: {cr.value}")
