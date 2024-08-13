import logging

from kstreams import ConsumerRecord, Send, stream

logger = logging.getLogger(__name__)


@stream("local--hello-world", group_id="example-group")
async def consume(cr: ConsumerRecord, send: Send) -> None:
    logger.info(f"showing bytes: {cr.value}")
    value = f"Event confirmed. {cr.value}"

    await send(
        "local--kstreams",
        value=value.encode(),
        key="1",
    )
