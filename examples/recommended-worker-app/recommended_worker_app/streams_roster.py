import logging

from kstreams import ConsumerRecord, Send

logger = logging.getLogger(__name__)


async def stream_roster(cr: ConsumerRecord, send: Send) -> None:
    logger.info(f"showing bytes: {cr.value}")
    value = f"Event confirmed. {cr.value}"

    await send(
        "local--kstreams",
        value=value.encode(),
        key="1",
    )
