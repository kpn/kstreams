import asyncio
import logging

from kstreams import Send, consts

logger = logging.getLogger(__name__)


async def produce(*, topic: str, data: dict, send: Send, total_events: int = 3) -> None:
    for _ in range(total_events):
        await asyncio.sleep(3)
        await send(
            topic,
            value=data,
            headers={
                "content-type": consts.APPLICATION_JSON,
            },
        )
        logger.info(f"Message sent to topic {topic} \n\n")
