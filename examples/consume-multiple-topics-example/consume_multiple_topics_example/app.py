import asyncio
import json
import logging

import aiorun

from kstreams import ConsumerRecord, create_engine

logger = logging.getLogger(__name__)

topics = ["local--kstreams-2", "local--hello-world"]

stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream(topics, group_id="example-group")
async def consume(cr: ConsumerRecord) -> None:
    logger.info(
        f"Event consumed from topic: {cr.topic}, "
        f"headers: {cr.headers}, payload: {cr.value}"
    )


async def produce(events_per_topic: int = 5, delay_seconds: int = 1) -> None:
    for _ in range(events_per_topic):
        for topic in topics:
            payload = json.dumps({"message": f"Hello world from topic {topic}!"})
            metadata = await stream_engine.send(topic, value=payload.encode(), key="1")
            logger.info(f"Message sent: {metadata}")
            await asyncio.sleep(delay_seconds)


async def start():
    await stream_engine.start()
    await produce()


async def shutdown(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
