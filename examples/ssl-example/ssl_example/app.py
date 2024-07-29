import asyncio
import logging

import aiorun

from .resources import stream_engine
from .streams import my_stream

logger = logging.getLogger(__name__)


@stream_engine.after_startup
async def produce():
    for event_number in range(0, 5):
        await stream_engine.send("local--kstreams", value=b"hello SSL")
        logger.info(f"Producing event {event_number}")


stream_engine.add_stream(my_stream)


async def start():
    await stream_engine.start()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting application...")
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
