import asyncio
import logging

import aiorun

from .resources import stream_engine
from .streams import consume

logger = logging.getLogger(__name__)

stream_engine.add_stream(consume)


async def start():
    await stream_engine.start()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting application...")
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
