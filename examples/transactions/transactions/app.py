import asyncio
import logging

import aiorun

from .engine import stream_engine
from .producer import produce
from .streams import consume_from_transaction, consume_json

logger = logging.getLogger(__name__)

stream_engine.add_stream(consume_from_transaction)
stream_engine.add_stream(consume_json)


async def start():
    await stream_engine.start()
    await produce(
        topic="local--kstreams-json",
        data={"message": "Hello world!"},
        send=stream_engine.send,
    )


async def shutdown(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)


if __name__ == "__main__":
    start()
