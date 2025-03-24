import asyncio
import logging

import aiorun

import kstreams
from kstreams.consts import StreamErrorPolicy

logger = logging.getLogger(__name__)

stream_engine = kstreams.create_engine(title="my-stream-engine")


@stream_engine.stream(
    topics=["local--hello-world"],
    group_id="example-group",
    error_policy=StreamErrorPolicy.STOP_APPLICATION,
)
async def consume(cr: kstreams.ConsumerRecord):
    logger.info(f"Event consumed: headers: {cr.headers}, payload: {cr}")

    if cr.value == b"error":
        raise ValueError("error....")


@stream_engine.stream(topics=["local--kstream"], group_id="example-group-2")
async def consume_2(cr: kstreams.ConsumerRecord):
    logger.info(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    await asyncio.sleep(20)
    logger.info("Event finished...")


async def start():
    await stream_engine.start()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
