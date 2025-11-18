import asyncio
import logging

import aiorun

from kstreams import BatchEvent, ConsumerRecord, create_engine

logger = logging.getLogger(__name__)

topic = "local--kstreams-send-many"
stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream(topic, group_id="send-many-group")
async def consume(cr: ConsumerRecord):
    logger.info(f"Event consumed: {cr} \n")


async def send_many(*, partition: int = 0):
    batch_events = []

    for id in range(5):
        batch_events.append(
            BatchEvent(
                value=f"Hello world {str(id)}!".encode(),
                key=str(id),
            )
        )

    batch_metadata = await stream_engine.send_many(
        topic, batch_events=batch_events, partition=partition
    )

    print("Batch send metadata:", batch_metadata)
    await asyncio.sleep(5)


async def start():
    await stream_engine.start()
    await send_many()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
