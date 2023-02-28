import logging
from typing import List

import aiorun

import kstreams

logging.basicConfig(level=logging.INFO)


stream_engine = kstreams.create_engine(title="my-stream-engine")


batch: List[kstreams.ConsumerRecord] = []


async def process_msg_batch(events_batch: List[kstreams.ConsumerRecord]):
    for event in events_batch:
        print(f"Event consumed: headers: {event.headers}, payload: {event.value}")


@stream_engine.stream(
    topics=["local--hello-world"],
    group_id="example-group",
    enable_auto_commit=False,  # it means that we need to call commit
)
async def consume(stream):
    global batch

    async for event in stream:
        # Not need to catch the error `CommitFailedError`
        # as the steam rebalance listener is the
        # default one KstreamsRebalanceListener
        batch.append(event)
        if len(batch) == 10:
            await process_msg_batch(batch)
            await stream.commit()
            batch = []


async def start():
    await stream_engine.start()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
