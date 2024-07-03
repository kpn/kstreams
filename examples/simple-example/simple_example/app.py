import asyncio
import logging
import typing
from dataclasses import dataclass, field

import aiorun

from kstreams import ConsumerRecord, create_engine

logger = logging.getLogger(__name__)

topic = "local--kstreams-test"
stream_engine = create_engine(title="my-stream-engine")


@dataclass
class EventStore:
    """
    Store events in memory
    """

    events: typing.List[ConsumerRecord] = field(default_factory=list)

    def add(self, event: ConsumerRecord) -> None:
        self.events.append(event)

    def remove(self) -> None:
        self.events.pop(0)

    @property
    def total(self) -> int:
        return len(self.events)

    def clean(self) -> None:
        self.events = []


event_store = EventStore()


@stream_engine.stream(topic, group_id="example-group")
async def consume(cr: ConsumerRecord):
    logger.info(f"Event consumed: {cr} \n")

    if cr.value == b"remove-event":
        event_store.remove()
    else:
        event_store.add(cr)

    await stream_engine.send("local--hello-world", value=cr.value)


async def produce():
    payload = b'{"message": "Hello world!"}'

    for _ in range(5):
        metadata = await stream_engine.send(topic, value=payload, key="1")
        await asyncio.sleep(2)

    return metadata


async def start():
    await stream_engine.start()
    await produce()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
