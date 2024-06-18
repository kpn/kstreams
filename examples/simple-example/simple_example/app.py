import asyncio
import typing
from dataclasses import dataclass, field

from kstreams import ConsumerRecord, create_engine

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

    @property
    def total(self):
        return len(self.events)


event_store = EventStore()


@stream_engine.stream(topic, group_id="example-group")
async def consume(cr: ConsumerRecord):
    print(cr)
    event_store.add(cr)


async def produce():
    payload = b'{"message": "Hello world!"}'

    for _ in range(5):
        metadata = await stream_engine.send(topic, value=payload, key="1")
        await asyncio.sleep(2)

    return metadata


async def start():
    await stream_engine.start()
    await produce()


async def shutdown():
    await stream_engine.stop()


def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(start())
        loop.run_forever()
    finally:
        loop.run_until_complete(shutdown())
        loop.close()
