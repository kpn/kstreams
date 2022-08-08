import asyncio

from aiokafka import ConsumerRecord
from aiokafka.structs import RecordMetadata

from kstreams import create_engine
from kstreams.streams import Stream

topic = "local--py-streams"

stream_engine = create_engine(title="my-stream-engine")


def on_consume(cr: ConsumerRecord):
    print(f"Value {cr.value} consumed")


def on_produce(metadata: RecordMetadata):
    print(f"Event sent. Metadata {metadata}")


@stream_engine.stream(topic, group_id="example-group")
async def consume(stream: Stream):
    async for cr in stream:
        on_consume(cr)


async def produce():
    payload = b'{"message": "Hello world!"}'

    for _ in range(5):
        metadata = await stream_engine.send(topic, value=payload, key="1")
        on_produce(metadata)
        await asyncio.sleep(2)


async def main():
    await stream_engine.start()
    await produce()
    await stream_engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
