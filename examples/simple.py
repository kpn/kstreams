import asyncio

from aiokafka.structs import RecordMetadata

from kstreams import ConsumerRecord, create_engine
from kstreams.streams import Stream

topic = "local--kstreams"

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


async def start():
    await stream_engine.start()
    await produce()


async def shutdown():
    await stream_engine.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(start())
        loop.run_forever()
    finally:
        loop.run_until_complete(shutdown())
        loop.close()
