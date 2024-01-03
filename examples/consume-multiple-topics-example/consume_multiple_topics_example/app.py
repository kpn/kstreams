import asyncio
import json

from kstreams import ConsumerRecord, create_engine

topics = ["local--kstreams-2", "local--hello-world"]

stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream(topics, group_id="example-group")
async def consume(cr: ConsumerRecord) -> None:
    print(
        f"Event consumed from topic: {cr.topic}, "
        f"headers: {cr.headers}, payload: {cr.value}"
    )


async def produce(events_per_topic: int = 5, delay_seconds: int = 1) -> None:
    for _ in range(events_per_topic):
        for topic in topics:
            payload = json.dumps({"message": f"Hello world from topic {topic}!"})
            metadata = await stream_engine.send(topic, value=payload.encode(), key="1")
            print(f"Message sent: {metadata}")
            await asyncio.sleep(delay_seconds)


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
