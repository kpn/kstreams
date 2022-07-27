import asyncio
import json

from kstreams import Stream, create_engine

topics = ["local--kstreams-2", "local--hello-world"]

stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream(topics, group_id="example-group")
async def consume(stream: Stream) -> None:
    async for cr in stream:
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


async def main():
    await stream_engine.start()
    await produce()
    await stream_engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
