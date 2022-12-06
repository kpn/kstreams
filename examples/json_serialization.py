import asyncio
import json
from typing import Any, Dict, Optional

from kstreams import ConsumerRecord, Stream, consts, create_engine
from kstreams.types import Headers


class JsonSerializer:
    async def serialize(
        self,
        payload: Any,
        headers: Optional[Headers] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> bytes:
        """
        Serialize a payload to json
        """
        value = json.dumps(payload)
        return value.encode()


class JsonDeserializer:
    async def deserialize(
        self, consumer_record: ConsumerRecord, **kwargs
    ) -> ConsumerRecord:
        if consumer_record.value is not None:
            data = json.loads(consumer_record.value.decode())
            consumer_record.value = data
        return consumer_record


stream_engine = create_engine(
    title="my-stream-engine",
    serializer=JsonSerializer(),
    deserializer=JsonDeserializer(),
)

data = {"message": "Hello world!"}
topic = "local--kstreams-json"


@stream_engine.stream(topic)
async def consume(stream: Stream):
    try:
        async for cr in stream:
            print(f"Event consumed: headers: {cr.headers}, value: {cr.value}")
            assert cr.value == data
    finally:
        await stream.consumer.stop()


async def produce():
    for _ in range(5):
        # Serialize the data with APPLICATION_JSON
        metadata = await stream_engine.send(
            topic,
            value=data,
            headers={
                "content-type": consts.APPLICATION_JSON,
            },
        )
        print(f"Message sent: {metadata}")
        await asyncio.sleep(3)


async def main():
    await stream_engine.start()
    await produce()
    await stream_engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
