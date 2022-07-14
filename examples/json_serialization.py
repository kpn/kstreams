from kstreams import consts, create_engine, Stream
from kstreams.custom_types import Headers
from typing import Any, Dict, Optional

import aiokafka
import asyncio
import json


class JsonSerializer:
    async def serialize(
        self,
        payload: Any,
        headers: Optional[Headers] = None,
        value_serializer_kwargs: Optional[Dict] = None,
    ) -> bytes:
        """
        Serialize a payload to json
        """
        value = json.dumps(payload)
        return value.encode()


class JsonDeserializer:
    async def deserialize(
        self, consumer_record: aiokafka.structs.ConsumerRecord, **kwargs
    ) -> aiokafka.structs.ConsumerRecord:
        data = json.loads(consumer_record.value.decode())
        consumer_record.value = data
        return consumer_record


stream_engine = create_engine(
    title="my-stream-engine",
    value_serializer=JsonSerializer(),
    value_deserializer=JsonDeserializer(),
)

data = {"message": "Hello world!"}
topic = "dev-kpn-des--kstreams-json"


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
    await stream_engine.init_streaming()
    await produce()
    await stream_engine.stop_streaming()


if __name__ == "__main__":
    asyncio.run(main())
