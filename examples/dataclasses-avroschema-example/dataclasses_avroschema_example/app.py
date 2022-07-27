from . import serializers
from .models import Address, User
from kstreams import create_engine, Stream

import asyncio

user_topic = "local--avro-user"
address_topic = "local--avro-address"

stream_engine = create_engine(
    title="my-stream-engine",
    value_serializer=serializers.AvroSerializer(),
)


@stream_engine.stream(
    user_topic, value_deserializer=serializers.AvroDeserializer(model=User)
)
async def user_stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed on topic {user_topic}. The user is {cr.value}")


@stream_engine.stream(
    address_topic, value_deserializer=serializers.AvroDeserializer(model=Address)
)
async def address_stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed on topic {address_topic}. The Address is {cr.value}")


async def produce():
    for _ in range(5):
        await stream_engine.send(
            user_topic,
            value=User.fake(),
        )
        await stream_engine.send(
            address_topic,
            value=Address.fake(),
        )

        await asyncio.sleep(3)


async def start():
    await stream_engine.start()
    await produce()
    await stream_engine.stop()


def main():
    asyncio.run(start())
