import asyncio

import aiorun

from kstreams import ConsumerRecord, create_engine, middleware

from .middlewares import AvroDeserializerMiddleware
from .models import Address, User
from .serializers import AvroSerializer

user_topic = "local--avro-user"
address_topic = "local--avro-address"

stream_engine = create_engine(
    title="my-stream-engine",
    serializer=AvroSerializer(),
)


@stream_engine.stream(
    user_topic,
    middlewares=[middleware.Middleware(AvroDeserializerMiddleware, model=User)],
)
async def user_stream(cr: ConsumerRecord):
    print(f"Event consumed on topic {user_topic}. The user is {cr.value}")


@stream_engine.stream(
    address_topic,
    middlewares=[middleware.Middleware(AvroDeserializerMiddleware, model=Address)],
)
async def address_stream(cr: ConsumerRecord):
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


async def shutdown(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
