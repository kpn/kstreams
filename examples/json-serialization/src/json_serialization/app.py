import asyncio
import logging

import aiorun

from kstreams import ConsumerRecord, consts, create_engine, middleware

from .serializers import JsonDeserializerMiddleware, JsonSerializer

logger = logging.getLogger(__name__)


json_data = {"message": "Hello world!"}
raw_data = b"Hello world!"
raw_topic = "local--kstreams"
json_topic = "local--kstreams-json"


stream_engine = create_engine(
    title="my-stream-engine",
    serializer=JsonSerializer(),
)


@stream_engine.stream(
    raw_topic,
    group_id="my-group-raw-data",
)
async def consume_raw(cr: ConsumerRecord):
    logger.info(f"Event consumed: headers: {cr.headers}, value: {cr.value}")
    assert cr.value == raw_data


@stream_engine.stream(
    json_topic,
    group_id="my-group-json-data",
    middlewares=[middleware.Middleware(JsonDeserializerMiddleware)],
)
async def consume_json(cr: ConsumerRecord):
    logger.info(f"Event consumed: headers: {cr.headers}, value: {cr.value}")
    assert cr.value == json_data


async def produce():
    for _ in range(5):
        # Serialize the data with APPLICATION_JSON
        metadata = await stream_engine.send(
            json_topic,
            value=json_data,
            headers={
                "content-type": consts.APPLICATION_JSON,
            },
        )
        logger.info(f"Message sent: {metadata}")
        await asyncio.sleep(3)

    # send raw data to show that it is possible to send data without serialization
    metadata = await stream_engine.send(
        raw_topic,
        value=raw_data,
        serializer=None,
    )

    logger.info(f"Message sent: {metadata}")


async def start():
    await stream_engine.start()
    await produce()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)


if __name__ == "__main__":
    main()
