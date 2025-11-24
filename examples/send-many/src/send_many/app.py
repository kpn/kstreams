import asyncio
import logging

from kstreams import BatchEvent, create_engine

logger = logging.getLogger(__name__)

stream_engine = create_engine(title="my-stream-engine")


async def send_many():
    batch_events = [
        BatchEvent(
            value=f"Hello world {str(id)}!".encode(),
            key=str(id),
        )
        for id in range(5)
    ]

    return await stream_engine.send_many(
        topic="local--kstreams-send-many", batch_events=batch_events, partition=0
    )


async def start():
    await stream_engine.start()
    await send_many()
    await asyncio.sleep(6)


async def stop(_):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    asyncio.run(start())


if __name__ == "__main__":
    main()
