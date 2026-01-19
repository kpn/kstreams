import logging

import aiorun

from kstreams import BatchEvent, ConsumerRecord, GetMany, create_engine

logger = logging.getLogger(__name__)

stream_engine = create_engine(title="my-stream-engine")


total_events = 5


async def send_many():
    batch_events = [
        BatchEvent(
            value=f"Hello world {str(id)}!".encode(),
            key=str(id),
        )
        for id in range(total_events)
    ]

    metadata = await stream_engine.send_many(
        topic="local--kstreams", batch_events=batch_events, partition=0
    )

    print(f"{metadata}")


@stream_engine.stream(
    "local--kstreams",
    group_id="get-many-group",
    get_many=GetMany(max_records=total_events, timeout_ms=1000),
)
async def consume(cr: ConsumerRecord) -> None:
    print(f"Event from {cr.topic}: headers: {cr.headers}, payload: {cr.value}")


async def start():
    await stream_engine.start()
    await send_many()


async def stop(_):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)


if __name__ == "__main__":
    main()
