import asyncio
import logging
import time
import typing

from kstreams import BatchEvent, ConsumerRecord, create_engine

logger = logging.getLogger(__name__)

topic = "local--kstreams-send-many"
stream_engine = create_engine(title="my-stream-engine")

# Benchmark parameters
runs = 2
events_per_run = 100

# Prepare events for send_many and send_by_one
batch_events = [
    BatchEvent(
        value=f"Hello world {str(id)}!".encode(),
        key=str(id),
    )
    for id in range(events_per_run)
]

single_events = [f"Hello world {str(id)}!".encode() for id in range(events_per_run)]


async def time_elapsed(func: typing.Callable[[], typing.Awaitable[None]]):
    start = time.time()
    await func()
    end = time.time()
    return end - start


@stream_engine.stream(topic, group_id="send-many-group")
async def consume(cr: ConsumerRecord):
    logger.info(f"Event consumed: {cr} \n")


async def send_many():
    await stream_engine.send_many(topic, batch_events=batch_events, partition=0)


async def send_by_one():
    for event in single_events:
        await stream_engine.send(topic, value=event, partition=0)


async def start():
    await stream_engine.start()

    # Benchmark send_many vs send_by_one
    average_send_many_time = (
        sum([await time_elapsed(send_many) for _ in range(runs)]) / runs
    )
    average_send_by_one_time = (
        sum([await time_elapsed(send_by_one) for _ in range(runs)]) / runs
    )

    logger.info(f"send_many took {average_send_many_time} seconds")
    logger.info(f"send_by_one took {average_send_by_one_time} seconds")
    logger.info(
        f"send_many took {average_send_by_one_time - average_send_many_time} seconds "
        "less that send_by_one to send 200 events"
    )

    assert average_send_many_time < average_send_by_one_time, (
        "send_many should be faster than send_by_one"
    )

    # Calculate percentage difference
    speed_difference = average_send_by_one_time / average_send_many_time

    logger.info(
        f"send many event is {speed_difference}x "
        f"faster than send by one event ({(speed_difference - 1) * 100}%)\n"
    )

    await asyncio.sleep(5)


async def stop(_):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    asyncio.run(start())


if __name__ == "__main__":
    main()
