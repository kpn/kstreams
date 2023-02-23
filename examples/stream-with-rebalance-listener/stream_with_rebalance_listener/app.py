import signal
from typing import List

import aiorun

import kstreams

stream_engine = kstreams.create_engine(title="my-stream-engine")


class MyListener(kstreams.RebalanceListener):
    async def on_partitions_revoked(
        self, revoked: List[kstreams.TopicPartition]
    ) -> None:
        print(f"Partition revoked {revoked} for stream {self.stream}")

    async def on_partitions_assigned(
        self, assigned: List[kstreams.TopicPartition]
    ) -> None:
        print(f"Partition assigned {assigned} for stream {self.stream}")


@stream_engine.stream(
    topics=["local--hello-world"],
    group_id="example-group",
    rebalance_listener=MyListener(),
)
async def consume(stream):
    print("Consumer started")
    try:
        async for cr in stream:
            print(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    finally:
        # Terminate the program if something fails. (aiorun will cath this signal and properly shutdown this program.)
        signal.alarm(signal.SIGTERM)


async def start():
    await stream_engine.start()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
