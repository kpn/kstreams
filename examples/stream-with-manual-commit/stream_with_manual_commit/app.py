import logging
from dataclasses import dataclass, field
from typing import List, Set

import aiorun

import kstreams

logging.basicConfig(level=logging.INFO)

stream_engine = kstreams.create_engine(title="my-stream-engine")


@dataclass
class BatchProcessor:
    batch: List[kstreams.ConsumerRecord] = field(default_factory=list)

    def add_event(self, event: kstreams.ConsumerRecord) -> None:
        self.batch.append(event)

    def should_process(self) -> bool:
        return len(self.batch) == 5

    async def process(self) -> None:
        for event in self.batch:
            print(f"Event consumed: headers: {event.headers}, payload: {event.value}")
        self.batch = []


class MyListener(kstreams.ManualCommitRebalanceListener):
    def __init__(self, batch_processor: BatchProcessor) -> None:
        self.batch_processor = batch_processor
        super().__init__()

    async def on_partitions_revoked(
        self, revoked: Set[kstreams.TopicPartition]
    ) -> None:
        await self.batch_processor.process()
        await super().on_partitions_revoked(revoked=revoked)


batch_processor = BatchProcessor()
rebalance_listener = MyListener(batch_processor=batch_processor)


@stream_engine.stream(
    topics=["local--hello-world"],
    group_id="example-group",
    enable_auto_commit=False,  # it means that we need to call commit
    rebalance_listener=rebalance_listener,
)
async def consume(stream):
    async for event in stream:
        # Not need to catch the error `CommitFailedError`
        # as the steam rebalance listener is the
        # default one KstreamsRebalanceListener
        batch_processor.add_event(event)
        if batch_processor.should_process():
            await batch_processor.process()
            await stream.commit()


async def start():
    await stream_engine.start()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
