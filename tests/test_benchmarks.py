from typing import Callable, List

from kstreams.engine import StreamEngine
from kstreams.streams import Stream
from kstreams.types import ConsumerRecord, Send


async def bench_processing_single_consumer_record(
    stream_engine: StreamEngine,
    cr: ConsumerRecord,
):
    """
    Benchmark the processing of a single consumer record being injected.

    This function benchmarks the processing of a single consumer record by
    using a stream engine to consume the record from a specified topic.
    """

    @stream_engine.stream("local--kstreams")
    async def consume_bench(cr: ConsumerRecord):
        return cr.topic

    r = await consume_bench.func(cr)
    return r


async def bench_inject_all_deps(
    stream_engine: StreamEngine,
    cr: ConsumerRecord,
):
    """
    Benchmark injecting all parameters.

    This function benchmarks the processing of a single consumer record by
    using a stream engine to consume the record from a specified topic.
    """

    @stream_engine.stream("local--kstreams")
    async def consume_bench(cr: ConsumerRecord, stream: Stream, send: Send):
        return cr.topic

    r = await consume_bench.func(cr)
    return r


async def bench_consume_many(
    stream_engine: StreamEngine,
    crs: List[ConsumerRecord],
):
    """
    Benchmark injecting all parameters.

    This function benchmarks the processing of a single consumer record by
    using a stream engine to consume the record from a specified topic.
    """

    @stream_engine.stream("local--kstreams")
    async def consume_bench(cr: ConsumerRecord, stream: Stream, send: Send):
        return cr.topic

    for cr in crs:
        _ = await consume_bench.func(cr)


def test_processing_single_consumer_record(
    stream_engine: StreamEngine,
    consumer_record_factory: Callable[..., ConsumerRecord],
    aio_benchmark,
):
    cr = consumer_record_factory()

    aio_benchmark(
        bench_processing_single_consumer_record,
        stream_engine,
        cr,
    )


def test_inject_all(
    stream_engine: StreamEngine,
    consumer_record_factory: Callable[..., ConsumerRecord],
    aio_benchmark,
):
    cr = consumer_record_factory()

    aio_benchmark(
        bench_inject_all_deps,
        stream_engine,
        cr,
    )


def test_consume_many(
    stream_engine: StreamEngine,
    consumer_record_factory: Callable[..., ConsumerRecord],
    aio_benchmark,
):
    crs = [consumer_record_factory() for _ in range(1000)]

    aio_benchmark(
        bench_consume_many,
        stream_engine,
        crs,
    )
