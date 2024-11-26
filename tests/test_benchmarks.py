"""
This file contians benchmarks for kstreams.

Interpreting the data:

- Name: Name of the test or function being benchmarked.
- Min: The shortest execution time recorded across all runs.
- Max: The longest execution time recorded.
- Mean: The average execution time across all runs.
- StdDev: The standard deviation of execution times, representing the variability of the
    measurements. A low value indicates consistent performance.
- Median: The middle value when all execution times are sorted.
- IQR (Interquartile Range): The range between the 25th and 75th percentile of execution
    times. It’s a robust measure of variability that’s less sensitive to outliers.
- Outliers: Measurements that significantly differ from others.
    E.g., 5;43 means 5 mild and 43 extreme outliers.
- OPS (Operations Per Second): How many times the function could execute
    in one second (calculated as 1 / Mean).
- Rounds: The number of times the test was run.
- Iterations: Number of iterations per round.
"""

from typing import Callable, List

from kstreams.engine import StreamEngine
from kstreams.streams import Stream
from kstreams.types import ConsumerRecord, Send


async def bench_startup_and_processing_single_consumer_record(
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


async def bench_startup_and_inject_all_deps(
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
    consume_bench: Stream,
    crs: List[ConsumerRecord],
):
    """
    Benchmark injecting all parameters.

    This function benchmarks the processing of a single consumer record by
    using a stream engine to consume the record from a specified topic.
    """
    for cr in crs:
        _ = await consume_bench.func(cr)


def test_startup_and_processing_single_consumer_record(
    stream_engine: StreamEngine,
    consumer_record_factory: Callable[..., ConsumerRecord],
    aio_benchmark,
):
    cr = consumer_record_factory()

    aio_benchmark(
        bench_startup_and_processing_single_consumer_record,
        stream_engine,
        cr,
    )


def test_startup_and_inject_all(
    stream_engine: StreamEngine,
    consumer_record_factory: Callable[..., ConsumerRecord],
    aio_benchmark,
):
    cr = consumer_record_factory()

    aio_benchmark(
        bench_startup_and_inject_all_deps,
        stream_engine,
        cr,
    )


def test_consume_many(
    stream_engine: StreamEngine,
    consumer_record_factory: Callable[..., ConsumerRecord],
    aio_benchmark,
):
    """In this case, the startup happens outside the benchmark."""
    crs = [consumer_record_factory() for _ in range(1000)]

    @stream_engine.stream("local--kstreams")
    async def consume_bench(cr: ConsumerRecord, stream: Stream, send: Send):
        return cr.topic

    aio_benchmark(
        bench_consume_many,
        consume_bench,
        crs,
    )
