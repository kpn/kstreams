A `Stream` in `kstreams` is an extension of [AIOKafkaConsumer](https://aiokafka.readthedocs.io/en/stable/consumer.html)

Consuming can be done using `kstreams.Stream`. You only need to decorate a `coroutine` with `@stream_engine.streams`. The decorator has the same  [aiokafka consumer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafkaconsumer-class) API at initialization, in other words they accept the same `args` and `kwargs` that the `aiokafka consumer` accepts.

```python title="Stream usage"
import asyncio
from kstreams import create_engine

stream_engine = create_engine(title="my-stream-engine")


# here you can add any other AIOKafkaConsumer config, for example auto_offset_reset
@stream_engine.stream("dev-kpn-des--py-stream", group_id="de-my-partition")
async def stream(stream: Stream) -> None:
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")


async def main():
    await stream_engine.init_streaming()
    await stream_engine.stop_streaming()


if __name__ == "__main__":
    asyncio.run(main())
```

## Consuming from multiple topics

Consuming from multiple topics using one `stream` is possible. A `List[str]` of topics must be provided.

```python title="Consume from multiple topics"
stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream(["dev-kpn-des--kstreams", "dev-kpn-des--hello-world"], group_id="example-group")
async def consume(stream: Stream) -> None:
    async for cr in stream:
        print(f"Event consumed from topic {cr.topic}: headers: {cr.headers}, payload: {cr.value}")
```

## Changing consumer behavior

Most of the time you will only set the `topic` and the `group_id` to the `consumer`, but sometimes you might want more control over it, for example changing the `policy for resetting offsets on OffsetOutOfRange errors` or `session timeout`. To do this, you have to use the same `kwargs` as the [aiokafka consumer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafkaconsumer-class) API

```python
# The consumer sends periodic heartbeats every 500 ms
# On OffsetOutOfRange errors, the offset will move to the oldest available message (‘earliest’)

@stream_engine.stream("dev-kpn-des--kstream", group_id="de-my-partition", session_timeout_ms=500, auto_offset_reset"earliest")
async def stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")
```

## Manual commit

When processing more sensitive data and you want to be sure that the `kafka offeset` is commited once that you have done your tasks, you can use `enable_auto_commit=False` mode of Consumer.

```python title="Manual commit example"
@stream_engine.stream("dev-kpn-des--kstream", group_id="de-my-partition", enable_auto_commit=False)
async def stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")

        # We need to make sure that the pyalod was stored before commiting the kafka offset
        await store_in_database(payload)
        await stream.consumer.commit()  # You need to commit!!!
```

!!! note
    This is a tradeoff from at most once to at least once delivery, to achieve exactly once you will need to save offsets in the destination database and validate those yourself.
