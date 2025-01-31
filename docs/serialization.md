
Kafka's job is to move bytes from producer to consumers, through a topic.

By default, this is what kstream does.

```python
--8<-- "examples/recommended-worker-app/recommended_worker_app/streams.py"
```

As you can see the ConsumerRecord's `value` is bytes.

In order to keep your code pythonic, we provide a mechanism to `serialize/deserialize` these `bytes`, into something more useful.
This way, you can work with other data structures, like a `dict` or `dataclasses`.

## Serializers

Sometimes it is easier to work with a `dict` in your app, give it to `kstreams`, and let it transform it into `bytes` to be delivered to Kafka. For this situation, you need to implement `kstreams.serializers.Serializer`.

::: kstreams.serializers.Serializer
    options:
        show_root_heading: true
        docstring_section_style: table
        show_bases: false
        members:
          -  

## Deserializers

The other situation is when you consume from Kafka (or other brokers). Instead of dealing with `bytes`, you may want to receive in your function the `dict` ready to be used.
For those cases, we need to use [middlewares](https://kpn.github.io/kstreams/middleware/).

### Deserializers Middleware

For example, we can implement a `JsonMiddleware`:

```python
from kstreams import middleware, ConsumerRecord


class JsonDeserializerMiddleware(middleware.BaseMiddleware):
    async def __call__(self, cr: ConsumerRecord):
        if cr.value is not None:
            data = json.loads(cr.value.decode())
            cr.value = data
        return await self.next_call(cr)
```

### Old Deserializers

The old fashion way is to use `Deserializers`, which has been deprecated (but still maintained) in favor of [middleware](https://kpn.github.io/kstreams/middleware/)

::: kstreams.serializers.Deserializer
    options:
        show_root_heading: false
        docstring_section_style: table
        show_bases: false
        members:
          -  

!!! warning
    `kstreams.serializers.Deserializer` will be deprecated, use [middlewares](https://kpn.github.io/kstreams/middleware/) instead

## Usage

Once you have written your `serializer` and  `middleware/deserializer`, there are two ways of use them:

- `Global`: When `Serializer` and/or `Deserializer` is set to the `StreamEngine` isntance
- `Per case`: When a `Serializer` is used with the `send coroutine` or a `Middleware/Deserializer` is set to a `stream`

### Global

By doing this all the streams will use these serializers by default.

```python title="Json events example"
from kstreams import create_engine, middleware, ConsumerRecord

topic = "local--kstreams"

stream_engine = create_engine(
    title="my-stream-engine",
    serializer=JsonSerializer(),
    deserializer=JsonDeserializer(),  # old fashion way and it will be deprecated
)


@stream_engine.stream(topic)
async def hello_stream(cr: ConsumerRecord):
    # remember event.value is now a dict
    print(cr.value["message"])
    save_to_db(cr)
    assert cr.value == {"message": "test"}


await stream_engine.send(
    topic,
    value={"message": "test"}
    headers={"content-type": consts.APPLICATION_JSON,}
    key="1",
)
```

### Per case

This is when `streams` are initialized with a `deserializer` (preferible a middleware) and we produce events with `serializers` in the send function.

- If a `global serializer` is set but we call `send(serializer=...)`, then the local `serializer` is used, not the global one.
- If a `global deserializer` is set but a `stream` has a local one, then then the local `deserializer` is used, not the global one.

```python
from kstreams import create_engine, middleware, ConsumerRecord

topic = "local--kstreams"

# stream_engine created without a `serializer/deserializer`
stream_engine = create_engine(
    title="my-stream-engine",
)

# Here deserializer=JsonDeserializer() instead, but it will be deprecated
@stream_engine.stream(topic, middlewares=[middleware.Middleware(JsonDeserializerMiddleware)])
async def hello_stream(cr: ConsumerRecord):
    # remember event.value is now a dict
    print(cr.value["message"])
    save_to_db(cr)


# send with a serializer
await stream_engine.send(
    topic,
    value={"message": "test"}
    headers={"content-type": consts.APPLICATION_JSON,}
    serializer=JsonSerializer()  # in this case the Global Serializer is not used if there is one
    key="1",
)
```

## Forcing raw data

There is a situation when a `global` serializer is being used but still you want to produce raw data, for example when producing to a `DLQ`.
For this case, when must set the `deserialzer` option to `None`:

```python title="DLQ example"
from kstreams import create_engine, middleware, ConsumerRecord


topic = "local--kstreams"
dlq_topic = "dlq--kstreams"

stream_engine = create_engine(
    title="my-stream-engine",
    serializer=JsonSerializer(),  # Global serializer
)


@stream_engine.stream(topic)
async def hello_stream(cr: ConsumerRecord):
    try:
        # remember event.value is now a dict
        save_to_db(cr)
        assert cr.value == {"message": "test"}
    except DeserializationException:
        await stream_engine.send(
            dlq_topic,
            value=cr.value
            headers=cr.headers
            key=cr.key,
            serializer=None, # force raw data
        )


# this will produce Json
await stream_engine.send(
    topic,
    value={"message": "test"}
    headers={"content-type": consts.APPLICATION_JSON,}
    key="1",
)
```
