
Kafka's job is to move bytes from producer to consumers, through a topic.

By default, this is what kstream does.

```python
--8<-- "examples/recommended-worker-app/recommended_worker_app/streams.py"
```

As you can see the ConsumerRecord's `value` is bytes.

In order to keep your code pythonic, we provide a mechanism to serialize/deserialize
these bytes, into something more useful.
This way, you can work with other data structures, like a `dict` or `dataclasses`.

Sometimes it is easier to work with a `dict` in your app, give it to `kstreams`, and let it transform it into `bytes` to be delivered to Kafka. For this situations, you need to implement `kstreams.serializers.Serializer`.

The other situation is when you consume from Kafka (or other brokers). Instead of dealing with `bytes`,
you may want to receive in your function the `dict` ready to be used. For those cases, implement `kstreams.serializers.Deserializer`

::: kstreams.serializers.Serializer
    options:
        show_root_heading: true
        docstring_section_style: table
        show_bases: false

::: kstreams.serializers.Deserializer
    options:
        show_root_heading: true
        docstring_section_style: table
        show_bases: false

## Usage

Once you have written your serializer or deserializer, there are 2 ways of using them, in a
generic fashion or per stream.

### Initialize the engine with your serializers

By doing this all the streams will use these serializers by default.

```python
stream_engine = create_engine(
    title="my-stream-engine",
    serializer=JsonSerializer(),
    deserializer=JsonDeserializer(),
)
```

### Initilize `streams` with a `deserializer` and produce events with `serializers`

```python
@stream_engine.stream(topic, deserializer=JsonDeserializer())
    async def hello_stream(stream: Stream):
        async for event in stream:
            # remember event.value is now a dict
            print(event.value["message"])
            save_to_db(event)
```

```python
await stream_engine.send(
    topic,
    value={"message": "test"}
    headers={"content-type": consts.APPLICATION_JSON,}
    key="1",
    serializer=JsonSerializer(),
)
```
