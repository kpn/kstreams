You can use custom `serialization/deserialization`. The protocol is the following:

```python
from typing import Any, Dict, Optional, Protocol

import aiokafka


class Deserializer(Protocol):
    async def deserialize(
        self, consumer_record: aiokafka.structs.ConsumerRecord, **kwargs
    ) -> Any:
        """
        This method is used to deserialize the data in a KPN way.
        End users can provide their own class overriding this method.
        If the engine was created with a schema_store_client, it will be available.

        class CustomDeserializer(Deserializer):

            async deserialize(self, consumer_record: aiokafka.structs.ConsumerRecord):
                # custom logic and return something like a ConsumerRecord
                return consumer_record
        """
        ...


class Serializer(Protocol):
    async def serialize(
        self,
        payload: Any,
        headers: Optional[Dict[str, str]] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> bytes:
        """
        Serialize the payload to bytes
        """
        ...

```

You can write custom `Serializers` and `Deserializers`. There are 2 ways of using them:

1. Initialize the engine with them
2. Initilize `streams` with a `deserializer` and produce events with `serializers`

```python
stream_engine = create_engine(
    title="my-stream-engine",
    serializer=MySerializer(),
    deserializer=MyDeserializer(),
)
```


```python
@stream_engine.stream(topic, deserializer=MyDeserializer())
    async def hello_stream(stream: Stream):
        async for event in stream:
            save_to_db(event)
```

```python
await stream_engine.send(
    topic,
    value={"message": "test"}
    headers={"content-type": consts.APPLICATION_JSON,}
    key="1",
    serializer=MySerializer(),
)
```
