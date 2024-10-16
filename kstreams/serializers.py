from typing import Any, Dict, Optional, Protocol

from .types import ConsumerRecord, Headers


class Deserializer(Protocol):
    """Protocol used by the Stream to deserialize.

    A Protocol is similar to other languages features like an interface or a trait.

    End users should provide their own class implementing this protocol.

    For example a `JsonDeserializer`

    ```python
    import json
    from kstreams import ConsumerRecord

    class JsonDeserializer:

        async def deserialize(
            self, consumer_record: ConsumerRecord, **kwargs
        ) -> ConsumerRecord:
            data = json.loads(consumer_record.value.decode())
            consumer_record.value = data
            return consumer_record
    ```
    """

    async def deserialize(
        self, consumer_record: ConsumerRecord, **kwargs
    ) -> ConsumerRecord:
        """
        Implement this method to deserialize the data received from the topic.
        """
        ...


class Serializer(Protocol):
    """Protocol used by the Stream to serialize.

    A Protocol is similar to other languages features like an interface or a trait.

    End users should provide their own class implementing this protocol.

    For example a `JsonSerializer`

    ```python
    from typing import Optional, Dict
    import json

    class JsonSerializer:

        async def serialize(
            self,
            payload: dict,
            headers: Optional[Dict[str, str]] = None,
            serializer_kwargs: Optional[Dict] = None,
        ) -> bytes:
            \"""Return UTF-8 encoded payload\"""
            value = json.dumps(payload)
            return value.encode()
    ```

    Notice that you don't need to inherit anything,
    you just have to comply with the Protocol.
    """

    async def serialize(
        self,
        payload: Any,
        headers: Optional[Headers] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> bytes:
        """
        Implement this method to deserialize the data received from the topic.
        """
        ...
