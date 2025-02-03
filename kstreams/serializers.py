from typing import Any, Dict, Optional, Protocol

from .types import ConsumerRecord, Headers


class Deserializer(Protocol):
    """Deserializers must implement the Deserializer Protocol

    !!! Example
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
        ...  # pragma: no cover


class _NoDefault:
    """
    This class is used as sentinel to indicate that no default serializer
    value is provided when calling StreamEngine.send(...).

    The sentinel helps to make a distintion between `None` and `_NoDefault` to solve
    the case when there is a global serializer and `send(serializer=None)` is called
    to indicate that `binary` must be send rather a serialized payload.

    If we do not have this sentinel, then we can't distinguish when the global
    serializer should be used or not.

    Example:
        StreamEngine(...).send("topic", value) # use global serializer if set
        StreamEngine(...).send("topic", value, serializer=None) # send binary
        StreamEngine(...).send(
            "topic", value, serializer=CustomSerializer()) # use custom serializer

        * If a global serializer is not set, then binary is always send.

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
        return payload  # pragma: no cover


NO_DEFAULT = _NoDefault()
