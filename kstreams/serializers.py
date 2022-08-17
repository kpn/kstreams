from typing import Any, Dict, Optional, Protocol

import aiokafka

from .types import Headers


class Deserializer(Protocol):
    async def deserialize(
        self, consumer_record: aiokafka.structs.ConsumerRecord, **kwargs
    ) -> Any:
        """
        This method is used to deserialize the data in a KPN way.
        End users can provide their own class overriding this method.

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
        headers: Optional[Headers] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> bytes:
        """
        Serialize the payload to bytes
        """
        ...
