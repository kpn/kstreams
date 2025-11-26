import json
from typing import Any, Dict, Optional

from kstreams import ConsumerRecord, middleware
from kstreams.types import Headers


class JsonSerializer:
    async def serialize(
        self,
        payload: Any,
        headers: Optional[Headers] = None,
        serializer_kwargs: Optional[Dict] = None,
    ) -> bytes:
        """
        Serialize a payload to json
        """
        value = json.dumps(payload)
        return value.encode()


class JsonDeserializerMiddleware(middleware.BaseMiddleware):
    async def __call__(self, cr: ConsumerRecord):
        if cr.value is not None:
            data = json.loads(cr.value.decode())
            cr.value = data
        return await self.next_call(cr)
