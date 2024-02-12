from typing import Dict, Optional

from schema_registry.client import AsyncSchemaRegistryClient, schema
from schema_registry.serializers import AsyncAvroMessageSerializer

from kstreams import ConsumerRecord, middleware


class ConfluentMiddlewareDeserializer(
    middleware.BaseMiddleware, AsyncAvroMessageSerializer
):
    def __init__(
        self,
        *,
        schema_registry_client: AsyncSchemaRegistryClient,
        reader_schema: Optional[schema.AvroSchema] = None,
        return_record_name: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.schemaregistry_client = schema_registry_client
        self.reader_schema = reader_schema
        self.return_record_name = return_record_name
        self.id_to_decoder_func: Dict = {}
        self.id_to_writers: Dict = {}

    async def __call__(self, cr: ConsumerRecord):
        """
        Deserialize the event to a dict
        """
        data = await self.decode_message(cr.value)
        cr.value = data
        return await self.next_call(cr)
