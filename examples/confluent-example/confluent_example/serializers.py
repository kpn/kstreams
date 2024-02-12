from typing import Dict

from schema_registry.serializers import AsyncAvroMessageSerializer


class AvroSerializer(AsyncAvroMessageSerializer):
    async def serialize(
        self, payload: Dict, serializer_kwargs: Dict[str, str], **kwargs
    ) -> bytes:
        """
        Serialize a payload to avro-binary using the schema and the subject
        """
        schema = serializer_kwargs["schema"]
        subject = serializer_kwargs["subject"]
        event = await self.encode_record_with_schema(subject, schema, payload)

        return event
