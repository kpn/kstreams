from typing import Dict
import aiokafka

from schema_registry.serializers import AsyncAvroMessageSerializer


class AvroSerializer(AsyncAvroMessageSerializer):

    async def serialize(self, payload: Dict, value_serializer_kwargs: Dict[str, str], **kwargs) -> bytes:
        """
        Serialize a payload to avro-binary using the schema and the subject
        """
        schema = value_serializer_kwargs["schema"]
        subject = value_serializer_kwargs["subject"]
        event = await self.encode_record_with_schema(subject, schema, payload)        

        return event


class AvroDeserializer(AsyncAvroMessageSerializer):
    async def deserialize(
        self, consumer_record: aiokafka.structs.ConsumerRecord, **kwargs
    ) -> aiokafka.structs.ConsumerRecord:
        """
        Deserialize the event to a dict
        """
        data = await self.decode_message(consumer_record.value)
        consumer_record.value = data
        return consumer_record
