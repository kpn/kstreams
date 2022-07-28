import aiokafka
from dataclasses_avroschema import AvroModel


class AvroSerializer:
    async def serialize(self, instance: AvroModel, **kwargs) -> bytes:
        """
        Serialize an AvroModel to avro-binary
        """
        return instance.serialize()


class AvroDeserializer:
    def __init__(self, *, model: AvroModel) -> None:
        self.model = model

    async def deserialize(
        self, consumer_record: aiokafka.structs.ConsumerRecord, **kwargs
    ) -> aiokafka.structs.ConsumerRecord:
        """
        Deserialize a payload to an AvroModel
        """
        data = self.model.deserialize(consumer_record.value)
        consumer_record.value = data
        return consumer_record
