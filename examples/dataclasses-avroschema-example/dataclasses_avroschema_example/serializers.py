from dataclasses_avroschema import AvroModel

from kstreams import ConsumerRecord


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
        self, consumer_record: ConsumerRecord, **kwargs
    ) -> ConsumerRecord:
        """
        Deserialize a payload to an AvroModel
        """
        if consumer_record.value is not None:
            data = self.model.deserialize(consumer_record.value)
            consumer_record.value = data
        return consumer_record
