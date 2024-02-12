from dataclasses_avroschema import AvroModel


class AvroSerializer:
    async def serialize(self, instance: AvroModel, **kwargs) -> bytes:
        """
        Serialize an AvroModel to avro-binary
        """
        return instance.serialize()
