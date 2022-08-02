from kstreams import create_engine
from confluent_example import serializers

from schema_registry.client import AsyncSchemaRegistryClient


client = AsyncSchemaRegistryClient("http://localhost:8081")

stream_engine = create_engine(
    title="my-stream-engine",
    value_serializer=serializers.AvroSerializer(client),
    value_deserializer=serializers.AvroDeserializer(client)
)
