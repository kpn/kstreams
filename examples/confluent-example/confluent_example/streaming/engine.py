from confluent_example import serializers
from schema_registry.client import AsyncSchemaRegistryClient

from kstreams import create_engine

client = AsyncSchemaRegistryClient("http://localhost:8081")

stream_engine = create_engine(
    title="my-stream-engine",
    serializer=serializers.AvroSerializer(client),
    deserializer=serializers.AvroDeserializer(client),
)
